import json
import os
import re
from typing import Optional, Self
import textwrap
import logging
import html
from concurrent.futures import ThreadPoolExecutor, as_completed

from snowflake.cortex import complete
from lxml import etree
from snowflake.snowpark import Session

from prompts import TableauCalculationPrompt

logger = logging.getLogger("snowparser")
logger.setLevel(logging.DEBUG)

# Keyword and pattern matching
parameter_pattern = r'^\[Parameter \d+\]$' # Parameter parent prefix
parameter_col_pattern = r'^Parameter \d+$' # Parameter column name
lod_pattern = re.compile(r'\{\s*(FIXED|INCLUDE|EXCLUDE)\b.*?\}', re.IGNORECASE | re.DOTALL) # Level of Detail pattern
agg_pattern = re.compile( # Explicit aggregation pattern
    r'\b(SUM|AVG|COUNTD?|MIN|MAX|MEDIAN|PERCENTILE|STDEV|VAR|TOTAL|WINDOW_\w+|RUNNING_\w+|RANK(?:_DENSE|_UNIQUE)?|INDEX|FIRST|LAST)\s*\(',
    re.IGNORECASE
)
# SQL DDL patterns known to cause issues in Snowflake from Tableau
ddl_patterns = [
        # Replace ISNULL(column) with column IS NULL
        (r'\bISNULL\s*\(\s*([^\)]+?)\s*\)', r'\1 IS NULL'),

        # Normalize DATEPART to DATE_PART
        (r'\bDATEPART\b', 'DATE_PART'),

        # Add more mappings as needed
        # Example: replace NVL with COALESCE
        # (r'\bNVL\s*\(', 'COALESCE('),
    ]

def prep_for_sql_names(text: str) -> str:
    """
    Prepares a string to be used as a SQL object name.
    1. Remove all non-alphanumeric characters except for underscores
    2. Collapse multiple underscores into a single underscore
    3. Remove trailing underscore
    4. Names can't start with a number, so add an underscore if the name starts with a number

    :param text: The text to clean up
    :returns: The cleaned up text
    """

    # Remove all non-alphanumeric characters except for underscores
    # Collapse multiple underscores into a single underscore
    # Remove trailing underscore
    cleaned = re.sub(r'_+$', '', re.sub(r'_+', '_', re.sub(r'[^\w]', '_', text)))

    # Names can't start with a number
    if cleaned[0].isdigit():
        cleaned = "_" + cleaned
    return cleaned

def clean_snowflake_ddl(sql: str, patterns: list[tuple[str, str]]) -> str:
    """
    Clean up SQL DDL to avoid known issues in Snowflake

    :param sql: The SQL DDL to clean up
    :returns: The cleaned up SQL DDL
    """

    # Apply replacements in order
    for pattern, replacement in patterns:
        sql = re.sub(pattern, replacement, sql, flags=re.IGNORECASE)

    return sql

class TableauConnection:
    def __init__(
            self,
            conn_class: str,
            dbname: str,
            schema: str,
            server: str,
            warehouse: Optional[str] = None,
            role : Optional[str] = None,
            ):
        self.conn_class = conn_class
        self.dbname = dbname
        self.schema = schema
        self.server = server
        self.warehouse = warehouse
        self.role = role


class TableauRelation:
    def __init__(
            self,
            session: Session,
            alias: None|str,
            name: None|str,
            id: None|str,
            rel_type: None|str,
            full_table_name: None|str = None,
            sql: None|str = None,
            create_view: bool = True, # Determines if a custom SQL view should be created in process
            ):
        self.session = session
        self.alias = alias
        self.name = name
        self.id = id
        self.rel_type = rel_type
        self.full_table_name = full_table_name
        self.unique_key = []
        self.comment = None
        self.db, self.schema, self.table = (None, None, None)
        self.sql = sql
        self.create_view = create_view
        self.create_custom_view() # Run to create view for custom SQL before pulling metadata from it
        self.get_table_context()
        self.get_primary_key()
        self.get_comment()

    def get_table_context(self) -> None:
        """Extracts the database, schema, and table name from the full_table_name."""

        if self.full_table_name is not None:
            self.db, self.schema, self.table = self.full_table_name.replace('[', '').replace(']', '').split('.')

    def get_primary_key(self) -> None:
        """
        Extracts the primary key from the table.
        This is only needed if we are not using a custom SQL source.
        """

        # Only need to get PKs if we are not using a custom SQL source
        if self.full_table_name is not None and self.sql is None:
            results = self.session.sql(f'SHOW PRIMARY KEYS IN "{self.db}"."{self.schema}"."{self.table}"').collect()
            keys = [r.column_name for r in results]
            if len(keys) > 0:
                self.unique_key.append(keys)

    def get_comment(self) -> None:
        """
        Extracts the comment from the table (or view).
        This is only needed if we are not using a custom SQL source.
        """
        # Only need to get comment if we are not using a custom SQL source
        if self.full_table_name is not None and self.sql is None:
            result = self.session.sql(f"SHOW TABLES LIKE '{self.table}' in schema {self.db}.{self.schema}").collect()
            if len(result) > 0:
                self.comment = result[0].comment
            else: # Relation may be a view
                result = self.session.sql(f"SHOW VIEWS LIKE '{self.table}' in schema {self.db}.{self.schema}").collect()
                if len(result) > 0:
                    self.comment = result[0].comment
            # TO DO: Generate a comment using Cortex

    def create_custom_view(self) -> None:
        """
        Creates a Snowflake view using custom SQL SELECT as DDL predicate.
        This is only needed if we are using a custom SQL source.
        """
        if self.sql is not None:
            try:
                alias = prep_for_sql_names(self.alias)
                # Will require having a current db and schema set which may introduce issues later
                self.full_table_name = f'{self.session.get_current_database()}.{self.session.get_current_schema()}.{alias}'
                if self.create_view:
                    self.session.sql(f'CREATE OR REPLACE VIEW {alias} AS {self.sql}').collect()
                    logger.info("Successfully created custom Snowflake view %s", self.full_table_name)
                else:
                    logger.warning("Warning: Custom SQL source %s not created as a view", self.full_table_name)

            except Exception as e:
                logger.error("An error occurred at CREATE VIEW: %s", e)
                raise e

    def get_ddl(self) -> str:
        """
        Returns the DDL for the relation as a table in Semantic View.

        :returns: The DDL for the table
        """
        comment_clause = f"COMMENT = '{self.comment}'" if self.comment else ''

        self.table = prep_for_sql_names(self.table)
        self.alias = prep_for_sql_names(self.alias)

        # State each unique key set with a new UNIQUE clause
        unique_clause = ''
        if len(self.unique_key) > 0:
            for key_set in self.unique_key:
                unique_clause += f"UNIQUE (" + ', '.join([f'{prep_for_sql_names(x)}' for x in key_set]) +") "

        # Synonym and name cannot match
        synonym_clause = f"WITH SYNONYMS = ('{self.alias}')" if self.alias != self.table else ''

        query = f"""
{self.table} AS {self.db}.{self.schema}.{self.table}
{unique_clause}
{synonym_clause}
{comment_clause}"""

        return query


class TableauColumnarMetadata:
    def __init__(
            self,
            remote_name: None|str, # actual column name in SF table
            local_name: str, # display name in Tableau
            parent_name: None|str, # table or relation this column belongs to
            local_type: None|str, # Tableau's interpreted type
            alias: None|str = None, # caption for the column
            role: None|str = None, # dictates if measure or dimension
                 ):
        self.name = local_name
        self.table = parent_name.replace('[', '').replace(']', '') if parent_name is not None else None
        self.column_name = remote_name
        self.datatype = local_type
        self.alias = alias
        self.role = role
        self.sf_section = 'dimensions'
        self.get_sf_section()

    def get_sf_section(self) -> None:
        """Categorizes the column as a fact or dimension based on its role in Tableau."""

        # Some columns will not have role as they don't have associated columns in .tds
        # We will leave these as dimensions
        if self.role is not None:
            self.sf_section = 'facts' if self.role == 'measure' else 'dimensions'

    def get_ddl(self) -> str:
        """
        Returns the DDL for the column as a dimension/measure in Semantic View.

        :returns: The DDL for the column
        """

        table = prep_for_sql_names(self.table)
        column_name = prep_for_sql_names(self.column_name)

        synonym_clause = f"WITH SYNONYMS = ('{self.alias}')" if self.alias is not None else ''
        query = f"""
{table}.{column_name} AS {table}."{self.column_name}"
{synonym_clause}"""

        return query

class TableauCalculations:
    def __init__(
            self,
            session: Session,
            alias: None|str, # display name in Tableau
            datatype: None|str,
            formula: None|str = None,
            role: None|str = None,
            name: None|str = None,
            type: None|str = None,
                ):

        self.session = session
        self.alias = alias
        self.datatype = datatype
        self.formula = formula
        self.translated_formula = None
        self.role = role
        self.first_referenced_column = None
        self.parent_table = []
        self.explanation = None
        self.name = name
        self.type = type
        self.sf_section = self.get_sf_section()
        self.references = self.get_formula_references()

    def get_formula_references(self) -> list[str]|None:
        """Extracts all bracketed column references from the Tableau formula.

        :returns: A list of column references
        """

        if self.formula is not None:
            pattern = r"\[(.*?)\]"
            match = re.findall(pattern, self.formula)
            if match:
                return list(set(match))
            else:
                return []

    def output_orphans(self) -> dict[str, str]:
        """
        Extracts key elements from Calculation for final orphan rendering.

        :returns: A dictionary of key elements
        """

        return {
            'alias': getattr(self, 'alias', 'not found'),
            'formula': getattr(self, 'formula', 'not found'),
            'translated_formula': getattr(self, 'translated_formula', 'not found'),
            'explanation': getattr(self, 'explanation', 'not found'),
            }

    def get_sf_section(self) -> str:
        """Categorizes the column as a fact/dimension/metric based on Tableau metadata.

        This is the initial categorization based on Tableau metadata.
        We will re-categorize the calculation after evaluating its references.

        :returns: The section the calculation should be placed in
        """
        section = 'facts' # Default to facts as safest option
        if self.role == 'dimension':
            section = 'dimensions'
        elif self.role == 'measure':
            if bool(re.search(agg_pattern, self.formula)):
                section = 'metrics'
            else:
                if self.type in ['nominal', 'ordinal']:
                    section = 'dimensions'
                else: # Only remaining option is 'quantitative'
                    section = 'facts'
        return section


    def evaluate_references(self,
                            columns: list[TableauColumnarMetadata],
                            calculations: list[Self]) -> Self:
        """
        Evaluates the references of the calculation to determine if it can be included in the Semantic View.

        Updates the parent_table atribute based on columns found in the references.
        Excluded calculations are assigned to the 'exclusions' section.

        :param columns: The list of all columns
        :param calculations: The list of all calculations

        :returns: The updated calculation
        """

        if bool(re.match(parameter_pattern, self.name)):
            self.sf_section = 'exclusions'
            self.explanation = 'This is a standalone parameter.'
        elif bool(re.search(lod_pattern, self.formula)):
            self.sf_section = 'exclusions'
            self.explanation = 'This calculation references a Level of Detail expression which is not currently supported in semantic view. It will be excluded from the semantic view.'
        else:
            parameter_found = self._recursive_evaluate(self.references, columns, calculations)

            if not parameter_found and len(self.parent_table) == 0:
                self.sf_section = 'exclusions'
                self.explanation = 'This calculation cannot be tied back to a physical column and subsequent parent table. It will be excluded from the semantic view.'

        return self

    def _recursive_evaluate(self,
                            refs: list[str],
                            columns: list[TableauColumnarMetadata],
                            calculations: list[Self]) -> bool:
        """
        Recursively evaluates the references of the calculation to determine if it can be included in the Semantic View.

        :param refs: The list of references
        :param columns: The list of all columns
        :param calculations: The list of all calculations

        :returns: A flag indicating if a parameter was found
        """
        if not refs:
            self.sf_section = 'exclusions'
            self.explanation = 'This calculation has no referenced fields and may just be a literal. It will be excluded from the semantic view.'
            return False
        else:
            found = False  # ← Add this flag
            for ref in refs:

                if ref == 'Parameters' or bool(re.match(parameter_col_pattern, ref)):
                    self.sf_section = 'exclusions'
                    self.explanation = 'This calculation references a parameter and is not translatable to SQL. It will be excluded from the semantic view.'
                    return True

                # Check for referenced column and grab parent table references
                matching_column = next((col for col in columns if col.name == f"[{ref}]"), None)
                if matching_column is not None and matching_column.table is not None:
                    if matching_column.table not in self.parent_table:
                        self.parent_table.append(matching_column.table)
                    found = True  # ← update flag but keep going

                # Recursively check referenced calculations
                matching_calculations = [calc for calc in calculations if calc.name == f"[{ref}]"]
                if matching_calculations:
                    for matching_calculation in matching_calculations:

                        # Need to exclude any calculation that references a LOD calculation
                        if bool(re.search(lod_pattern, matching_calculation.formula)):
                            self.sf_section = 'exclusions'
                            self.explanation = 'This calculation references a Level of Detail calculation which is not currently supported in semantic view. It will be excluded from the semantic view.'
                            return True

                        else:
                            nested_found = self._recursive_evaluate(matching_calculation.references, columns, calculations)
                            found = found or nested_found
                else:
                    # Only flag as exclusion if no match at all
                    if not matching_column and not matching_calculations:
                        self.sf_section = 'exclusions'
                        self.explanation = 'This calculation cannot be tied back to a physical column and subsequent parent table. It will be excluded from the semantic view.'

            return found  # ← summary of what was found

    def check_excluded_references(self, calculations: list[Self]) -> Self:
        """
        Checks if the calculation references any excluded calculations.

        If a calculation references an excluded calculation, it will also be excluded.

        :param calculations: The list of all calculations

        :returns: The updated calculation
        """
        def recursive_check(refs: list[str], calculations: list[Self]) -> Self:

            for ref in refs:
                matching_calculation = next((calc for calc in calculations if calc.name == f"[{ref}]"), None)
                if matching_calculation is not None:
                    if matching_calculation.sf_section == 'exclusions':
                        if self.sf_section != 'exclusions':
                            self.sf_section = 'exclusions'
                            self.explanation = 'This calculation references another excluded calculation. It will be excluded from the semantic view.'
                        return self
                    else:
                        return recursive_check(matching_calculation.references, calculations)
                else:
                    return self
            return self

        return recursive_check(self.references, calculations)


    def get_ddl(self) -> str:
        """
        Returns the DDL for the calculation as a dimension/measure/metric in Semantic View.

        :returns: The DDL for the column
        """
        # If formula is not translatable, we will not include it in the DDL
        # This is to prevent the DDL from failing if there is an error in the formula
        # Some calculations may not be pertinent to semantic view such as custom spacing calculations in Tableau
        if self.translated_formula is not None and len(self.parent_table) > 0:
            alias = prep_for_sql_names(self.alias)
            parent_table = prep_for_sql_names(self.parent_table[0])

            query = f"""
{parent_table}.{alias} AS {self.translated_formula}
"""
        else:
            query = ''
        return query

    def translate(self) -> None:
        """
        Translates the Tableau formula to SQL using Cortex AI.
        """
        if self.formula is not None and self.sf_section != 'exclusions':
            formula_prompt = TableauCalculationPrompt(
                alias = self.alias,
                formula = self.formula,
                )

            try:
                result = json.loads(complete(formula_prompt.model,
                            formula_prompt.prompt,
                            options = formula_prompt.options,
                            session = self.session)
                )
                self.explanation = result.get('explanation')
                if 'valid' in result:
                    if result.get('valid'):
                        if 'translated_formula' in result:
                            self.translated_formula = result.get('translated_formula')
                        else:
                            logger.warning("Warning: Unable to translate Tableau formula for %s", self.alias)
                            self.sf_section = 'exclusions'
                            self.translated_formula = None
                    else:
                        logger.info("Info: Excluding Tableau calculation %s", self.alias)
                        self.sf_section = 'exclusions'
                        self.translated_formula = None
                else:
                    logger.warning("Warning: An error occurred in categorizing Tableau calculation %s", self.alias)

            except json.JSONDecodeError as e:
                self.sf_section = 'exclusions'
                self.translated_formula = None
                logger.warning("Error unpacking Cortex response: %s", e)
            except Exception as e:
                self.sf_section = 'exclusions'
                self.translated_formula = None
                logger.warning("Error translating calculation formula: %s", e)


class TableauRelationship:
    def __init__(
            self,
            session: Session,
            relationship_block: etree._Element,
            relations = list[TableauRelation],
            columns = list[TableauColumnarMetadata],
            ):
        self.session = session
        self.relationship_block = relationship_block
        self.relationship_string = etree.tostring(self.relationship_block, encoding='unicode')
        self.relations = relations
        self.columns = columns
        self.translated_relationship = {}
        self.left_table = ''
        self.left_unique = False
        self.right_unique = False
        self.right_table = ''
        self.left_columns = []
        self.right_columns = []
        self.supported = True # Will be used to exclude relationships that are not supported
        self.explanation = None
        self.parse_relationship_flat()


    def parse_expression_pairs(self, expr: etree._Element, pairs: Optional[list[tuple[str, str]]] = None) -> list:
        """
        Parses the expression tree (containing join column names) and flattens the column names.
        """
        if pairs is None:
            pairs = []

        op = expr.attrib.get('op')
        children = list(expr)

        if op == '=' and len(children) == 2:
            # Base case: '=' with two fields
            left = children[0].attrib.get('op')
            right = children[1].attrib.get('op')
            pairs.append((left, right))
        elif op in ('AND', 'OR'):
            # Recurse into logical combinations
            for child in children:
                self.parse_expression_pairs(child, pairs)

        return pairs

    def parse_relationship_flat(self) -> None:
        """
        Parses the relationship expression and flattens the column names.
        """
        # Parse expression tree
        expression_node = self.relationship_block.find('expression')
        join_pairs = self.parse_expression_pairs(expression_node)

        # Get tables nodes
        table_one = self.relationship_block.find('first-end-point')
        table_two = self.relationship_block.find('second-end-point')

        # Determine which table has UNIQUE column designated by unique attribution
        # View supports many-to-1 in that specific order
        if (table_one.attrib.get('unique-key', False) or table_one.attrib.get('is-db-set-unique-key', False)) and \
            (table_two.attrib.get('unique-key', False) or table_two.attrib.get('is-db-set-unique-key', False)):
            self.left_unique = True
            self.right_unique = True
            left_table = table_one.attrib.get('object-id')
            right_table = table_two.attrib.get('object-id')
            logger.warning("Warning: Relationship between %s and %s designates a relationship as 1-to-1, which is not supported.", right_table, left_table)
            self.supported = False
            self.explanation = """Semantic View relationships currently support many-to-1 relationships. This is a 1-to-1 relationship.
            Denote unique keys in Tableau using [Performance Options](https://help.tableau.com/current/pro/desktop/en-us/datasource_relationships_perfoptions.htm) in Tableau Desktop."""
        elif table_one.attrib.get('unique-key', False) or table_one.attrib.get('is-db-set-unique-key', False):
            self.right_unique = True # View requires many-to-1 to be in this order
            right_table = table_one.attrib.get('object-id')
            left_table = table_two.attrib.get('object-id')
            # We need to switch left and right columns to  match the order of the tables
            join_pairs = [(right, left) for left, right in join_pairs]
        elif table_two.attrib.get('unique-key', False) or table_two.attrib.get('is-db-set-unique-key', False):
            self.right_unique = True # View requires many-to-1 to be in this order
            right_table = table_two.attrib.get('object-id')
            left_table = table_one.attrib.get('object-id')
        else:
            left_table = table_one.attrib.get('object-id')
            right_table = table_two.attrib.get('object-id')
            logger.warning("Warning: Relationship between %s and %s designates a relationship as many-to-many, which is not supported.", right_table, left_table)
            self.supported = False
            self.explanation = """Semantic View relationships currently support many-to-1 relationships. This is a many-to-many relationship.
            Denote unique keys in Tableau using [Performance Options](https://help.tableau.com/current/pro/desktop/en-us/datasource_relationships_perfoptions.htm) in Tableau Desktop."""

        self.left_table = next((rel.alias for rel in self.relations if rel.id == left_table), None)
        self.right_table = next((rel.alias for rel in self.relations if rel.id == right_table), None)

        # Flatten left/right column names
        left_columns = []
        right_columns = []
        for left, right in join_pairs:
            left_columns.append(next((col.column_name for col in self.columns if col.name == left and col.table == self.left_table), None))
            right_columns.append(next((col.column_name for col in self.columns if col.name == right and col.table == self.right_table), None))

        self.left_columns = left_columns
        self.right_columns = right_columns


    def output_orphans(self) -> dict[str, str]:
        """
        Extracts key elements from Relationship for final orphan rendering.

        :returns: A dictionary of key elements
        """

        return {
            'left_table': getattr(self, 'left_table', 'not found'),
            'right_table': getattr(self, 'right_table', 'not found'),
            'left_columns': ','.join(getattr(self, 'left_columns', ['not found'])),
            'right_columns': ','.join(getattr(self, 'right_columns', ['not found'])),
            'supported': getattr(self, 'supported', 'not found'),
            'explanation': getattr(self, 'explanation', 'not found'),
            'left_unique': getattr(self, 'left_unique', 'not found'),
            'right_unique': getattr(self, 'right_unique', 'not found'),
            }


    def get_ddl(self) -> str:
        """
        Returns the DDL for the relationship as a relationship in Semantic View.

        :returns: The DDL for the relationship
        """
        left_table = prep_for_sql_names(self.left_table)
        right_table = prep_for_sql_names(self.right_table)
        name = f"{left_table}_to_{right_table}"
        query = f"""
{name} AS
{left_table} ({', '.join([f'{prep_for_sql_names(c)}' for c in self.left_columns])}) REFERENCES {right_table} ({', '.join([f'{prep_for_sql_names(c)}' for c in self.right_columns])})
"""
        if self.supported:
            return query


class TableauTDS:
    def __init__(self, file_path: str, session: Session, create_views: bool = True, defer_conversion: bool = False):
        """
        Initializes the Tableau TDS object.

        :param file_path: Path to the TDS file
        :param session: Snowflake Snowpark session
        :param create_views: Flag to determine if custom SQL views should be created if Tableau source uses custom SQL SELECT statements
        :param defer_conversion: Flag to determine if the TDS file should be parsed and converted to Semantic View DDL
                                 If set to True, minimal parsing will be done to extract the list of physical tables and columns.
        """

        self.session = session
        self.file_path = file_path
        self.create_views = create_views
        self.connections = []
        self.relations = []
        self.columnar_metadata = []
        self.calculations = []
        self.relationships = []
        self.orphans = {} # Captures elements that could not be translated to Semantic View equivalent
        self.root = self.read_file() # Read the file once and reuse the root element
        self.parse_tds()

        if not defer_conversion:
            self.parse_calculations()
            self.parse_relationships()
            self.add_view_pks() # Extract relationship primary keys to assign to new custom VIEWs
            self.gather_orphans() # Gather elements that could not be translated to Semantic View equivalent

    def read_file(self) -> etree._Element:
        """
        Reads the Tableau TDS file.

        Assumes the file is in XML format and location is passed via stage file location or URL or locally.

        :returns: The root element of the XML file
        """

        logger.info('Reading file')
        if self.file_path.startswith('https://') or self.file_path.startswith('@'):
            from snowflake.snowpark.files import SnowflakeFile
            if self.file_path.startswith('https://'):
                open_file = SnowflakeFile.open(self.file_path)
            else:
                open_file = SnowflakeFile.open(self.file_path, require_scoped_url = False)
            tree = etree.parse(open_file)
        else:
            tree = etree.parse(self.file_path)

        root = tree.getroot()
        return root

    def process_calculation_node(self, column: TableauCalculations) -> TableauCalculations:
        """
        Processes a calculation node to translate the formula and replace references with DDL-compatible references.

        Translation is done using Cortex AI ONLY if the section is not already set to 'exclusions'.

        :param column: The calculation node
        :returns: The updated calculation node
        """

        # Replace names in formulas to match what will be in semantic views as referenceable fields
        for col in self.calculations:
            if col.parent_table: # Only replace if we have a parent table
                column.formula = column.formula.replace(f'{col.name}', f'[{prep_for_sql_names(col.parent_table[0])}.{prep_for_sql_names(col.alias)}]')
        for col in self.columnar_metadata:
            column.formula = column.formula.replace(f'[{col.column_name}]', f'[{prep_for_sql_names(col.table)}.{prep_for_sql_names(col.column_name)}]')

        # Many calculations should already be classified as exclusions so we don't want to translate those
        # But we want to keep them in the list so we can report back to the user
        if column.sf_section != 'exclusions':
            column.translate()

        return column

    def extract_calculations(self):
        """
        Conducts all the necessary evaluations and recursions to extract and categorize calculations from the TDS file.

        This includes:
        1. Evaluating calculation references
        2. Translating calculation formulas
        3. Checking for excluded references
        """

        # References are already captured for Calculation at instantiation
        # We must run this first before replacing calculation names cause we need to pull in parent tables after traversals are completed
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(col.evaluate_references, self.columnar_metadata, self.calculations) for col in self.calculations]
            self.calculations = [f.result() for f in as_completed(futures) if f.result() is not None]

        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(self.process_calculation_node, col) for col in self.calculations]
            self.calculations = [f.result() for f in as_completed(futures) if f.result() is not None]

        # Final recursion to check if any calculations are referencing excluded calculations from prior processing
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(col.check_excluded_references, self.calculations) for col in self.calculations]
            self.calculations = [f.result() for f in as_completed(futures) if f.result() is not None]


    def parse_tds(self) -> None:
        """
        Parses the Tableau TDS file to extract all TDS metadata as Semantic View component attributes.
        """

        logger.info('Parsing file')

        # Check if extracts are found in file.
        # We cannot reverse engineer extracts so abort early
        if self.root.find(".//extract") is not None:
            logger.error("Extract node found in the root.")
            msg = """Extract node found in the root.
            Only live Snowflake connection sources can be translated.
            Please use live connections only and remove existing extract dependencies.
            See 'Remove the extract from the workbook' section in https://help.tableau.com/current/pro/desktop/en-us/extracting_data.htm."""

            return msg

        # Parse connections
        for conn in self.root.findall(".//connection[@class='snowflake']"):
            self.connections.append(
                TableauConnection(
                    conn_class = conn.get("class"),
                    dbname = conn.get("dbname"),
                    schema = conn.get("schema"),
                    server = conn.get("server"),
                    warehouse = conn.get("warehouse"),
                    role = conn.get("service"),
                )
            )

        # Parse relations (tables) from object tags
        for object in self.root.findall(".//object"):
            caption = object.get("caption")
            id = object.get("id") # Unique ID to help with table relationships
            for relation in object.findall(".//relation"):
                self.relations.append(
                    TableauRelation(
                        session = self.session,
                        alias = caption,
                        name = relation.get("name"),
                        id = id,
                        rel_type = relation.get("type"),
                        full_table_name = relation.get("table", None), # Only used if source is live table connection
                        sql = html.unescape(relation.text) if relation.get("type") == "text" else None, # Custom SQL sourcing
                        create_view = self.create_views, # Determines if a custom SQL view should be created in process
                      )
                )

        # Parse columnar metadata
        for meta in self.root.findall(".//metadata-record[@class='column']"):
            key_name = meta.find("local-name").text if meta.find("local-name") is not None else None
            if key_name is not None:
                # Columns referencing custom SQL sources cite the parent name as Custom SQL Query X
                # We must map this to the caption/alias of the source as this is the logical and physical name of the View source
                column_parent_name = meta.find("parent-name").text.replace("[", "").replace("]", "")
                parent_name = next((rel.alias for rel in self.relations if rel.name == column_parent_name), None)
                column_element = self.root.find(f".//column[@name='{key_name}']")

                self.columnar_metadata.append(
                    TableauColumnarMetadata(
                        remote_name = meta.find("remote-name").text if meta.find("remote-name") is not None else None,
                        local_name = key_name,
                        parent_name = parent_name,
                        local_type = meta.find("local-type").text if meta.find("local-type") is not None else None,
                        alias = column_element.get("caption") if column_element is not None else None, # Hidden columns will not have a caption
                        role = column_element.get("role") if column_element is not None else None,
                    )
                )

    # Parse calculations
    def parse_calculations(self) -> None:
        """
        Parses the calculations from the Tableau TDS file.
        """

        for column in self.root.findall(f".//calculation"):
            parent_column = column.getparent()
            formula = column.get("formula")

            if formula is not None:

                self.calculations.append(
                    TableauCalculations(
                        session = self.session,
                        alias = parent_column.get("caption"),
                        datatype = parent_column.get("datatype"),
                        formula = html.unescape(formula),
                        role = parent_column.get("role"),
                        name=parent_column.get("name"),
                        type=parent_column.get("type"),
                    )
                )

        # Run calculation evaluation helper function
        self.extract_calculations()

    def parse_relationships(self) -> None:
        """
        Parses the relationships from the Tableau TDS file.
        """

        for relationship in self.root.findall(".//relationship"):
            self.relationships.append(
                TableauRelationship(
                    session = self.session,
                    relationship_block = relationship,
                    relations = self.relations,
                    columns = self.columnar_metadata
                )
            )

    # Relationship keys are only useable if they're referenced as UNIQUE keys in the table
    # We must add these to the table definitions
    def add_view_pks(self) -> None:
        """
        Adds the relationship keys to the table definitions if relevant.
        """
        if len(self.relationships) > 0:
            for rel in self.relations:
                for relationship in self.relationships:
                    if relationship.left_table == rel.alias and relationship.left_unique: # Only add if column is designated as unique
                        if relationship.left_columns not in rel.unique_key:
                            rel.unique_key.append(relationship.left_columns)
                    if relationship.right_table == rel.alias and relationship.right_unique: # Only add if column is designated as unique
                        if relationship.right_columns not in rel.unique_key:
                            rel.unique_key.append(relationship.right_columns)


    @staticmethod
    def get_list_ddl(lst) -> str:
        """Unrolls class get_ddl() method from each Semantic View component into DDL string."""

        return ', '.join([i.get_ddl() for i in lst if i is not None and i.get_ddl() != '' and i.get_ddl() is not None])

    def deduplicate_calculations(self) -> list[TableauCalculations]:
        """
        Deduplicates calculations by parent table and alias.
        Tableau users may create multiple calculations with the same name.

        :returns: A list of unique calculations
        """

        seen = set()
        unique = []

        for inst in self.calculations:
            if len(inst.parent_table) > 0:
                parent_table = getattr(inst, 'parent_table')[0].lower()
                key = (parent_table, getattr(inst, 'alias').lower())
                if key not in seen:
                    seen.add(key)
                    unique.append(inst)

        return unique

    def gather_orphans(self) -> None:
        """
        Gathers all elements that are not supported in Semantic View in orphans attribute for user.
        """

        # Gather unsupported calculations
        unsupported_calculations = []
        for item in self.calculations:
            if item.sf_section == 'exclusions':
                unsupported_calculations.append(item)
        self.orphans['calculations'] = unsupported_calculations

        # Gather unsupported relationships
        unsupported_relationships = []
        for item in self.relationships:
            if not item.supported:
                unsupported_relationships.append(item)
        self.orphans['relationships'] = unsupported_relationships


    def get_view_ddl(self, name: str) -> dict[str, str|list]:
        """
        Returns the DDL for the Semantic View.

        DDL uses a CREATE OR REPLACE clause.

        :param name: The name of the Semantic View

        :returns:
            The DDL for the Semantic View
            A dictionary of orphans (elements that could not be translated to Semantic View equivalent)
        """

        # De-dupe calculations but don't override the original list so we can still report back to the user
        calculations = self.deduplicate_calculations()

        facts = []
        dimensions = []
        metrics = []
        for item in self.columnar_metadata:
            if item.sf_section == 'facts':
                facts.append(item)
            elif item.sf_section == 'metrics':
                metrics.append(item)
            else:
                dimensions.append(item)
        for item in calculations:
            if item.sf_section == 'facts':
                facts.append(item)
            if item.sf_section == 'metrics':
                metrics.append(item)
            if item.sf_section == 'dimensions':
                dimensions.append(item)

        query = f"""
CREATE OR REPLACE SEMANTIC VIEW {name}
tables (
    {self.get_list_ddl(self.relations)})"""
        if sum([i.supported for i in self.relationships]) > 0:
            query += f"""
relationships (
    {self.get_list_ddl(self.relationships)})"""

        if len(facts) > 0:
            query += f"""
facts (
    {self.get_list_ddl(facts)})"""

        if len(dimensions) > 0:
            query += f"""
dimensions (
    {self.get_list_ddl(dimensions)})"""

        if len(metrics) > 0:
            query += f"""
metrics (
    {self.get_list_ddl(metrics)})"""

        query = clean_snowflake_ddl(query, ddl_patterns)

        orphans = {
            key: [item.output_orphans() for item in value_list]
            for key, value_list in self.orphans.items()
        }

        return {
            'ddl': os.linesep.join([s for s in query.splitlines() if s]), # Remove extra line breaks
            'orphans': orphans,
        }

    def create_semantic_view(self, name: str) -> str:
        """
        Creates the Semantic View in Snowflake.

        :param name: The name of the Semantic View
        :returns: A message indicating the success or failure of the operation
        """
        try:
            query = self.get_view_ddl(name)
            self.session.sql(query['ddl']).collect()
            logger.info("Successfully created SEMANTIC VIEW %s.", name)
            return f"Successfully created SEMANTIC VIEW {name}."
        except Exception as e:
            logger.error("An error occurred at CREATE SEMANTIC VIEW: %s", e)
            return f"An error occurred at CREATE SEMANTIC VIEW: {e}"

    def get_table_names(self) -> list[str]:
        """
        Returns a list of table/view names from the TDS file.

        :returns: A list of table names
        """
        return [rel.full_table_name.replace('[','').replace(']','') for rel in self.relations]

    def get_column_names(self) -> list[str]:
        """
        Returns a list of table/view names from the TDS file.

        :returns: A list of table names
        """

        return [f'{col.table}.{col.column_name}' for col in self.columnar_metadata]

    def get_object_names(self) -> dict[str, list[str]]:
        """
        Returns a dictionary of table/view and column names the TDS file.

        :returns: A dictionary of table and column names
        """

        tables = self.get_table_names()
        columns = self.get_column_names()

        return {
            "tables": [
                {
                    "name": table,
                    "columns": [col for col in columns if col.split('.')[0].lower() == table.split('.')[-1].lower()]
                }
                for table in tables
            ]
        }
