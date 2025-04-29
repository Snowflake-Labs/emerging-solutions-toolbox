from snowflake.cortex import CompleteOptions

# class TableauRelationshipPrompt():
#     def __init__(self,
#                  relationship_string,
#                  relations,
#                  columns) -> None:
#         self.system_prompt = f"""You are an Analyst tasked with translating a Tableau data source into a semantic model for a new system.
#         You will be asked to extract information from the Tableau data source specifications to be translated later into the new semantic model.
#         The Tableau data source contains the following tables, referred to as TableauRelations:
#         {relations}
#         The Tableau data source contains the following columns, referred to as TableauMetadata:
#         {columns}
#         You will receive a relationship node from the Tableau data source XML file.
#         You are to extract the following information from the relationship node:
#         - The left table name (from first-end-point node)
#         - The right table name (from second-end-point node)
#         - The left join column(s) from ALL expression nodes
#         - The right join column(s) from ALL expression nodes
#         Follow these rules when extracting the information:
#         - Extract the table alias names based on the matching object-id in the relationship.
#         - Remove the brackets from the column names.
#         - The join column(s) in the left and right table should be in the same order as the expression nodes.
#         - Do not omit any columns referenced in the expression nodes."""
#         self.user_prompt = f"Here is the relationship node from the Tableau data source XML file: \n{relationship_string}"
#         self.response_schema = {
#             "type": "json",
#             "schema": {
#                 "type": "object",
#                 "properties": {
#                     "left_table_name": {"type": "string"},
#                     "right_table_name": {"type": "string"},
#                     "left_join_columns": {
#                         "type": "array",
#                         "items": {"type": "string"},
#                         "description": "All join column(s) for the left table.",
#                     },
#                     "right_join_columns": {
#                         "type": "array",
#                         "items": {"type": "string"},
#                         "description": "All join column(s) for the right table.",
#                     },
#                 },
#                 "required": [
#                     "left_table_name",
#                     "right_table_name",
#                     "left_join_columns",
#                     "right_join_columns",
#                 ],
#             }
#         }
#         self.model = 'mistral-large2'
#         self.temperature = 0.0
#         self.max_tokens = 1000
#         self.prompt=[
#             {"role": "system", "content": self.system_prompt},
#             {"role": "user", "content": self.user_prompt}
#             ]
#         self.options = CompleteOptions(
#                 max_tokens=self.max_tokens,
#                 temperature=self.temperature,
#                 response_format=self.response_schema
#             )

# class TableauFormulaPrompt():
#     def __init__(self,
#                  formula) -> None:
#         self.system_prompt = f"""You are an Analyst tasked with translating a Tableau data source into a semantic model for Snowflake.
#         You will be asked to translate a Tableau calculated field formula to the Snowflake SQL equivalent formula.
#         The Tableau formula contains column referenced enclosed by brackets.
#         You are to translate the Tableau formula to a Snowflake SQL SELECT clause.
#         If the Tableau formula cannot be translated, provide an explanation.
#         Follow these rules when translating the formula:
#         - Retain the Tableau formula column references in the Snowflake SQL translation but remove the brackets.
#         - The Snowflake SQL formula should be a valid Snowflake SQL SELECT expression.
#         - Omit the SELECT keyword in the Snowflake SQL formula.
#         - The Snowflake SQL formula must be syntactically correct to use in a SELECT clause.
#         - If the Tableau formula cannot be translated, provide an explanation."""
#         self.user_prompt = f"Here is the Tableau calculated field formula: \n{formula}"
#         self.response_schema = {
#             "type": "json",
#             "schema": {
#                 "type": "object",
#                 "properties": {
#                     "type": {"type": "string",
#                               "description": "The type of response. Either 'formula' or 'explanation'."},
#                     "value": {"type": "string",
#                               "description": "The translated formula or explanation."},
#                 },
#                 "required": [
#                     "type",
#                     "value",
#                 ],
#             }
#         }
#         self.model = 'claude-3-5-sonnet'
#         self.temperature = 0.0
#         self.max_tokens = 1000
#         self.prompt=[
#             {"role": "system", "content": self.system_prompt},
#             {"role": "user", "content": self.user_prompt}
#             ]
#         self.options = CompleteOptions(
#                 max_tokens=self.max_tokens,
#                 temperature=self.temperature,
#                 response_format=self.response_schema
#             )

class TableauCalculationPrompt():
    def __init__(self,
                 alias,
                 formula,) -> None:
        self.system_prompt = f"""You are an Analyst tasked with translating a Tableau data source into a semantic model for Snowflake.
        You will be asked to translate a Tableau calculated field to the SQL equivalent for Snowflake.
        You will receive the following for the Tableau calculation:
        - field name
        - calculation formula

        You are to translate the Tableau formula to a Snowflake SQL SELECT clause
        AND determine if the field is pertinent for a semantic model.
        The formula itself may suggest if the field is just for aesthetics in Tableau vs. a field that has value for a semantic model.

        The Tableau formula contains column references enclosed with brackets like [<table_name>.<column_name>].
        If the Tableau formula cannot be translated, provide an explanation.
        Follow these rules when translating the formula:
        - Retain the Tableau formula column references exactly as shown in the formula but remove the brackets. Do not omit any part of the column reference.
        - The Snowflake SQL formula should be a valid Snowflake SQL SELECT expression. But omit the SELECT keyword in the Snowflake SQL formula.
        - The Snowflake SQL formula must be syntactically correct to use in a Snowflake SELECT clause like 'SELECT <formula>'.
        - If the Tableau formula cannot be translated, provide an explanation and omit the translated formula in the response.
        - If the formula and name are not relevant for a semantic model, omit the translated formula in the response.

        Mark the calculation as valid = False and provide an explanation if it is not pertinent or relevant for a semantic model."""
        self.user_prompt = f"""Here is the Tableau calculated field information:
        Field Name: {alias}
        Formula: {formula}"""
        self.response_schema = {
            "type": "json",
            "schema": {
                "type": "object",
                "properties": {
                    "valid": {"type": "boolean",
                              "description": "true or false indicating whether the formula can be translated to Snowflake SQL AND if it's pertinent to a semantic model."},
                    "translated_formula": {"type": "string",
                              "description": "The translated formula for the semantic model."},
                    "explanation": {"type": "string",
                              "description": "Explanation if the formula cannot be translated. Otherwise, empty string."},
                },
                "required": [
                    "valid",
                    "explanation",
                ],
            }
        }
        self.model = 'claude-3-5-sonnet'
        self.temperature = 0.0
        self.max_tokens = 1000
        self.prompt=[
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content": self.user_prompt}
            ]
        self.options = CompleteOptions(
                max_tokens=self.max_tokens,
                temperature=self.temperature,
                response_format=self.response_schema
            )

class DDLCorrectionPrompt():
    def __init__(self,
                 ddl,
                 error,
                 ) -> None:
        self.system_prompt = f"""You are an Analyst tasked with creating a semantic model in Snowflake.
        You will be given SQL DDL to create the Snowflake semantic model and an error received when this DDL is run.
        You are to re-write and return the SQL DDL to resolve the error.
        In addition, provide an explanation of the changes made to the DDL to correct the error.
        If the DDL is correct, provide an explanation of why the DDL is correct.
        If the error cannot be corrected, provide an explanation of why the error cannot be corrected.

        Here is the documentation for creating a semantic model in Snowflake:
        Within a semantic view, you define logical tables that typically correspond to business entities,
        such as customers, orders, or suppliers.
        You can define relationships between logical tables through joins on shared keys,
        enabling you to analyze data across entities (as you would when joining database tables).

        Using logical tables, you can define:

        Facts: Facts are row-level attributes in your data model that represent specific business events or transactions.
        While facts can be defined using aggregates from more detailed levels of data (such as SUM(t.x) where t represents data at a more detailed level),
        they are always presented as attributes at the individual row level of the logical table.
        Facts capture “how much” or “how many” at the most granular level, such as individual sales amounts, quantities purchased, or costs.
        It's important to note that facts typically function as “helper” concepts within the semantic view to help construct dimensions and metrics.

        Metrics: Metrics are quantifiable measures of business performance calculated by aggregating facts or other columns from the same table (using functions like SUM, AVG, and COUNT) across multiple rows.
        They transform raw data into meaningful business indicators, often combining multiple calculations in complex formulas.
        Metrics represent the KPIs in reports and dashboards that drive business decision-making.
        Metrics contain an explicit aggregation function (such as SUM, AVG, COUNT, etc.).

        Dimensions: Dimensions represent categorical attributes.
        They provide the contextual framework that gives meaning to metrics by grouping data into meaningful categories.
        They answer “who,” “what,” “where,” and “when” questions, such as purchase date, customer details, product category, or location.
        Typically text-based or hierarchical, dimensions enable users to filter, group, and analyze data from multiple perspectives.

        Here is an example of semantic model DDL in Snowflake:
        CREATE SEMANTIC VIEW tpch_rev_analysis

        TABLES (
            orders AS SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS
            PRIMARY KEY (o_orderkey)
            WITH SYNONYMS ('sales orders')
            COMMENT = 'All orders table for the sales domain',
            customers AS SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER
            PRIMARY KEY (c_custkey)
            COMMENT = 'Main table for customer data',
            line_items AS SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.LINEITEM
            PRIMARY KEY (l_orderkey, l_linenumber)
            COMMENT = 'Line items in orders'
        )

        RELATIONSHIPS (
            orders_to_customers AS
            orders (o_custkey) REFERENCES customers,
            line_item_to_orders AS
            line_items (l_orderkey) REFERENCES orders
        )

        FACTS (
            line_items.line_item_id AS CONCAT(l_orderkey, '-', l_linenumber),
            orders.count_line_items AS COUNT(line_items.line_item_id),
            line_items.discounted_price AS l_extendedprice * (1 - l_discount)
            COMMENT = 'Extended price after discount'
        )

        DIMENSIONS (
            customers.customer_name AS customers.c_name
            WITH SYNONYMS = ('customer name')
            COMMENT = 'Name of the customer',
            orders.order_date AS o_orderdate
            COMMENT = 'Date when the order was placed',
            orders.order_year AS YEAR(o_orderdate)
            COMMENT = 'Year when the order was placed'
        )

        METRICS (
            customers.customer_count AS COUNT(c_custkey)
            COMMENT = 'Count of number of customers',
            orders.order_average_value AS AVG(orders.o_totalprice)
            COMMENT = 'Average order value across all orders',
            orders.average_line_items_per_order AS AVG(orders.count_line_items)
            COMMENT = 'Average number of line items per order'
        )

        COMMENT = 'Semantic view for revenue analysis'
        """
        self.user_prompt = f"""Here is the DDL and corresponding error to correct:
        Semantic View DDL:
        {ddl}
        Error:
        {error}"""
        self.response_schema = {
            "type": "json",
            "schema": {
                "type": "object",
                "properties": {
                    "ddl": {"type": "string",
                              "description": "The corrected Semantic View SQL DDL."},
                    "explanation": {"type": "string",
                              "description": "The explanation of the correction if the DDL can be corrected. Otherwise, an explanation as to why the DDL cannot be corrected."},
                },
                "required": [
                    "ddl",
                    "explanation",
                ],
            }
        }
        self.model = 'claude-3-5-sonnet'
        self.temperature = 0.0
        self.max_tokens = 4000
        self.prompt=[
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content": self.user_prompt}
            ]
        self.options = CompleteOptions(
                max_tokens=self.max_tokens,
                temperature=self.temperature,
                response_format=self.response_schema
            )
