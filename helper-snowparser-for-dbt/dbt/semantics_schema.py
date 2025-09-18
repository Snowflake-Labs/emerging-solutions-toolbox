from dataclasses import dataclass, field
from typing import List, Optional, Union
from enum import Enum



class JoinType(Enum):
    """Enumeration for relationship join types"""
    LEFT_OUTER = "left_outer"
    RIGHT_OUTER =  None # Not supported yet
    INNER = "inner"
    FULL_OUTER =  None # Not supported yet


class RelationshipType(Enum):
    """Enumeration for relationship types"""
    ONE_TO_ONE = "one_to_one" # Not supported yet
    ONE_TO_MANY = None # Not supported yet
    MANY_TO_ONE = "many_to_one"
    MANY_TO_MANY = None # Not supported yet


class DataType(Enum):
    """Enumeration for data types"""
    NUMBER = "NUMBER"
    TEXT = "TEXT"
    DATE = "DATE"
    BOOLEAN = "BOOLEAN"
    TIMESTAMP = "TIMESTAMP"


class AggregationType(Enum):
    """Enumeration for aggregation types
    dbt equivalent = SNOWFLAKE SQL
    """
    sum = "sum"
    average = "avg"
    min = "min"
    max = "max"
    median = "median"
    count_distinct = "count_distinct"
    percentile = None # not implemented yet
    sum_boolean = "countif"
    count = "count"


@dataclass
class BaseTable:
    """Represents the base table configuration"""
    database: Optional[str] = None
    schema: Optional[str] = None
    table: Optional[str] = None


@dataclass
class PrimaryKey:
    """Represents a primary key configuration"""
    columns: List[str]


@dataclass
class Dimension:
    """Represents a dimension in a semantic table"""
    name: str
    description: str
    expr: str
    data_type: Union[DataType, str]
    synonyms: Optional[List[str]] = None
    unique: Optional[bool] = None
    sample_values: Optional[List[str]] = None
    cortex_search_service_name: Optional[str] = None

    def __post_init__(self):
        if isinstance(self.data_type, DataType):
            try:
                self.data_type = self.data_type.value
            except ValueError:
                pass  # Keep as string if not in enum

        if isinstance(self.description, str):
            self.description = self.description.strip()


@dataclass
class TimeDimension:
    """Represents a time dimension in a semantic table"""
    name: str
    description: str
    expr: str
    data_type: Union[DataType, str]
    synonyms: Optional[List[str]] = None
    sample_values: Optional[List[str]] = None

    def __post_init__(self):
        if isinstance(self.data_type, DataType):
            try:
                self.data_type = self.data_type.value
            except ValueError:
                pass  # Keep as string if not in enum

        if isinstance(self.description, str):
            self.description = self.description.strip()


@dataclass
class Fact:
    """Represents a measure in a semantic table"""
    name: str
    description: str
    expr: str
    data_type: Union[DataType, str]
    synonyms: Optional[List[str]] = None
    default_aggregation: Optional[Union[AggregationType, str]] = None
    sample_values: Optional[List[str]] = None

    def __post_init__(self):
        if isinstance(self.data_type, DataType):
            try:
                self.data_type = self.data_type.value
            except ValueError:
                pass  # Keep as string if not in enum

        if isinstance(self.default_aggregation, AggregationType):
            try:
                self.default_aggregation = self.default_aggregation.value
            except ValueError:
                pass  # Keep as string if not in enum

        if isinstance(self.description, str):
            self.description = self.description.strip()


@dataclass
class Metric:
    """Represents a metric in a semantic table"""
    name: str
    expr: str
    description: str
    data_type: Union[DataType, str]
    synonyms: Optional[List[str]] = None
    sample_values: Optional[List[str]] = None

    def __post_init__(self):

        if isinstance(self.data_type, DataType):
            try:
                self.data_type = self.data_type.value
            except ValueError:
                pass  # Keep as string if not in enum
        if isinstance(self.description, str):
            self.description = self.description.strip()

@dataclass
class GlobalMetric:
    """Represents a global metric outside of the scope of a single semantic table"""
    name: str
    expr: str
    description: str
    synonyms: Optional[List[str]] = None
    sample_values: Optional[List[str]] = None

    def __post_init__(self):

        if isinstance(self.description, str):
            self.description = self.description.strip()


@dataclass
class Filter:
    """Represents a filter in a semantic table"""
    name: str
    description: str
    expr: str
    synonyms: Optional[List[str]] = None

    def __post_init__(self):
        if isinstance(self.description, str):
            self.description = self.description.strip()


@dataclass
class RelationshipColumn:
    """Represents a column mapping in a relationship"""
    left_column: str
    right_column: str


@dataclass
class Relationship:
    """Represents a relationship between semantic tables"""
    name: str
    left_table: str
    right_table: str
    join_type: Union[JoinType, str]
    relationship_type: Union[RelationshipType, str]
    relationship_columns: List[dict[str, str]]

    def __post_init__(self):
        if isinstance(self.join_type, JoinType):
            try:
                self.join_type = self.join_type.value
            except ValueError:
                pass  # Keep as string if not in enum

        if isinstance(self.relationship_type, RelationshipType):
            try:
                self.relationship_type = self.relationship_type.value
            except ValueError:
                pass  # Keep as string if not in enum


@dataclass
class Table:
    """Represents a semantic table"""
    name: str
    description: str
    base_table: BaseTable
    dimensions: Optional[List[Dimension]] = field(default_factory=list)
    time_dimensions: Optional[List[TimeDimension]] = field(default_factory=list)
    facts: Optional[List[Fact]] = field(default_factory=list)
    metrics: Optional[List[Metric]] = field(default_factory=list)
    filters: Optional[List[Filter]] = field(default_factory=list)
    primary_key: Optional[PrimaryKey] = None

    def __post_init__(self):
        if isinstance(self.description, str):
            self.description = self.description.strip()


@dataclass
class SemanticModel:
    """Represents a complete semantic model"""
    name: str
    description: str
    tables: List[Table] = field(default_factory=list)
    metrics: List[Metric] = field(default_factory=list)
    relationships: List[Relationship] = field(default_factory=list)

    def __post_init__(self):
        if isinstance(self.description, str):
            self.description = self.description.strip()

    def get_table(self, table_name: str) -> Optional[Table]:
        """Get a table by name"""
        for table in self.tables:
            if table.name == table_name:
                return table
        return None

    def get_relationship(self, relationship_name: str) -> Optional[Relationship]:
        """Get a relationship by name"""
        for relationship in self.relationships:
            if relationship.name == relationship_name:
                return relationship
        return None

    def get_dimension(self, table_name: str, dimension_name: str) -> Optional[Dimension]:
        """Get a dimension from a specific table"""
        table = self.get_table(table_name)
        if table and table.dimensions:
            for dimension in table.dimensions:
                if dimension.name == dimension_name:
                    return dimension
        return None

    def get_fact(self, table_name: str, measure_name: str) -> Optional[Fact]:
        """Get a fact from a specific table"""
        table = self.get_table(table_name)
        if table and table.measures:
            for measure in table.measures:
                if measure.name == measure_name:
                    return measure
        return None

    def get_time_dimension(self, table_name: str, time_dimension_name: str) -> Optional[TimeDimension]:
        """Get a time dimension from a specific table"""
        table = self.get_table(table_name)
        if table and table.time_dimensions:
            for time_dimension in table.time_dimensions:
                if time_dimension.name == time_dimension_name:
                    return time_dimension
        return None

    def get_metric(self, table_name: str, metric_name: str) -> Optional[Metric]:
        """Get a metric from a specific table"""
        table = self.get_table(table_name)
        if table and table.metrics:
            for metric in table.metrics:
                if metric.name == metric_name:
                    return metric
        return None

    def list_all_dimensions(self) -> List[tuple[str, Dimension]]:
        """List all dimensions across all tables with their table names"""
        all_dimensions = []
        for table in self.tables:
            if table.dimensions:
                for dimension in table.dimensions:
                    all_dimensions.append((table.name, dimension))
        return all_dimensions

    def list_all_facts(self) -> List[tuple[str, Fact]]:
        """List all facts across all tables with their table names"""
        all_measures = []
        for table in self.tables:
            if table.measures:
                for measure in table.measures:
                    all_measures.append((table.name, measure))
        return all_measures

    def list_all_time_dimensions(self) -> List[tuple[str, TimeDimension]]:
        """List all time dimensions across all tables with their table names"""
        all_time_dimensions = []
        for table in self.tables:
            if table.time_dimensions:
                for time_dimension in table.time_dimensions:
                    all_time_dimensions.append((table.name, time_dimension))
        return all_time_dimensions

    def list_all_metrics(self) -> List[tuple[str, Metric]]:
        """List all metrics across all tables with their table names"""
        all_metrics = []
        for table in self.tables:
            if table.metrics:
                for metric in table.metrics:
                    all_metrics.append((table.name, metric))
        return all_metrics
