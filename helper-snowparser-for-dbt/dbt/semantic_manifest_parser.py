#!/usr/bin/env python3
"""
Semantic Manifest Parser

This script parses a dbt semantic_manifest.json file into structured Python class objects.
It creates a hierarchy of classes representing semantic models, dimensions, measures, and entities.
"""

import json
import yaml
import time
import requests
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Union
from pathlib import Path

from snowflake.connector import connect, DictCursor
from snowflake.connector.connection import SnowflakeConnection

# Stored procedure does not maintain specific directory structure
# In SPROC, .py files are imported as modules sans directory structure.
# If running locally with directory structure intact, import dbt.semantics_schema
try:
    import dbt.semantics_schema as semantics_schema
except ImportError:
    import semantics_schema as semantics_schema



@dataclass
class NodeRelation:
    """Database relation information for semantic models"""
    alias: str
    schema_name: str
    database: str
    relation_name: str

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'NodeRelation':
        return cls(
            alias=data['alias'],
            schema_name=data['schema_name'],
            database=data['database'],
            relation_name=data['relation_name']
        )

@dataclass
class NodeModelColumns:
    """Column metadata of a model node that corresponds to a SQL model as shown in manifest.json

    Will not be present for semantic_manifest.json"""
    name: str
    description: str

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'NodeModelColumns':

        return cls(
            name=data['name'],
            description=data['description'],
        )

@dataclass
class NodeModel:
    """Node of type model that corresponds to a SQL model as shown in manifest.json

    Will not be present for semantic_manifest.json"""
    name: str
    description: str
    relation_name: str
    resource_type: str
    materialized: str # Will not support `ephemeral` nor `materialzied_view`
    manifest_columns: List[NodeModelColumns]


    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'NodeRelation':

        if 'columns' in data:
            # In manifest.json, columns are nested inside of a dictionary with column names as keys
            columns = [column_object for column_object in data['columns'].values()]
        else:
            columns = []

        return cls(
            name=data['name'],
            description=data['description'],
            relation_name=data['relation_name'],
            resource_type=data['resource_type'],
            materialized=data['config'].get('materialized', 'table'),
            manifest_columns=[NodeModelColumns.from_dict(column) for column in columns],
        )

    def get_snow_columns(self, connection: SnowflakeConnection) -> List[str]:
        """Returns a list of column names that are present in the Snowflake table"""

        columns = []
        try:
            cur = connection.cursor(DictCursor)
            cur.execute(f"SHOW COLUMNS IN {self.relation_name}")
            for col in cur:
                columns.append(col['column_name'])
        finally:
            cur.close()

        return columns

    def convert_to_object(self, connection: SnowflakeConnection, parse_snowflake_columns: bool = True):
        """Iterates over table columns to produce object of table, column, and description.

        Description is only passed if found in the manifest.json"""

        return_object = {
            'table_name': self.relation_name,
            'name': self.name,
            'description': self.description,
            'columns': []
        }

        if parse_snowflake_columns: # Columns will consist of those associated to Snowflake table
            for column in self.get_snow_columns(connection):
                description = next((c.description for c in self.manifest_columns if c.name.lower() == column.lower()), None)
                return_object['columns'].append({
                    'column_name': column,
                    'description': description
                })
        else: # Columns will consist of those associated to dbt models
            for column in self.manifest_columns:
                return_object['columns'].append({
                    'column_name': column.name,
                    'description': column.description
                })

        return return_object



@dataclass
class Entity:
    """Entity class representing join keys and relationships"""
    name: str
    type: str  # primary, foreign, unique, natural
    description: Optional[str] = None
    role: Optional[str] = None
    expr: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    label: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Entity':
        return cls(
            name=data['name'],
            type=data['type'],
            description=data.get('description'),
            role=data.get('role'),
            expr=data.get('expr') or data['name'], # if expr is not provided, use the name as the expr
            metadata=data.get('metadata'),
            label=data.get('label'),
        )

    def convert(self) -> dict[str, Any]:
        return semantics_schema.Dimension(
            name=self.name,
            description=self.description,
            expr=self.expr,
            unique=True if (self.type=="primary" or self.type=="Unique") else False,
            data_type=semantics_schema.DataType.TEXT, # This is an assumption, we need to figure out how to handle this
            synonyms=[self.label] if self.label else None,
        ).__dict__

    def to_dict(self) -> Dict[str, Any]:
        x = self.convert()
        d = {}
        for key, value in x.items():
            if value is not None and value != []:
                d[key] = value
        return d


@dataclass
class Measure:
    """Measure class representing aggregatable fields"""
    name: str
    agg: str  # sum, count, avg, min, max, count_distinct, percentile, etc.
    description: Optional[str] = None
    create_metric: bool = False
    expr: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    non_additive_dimension: Optional[str] = None
    agg_time_dimension: Optional[str] = None
    label: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Measure':
        return cls(
            name=data['name'],
            agg=data['agg'],
            description=data.get('description'),
            create_metric=data.get('create_metric', False),
            expr=data.get('expr') or data['name'],
            metadata=data.get('metadata'),
            non_additive_dimension=data.get('non_additive_dimension'),
            agg_time_dimension=data.get('agg_time_dimension'),
            label=data.get('label'),
        )

    def create_metric_from_fact(self) -> semantics_schema.Metric:
        agg = None
        agg_type = semantics_schema.AggregationType.__members__.get(self.agg.lower(), None)
        if agg_type is not None:
            agg = agg_type.value

        if agg is not None and self.create_metric:
            expr = f"{agg}({self.name})"
            name = f"{agg}_{self.name}"
            return semantics_schema.Metric(
                name=name,
                description=self.description,
                expr=expr,
                data_type=semantics_schema.DataType.NUMBER,
            ).__dict__



    def convert(self) -> dict[str, Any]:
        return semantics_schema.Fact(
            name=self.name,
            description=self.description,
            expr=self.expr,
            data_type=semantics_schema.DataType.NUMBER, # This is an assumption, we need to figure out how to handle this
            synonyms=[self.label] if self.label else None,
            default_aggregation=semantics_schema.AggregationType[self.agg.lower()].value if self.agg.lower() in semantics_schema.AggregationType.__members__ else None,
        ).__dict__

    def to_dict(self) -> Dict[str, Any]:
        x = self.convert()
        d = {}
        for key, value in x.items():
            if value is not None and value != []:
                d[key] = value
        return d


@dataclass
class Dimension:
    """Dimension class representing categorical and time dimensions"""
    name: str
    type: str  # categorical, time
    description: Optional[str] = None
    is_partition: bool = False
    expr: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    label: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Dimension':
        return cls(
            name=data['name'],
            type=data['type'],
            description=data.get('description'),
            is_partition=data.get('is_partition', False),
            expr=data.get('expr') or data['name'],
            metadata=data.get('metadata'),
            label=data.get('label'),
        )

    def convert(self) -> dict[str, Any]:

        if self.type == "time":
            return semantics_schema.TimeDimension(
                name=self.name,
                description=self.description,
                expr=self.expr,
                data_type=semantics_schema.DataType.DATE, # This is an assumption, we need to figure out how to handle this
                synonyms=[self.label] if self.label else None,
            ).__dict__
        else:
            return semantics_schema.Dimension(
                name=self.name,
                description=self.description,
                expr=self.expr,
                unique=True if (self.type=="primary" or self.type=="Unique") else False,
                data_type=semantics_schema.DataType.TEXT, # This is an assumption, we need to figure out how to handle this
                synonyms=[self.label] if self.label else None,
            ).__dict__

    def to_dict(self) -> Dict[str, Any]:
        x = self.convert()
        d = {}
        for key, value in x.items():
            if value is not None and value != []:
                d[key] = value
        return d


@dataclass
class Defaults:
    """Default configuration for semantic models"""
    agg_time_dimension: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, Any]]) -> Optional['Defaults']:
        if data is None:
            return None
        return cls(
            agg_time_dimension=data.get('agg_time_dimension')
        )


@dataclass
class SemanticModel:
    """Semantic model class representing a single semantic model"""
    name: str
    description: str
    node_relation: NodeRelation
    entities: List[Entity] = field(default_factory=list)
    measures: List[Measure] = field(default_factory=list)
    dimensions: List[Dimension] = field(default_factory=list)
    defaults: Optional[Defaults] = None
    primary_entity: Optional[str] = None
    label: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SemanticModel':
        """Used if loading from semantic_manifest.json"""
        return cls(
            name=data['name'],
            description=data.get('description', ''),
            node_relation=NodeRelation.from_dict(data['node_relation']),
            entities=[Entity.from_dict(entity) for entity in data.get('entities', [])],
            measures=[Measure.from_dict(measure) for measure in data.get('measures', [])],
            dimensions=[Dimension.from_dict(dimension) for dimension in data.get('dimensions', [])],
            defaults=Defaults.from_dict(data.get('defaults')),
            primary_entity=data.get('primary_entity'),
            label=data.get('label'),
            metadata=data.get('metadata'),
        )

    def split_facts_and_metrics(self) -> dict[str, list[dict[str, Any]]]:

        metrics = []
        facts = []

        for measure in self.measures:
            fact = measure.to_dict()
            facts.append(fact)
            if measure.create_metric_from_fact():
                metrics.append(measure.create_metric_from_fact())

        return {
            "metrics": metrics,
            "facts": facts
        }

    def convert(self) -> dict[str, Any]:

        # dimensions could be time_dimensions or dimensions so separate out first
        dimensions = [entity.to_dict() for entity in self.entities] or []
        time_dimensions = []
        for dim in self.dimensions:

            new_dim = dim.to_dict()

            if dim.type == 'time':
                time_dimensions.append(new_dim)
            else:
                dimensions.append(new_dim)

        db, schema, table = self.node_relation.relation_name.split(".")

        facts_and_metrics = self.split_facts_and_metrics()

        return semantics_schema.Table(
            name=self.name,
            description=self.description,
            base_table=semantics_schema.BaseTable(db, schema, table).__dict__,
            dimensions=dimensions,
            time_dimensions=time_dimensions,
            facts=facts_and_metrics['facts'],
            metrics=facts_and_metrics['metrics'],
            primary_key=semantics_schema.PrimaryKey(columns=[entity.name for entity in self.entities if entity.type == "primary"]).__dict__,
        ).__dict__

    def to_dict(self) -> Dict[str, Any]:
        x = self.convert()
        d = {}
        for key, value in x.items():
            if value is not None and value != []:
                d[key] = value
        return d

@dataclass
class SimpleMetric:
    """Global metric in semantic manifest marked with type=simple"""
    name: str
    type: str  # simple
    measure: Measure
    description: Optional[str] = None
    label: Optional[str] = None
    fill_nulls_with: Optional[str|int] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SimpleMetric':
        return cls(
            name=data['name'],
            type="simple",
            measure=data['measure'], # Measure class object
            description=data.get('description'),
            label=data.get('label'),
            fill_nulls_with=data.get('fill_nulls_with'),
        )

    def convert(self) -> dict[str, Any]:


        agg = self.measure.agg
        measure = self.measure.name

        if self.fill_nulls_with is not None: # fill_nulls_with can be 0
            expr = f"{agg}(COALESCE({measure}, {self.fill_nulls_with}))"
        else:
            expr = f"{agg}({measure})"

        return semantics_schema.GlobalMetric(
            name=self.name,
            description=self.description,
            expr=expr,
            synonyms=[self.label] if self.label else None,
        ).__dict__

    def to_dict(self) -> Dict[str, Any]:
        x = self.convert()
        d = {}
        for key, value in x.items():
            if value is not None and value != []:
                d[key] = value
        return d


@dataclass
class DerivedMetric:
    """Global metric in semantic manifest marked with type=derived"""
    name: str
    type: str  # derived
    metric_specs: List[dict[str, Any]]
    expr: Optional[str] = None
    description: Optional[str] = None
    label: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DerivedMetric':
        return cls(
            name=data['name'],
            type="simple",
            metric_specs=data['metric_specs'],
            expr=data.get('expr'),
            description=data.get('description'),
            label=data.get('label'),
        )

    def convert(self) -> dict[str, Any]:
        expr = self.expr
        for spec in self.metric_specs:
            if spec.get('alias') is not None:
                expr = expr.replace(spec['alias'], spec['name'])


        return semantics_schema.GlobalMetric(
            name=self.name,
            description=self.description,
            expr=expr,
            synonyms=[self.label] if self.label else None,
        ).__dict__

    def to_dict(self) -> Dict[str, Any]:
        x = self.convert()
        d = {}
        for key, value in x.items():
            if value is not None and value != []:
                d[key] = value
        return d


@dataclass
class RatioMetric:
    """Global metric in semantic manifest marked with type=ratio"""
    name: str
    type: str  # derived
    numerator_specs: str|dict[str, Any]
    denominator_specs: str|dict[str, Any]
    description: Optional[str] = None
    label: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DerivedMetric':
        return cls(
            name=data['name'],
            type="simple",
            numerator_specs=data['numerator_specs'],
            denominator_specs=data['denominator_specs'],
            description=data.get('description'),
            label=data.get('label'),
        )

    def convert(self) -> dict[str, Any]:
        # numerator and denominator can be just the name or a dict of properties
        if isinstance(self.numerator_specs, dict):
            numerator = self.numerator_specs['name']
        else:
            numerator = self.numerator_specs
        if isinstance(self.denominator_specs, dict):
            denominator = self.denominator_specs['name']
        else:
            denominator = self.denominator_specs

        expr = f"CAST({numerator} AS DOUBLE) / CAST(NULLIF({denominator}, 0) AS DOUBLE)"


        return semantics_schema.GlobalMetric(
            name=self.name,
            description=self.description,
            expr=expr,
            synonyms=[self.label] if self.label else None,
        ).__dict__

    def to_dict(self) -> Dict[str, Any]:
        x = self.convert()
        d = {}
        for key, value in x.items():
            if value is not None and value != []:
                d[key] = value
        return d


@dataclass
class Manifest:
    """Main manifest class containing all semantic models and metrics.

    Will accept both semantic_manifest.json and manifest.json."""
    semantic_models: List[SemanticModel] = field(default_factory=list)
    metrics: List[SimpleMetric|DerivedMetric] = field(default_factory=list)
    node_models: List[NodeModel] = field(default_factory=list) # Only present in manifest.json
    selected_models: List[str] = field(default_factory=list)
    connection: Optional[SnowflakeConnection] = None

    @staticmethod
    def flatten_manifest_contents(data: Dict[str, Any], key: str) -> Any:
        if key in data:
            if isinstance(data[key], list):
                return data[key]
            elif isinstance(data[key], dict):
                return [item for item in data[key].values()]
            else:
                return []
        else:
            return []

    @classmethod
    def from_dict(cls, connection: SnowflakeConnection, data: Dict[str, Any], selected_models: List[str] = []) -> 'Manifest':

        # Ingest models from manifest.json
        node_models = []
        for model in cls.flatten_manifest_contents(data, 'nodes'):
            # ephermeral materializations are not materialized in Snowflake
            # materialized_view materializations are not supported in Snowflake; dynamic_tables are used instead
            # Remaining acceptable materializations are: table, view, incremental, and dynamic_table
            if model['resource_type'] == 'model' and model['config'].get('materialized') not in ['ephemeral', 'materialized_view']:
                if len(selected_models) == 0 or model['name'] in selected_models:
                    node_models.append(NodeModel.from_dict(model))

        semantic_models = [SemanticModel.from_dict(model) for model in cls.flatten_manifest_contents(data, 'semantic_models')]
        metrics_metadata = cls.flatten_manifest_contents(data, 'metrics')

        # Get all measures from semantic models to augment automated  metrics in next section
        measures = []
        if semantic_models:
            for model in semantic_models:
                measures.extend(model.measures)

        metrics = []
        if metrics_metadata:
            for metric in metrics_metadata:
                if metric['type'] == "simple":
                    measure = next((m for m in measures if m.name == metric['type_params']['measure']['name']), None)
                    if measure:
                        metric['measure'] = measure
                        metric['fill_nulls_with'] = metric.get('type_params').get('measure').get('fill_nulls_with')
                        metrics.append(SimpleMetric.from_dict(metric))
                elif metric['type'] == "derived":
                    metric['expr'] = metric.get('type_params').get('expr')
                    metric['metric_specs'] = metric.get('type_params').get('metrics')
                    metrics.append(DerivedMetric.from_dict(metric))
                elif metric['type'] == "ratio":
                    metric['numerator_specs'] = metric.get('type_params').get('numerator')
                    metric['denominator_specs'] = metric.get('type_params').get('denominator')
                    metrics.append(RatioMetric.from_dict(metric))
                else:
                    continue


        return cls(
            semantic_models=semantic_models,
            metrics=metrics,
            node_models=node_models,
            selected_models=selected_models,
            connection=connection,
        )

    @staticmethod
    def create_dbt_job_api(
        account_id: str | int,
        api_key: str,
        project_id: str | int,
        environment_id: str | int,
        execute_steps: List[str] = ["dbt parse --no-partial-parse"],
        name: str = "snowflake_parser"
    ) -> str | tuple[int, str]:
        """Creates a dbt job via API"""
        url = f"https://cloud.getdbt.com/api/v2/accounts/{account_id}/jobs/"

        headers = {
            'Authorization': f'Token {api_key}',
            'Content-Type': 'application/json'
        }

        data = {
            "account_id": int(account_id),
            "environment_id": int(environment_id),
            "project_id": int(project_id),
            "execute_steps": execute_steps,
            "name": name
        }

        response = requests.post(url, headers=headers, json=data)

        if response.status_code == 201:
            run_data = response.json().get('data')
            job_id = run_data.get('id')
            return job_id
        else:
            return response.status_code, response.text

    @staticmethod
    def get_run_status_api(
        account_id: str | int,
        api_key: str,
        run_id: str | int
    ) -> bool | tuple[int, str]:
        """Gets the status of a previously created dbt job"""
        url = f"https://cloud.getdbt.com/api/v2/accounts/{account_id}/runs/{run_id}/"

        headers = {
            'Authorization': f'Token {api_key}',
            'Content-Type': 'application/json'
        }

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            is_complete = response.json().get('data').get('is_complete')
            return is_complete
        else:
            return response.status_code, response.text


    @staticmethod
    def run_dbt_job_api(
        account_id: str | int,
        api_key: str,
        job_id: str | int,
    ) -> str | tuple[int, str]:
        """Runs a previously created dbt job and returns the run_id"""
        url = f"https://cloud.getdbt.com/api/v2/accounts/{account_id}/jobs/{job_id}/run/"

        headers = {
            'Authorization': f'Token {api_key}',
            'Content-Type': 'application/json'
        }

        data = {
            "cause": "Triggered via Snowflake parser"
        }

        response = requests.post(url, headers=headers, json=data)


        if response.status_code == 200:
            run_data = response.json().get('data')
            run_id = run_data.get('id')
            return run_id
        else:
            return response.status_code, response.text

    @classmethod
    def artifact_from_api(
        cls,
        account_id: str | int,
        job_run_id: str | int,
        api_key: str,
        artifact_path: str = 'manifest.json',
        selected_models: List[str] = []
    ) -> 'Manifest':
        """GET artifact from a previously run dbt job"""
        url = f"https://cloud.getdbt.com/api/v2/accounts/{account_id}/runs/{job_run_id}/artifacts/{artifact_path}"

        headers = {
            'Authorization': f'Token {api_key}',
            'Content-Type': 'application/json'
        }

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            return cls.from_dict(data, selected_models)
        else:
            return response.status_code, response.text

    @classmethod
    def manifest_from_api(
        cls,
        account_id: str | int,
        api_key: str,
        project_id: str | int,
        environment_id: str | int,
        selected_models: List[str] = [],
    ) -> 'Manifest':
        """Orchestrates creating and obtaining a manifest.json or semantic_manifest.json from dbt API"""

        job_id = cls.create_dbt_job_api(account_id, api_key, project_id, environment_id)
        if isinstance(job_id, tuple):
            raise Exception(f"Error creating dbt job: {job_id[1]}")

        job_run_id = cls.run_dbt_job_api(account_id, api_key, job_id)
        if isinstance(job_run_id, tuple):
            raise Exception(f"Error running dbt job: {job_run_id[1]}")

        max_attempts = 6
        attempt = 0

        while attempt < max_attempts:
            is_complete = cls.get_run_status_api(account_id, api_key, job_run_id)
            if is_complete:
                return cls.artifact_from_api(account_id, job_run_id, api_key, selected_models=selected_models)

            time.sleep(10)
            attempt += 1

        raise Exception("Max attempts reached while waiting for dbt job to complete")

    @classmethod
    def manifest_from_json_file(cls, connection: SnowflakeConnection, file_path: Union[str, Path], selected_models: List[str] = []) -> 'Manifest':
        """Load manifest from a local JSON file or staged file if running in Snowflake"""

        if file_path.startswith('https://') or file_path.startswith('@'):
            from snowflake.snowpark.files import SnowflakeFile # type: ignore

            if file_path.startswith('https://'):
                open_file = SnowflakeFile.open(file_path)
            else:
                open_file = SnowflakeFile.open(file_path, require_scoped_url = False)

        else:
            open_file = open(file_path, 'r')

        data = json.load(open_file)

        return cls.from_dict(connection, data, selected_models)



    def get_relationship_spec(self, foreign_key_name: str, model_name: str) -> dict[str, Any]:
        """Get the join key for a foreign key"""
        for model in self.semantic_models:
            if model.name != model_name: # Don't want to join table to itself
                for entity in model.entities:
                    # MetricFlow determines if foreign key is unique at inference time.
                    # There is no way to determine this from semantic_manifest.json
                    # So we are assuming the foreign key is not unique and is a many_to_one relationship
                    if entity.name == foreign_key_name and entity.type in ["primary", "unique", "natural"]:
                        name = f"{model_name}_to_{entity.name}"
                        return {
                            "name": name,
                            "left_table": model_name,
                            "right_table": model.name,
                            "join_type": semantics_schema.JoinType.LEFT_OUTER.value,
                            "relationship_type": semantics_schema.RelationshipType.MANY_TO_ONE.value,
                            "relationship_columns": [
                                {
                                    "left_column": foreign_key_name,
                                    "right_column": entity.name,
                                }
                            ]
                        }
                    else:
                        continue

    def get_relationships(self):
        """Get all relationships from the semantic manifest"""
        relationships = []
        for model in self.semantic_models:
            for entity in model.entities:
                if entity.type == "foreign":
                    relationship = self.get_relationship_spec(foreign_key_name=entity.name, model_name = model.name)
                    if relationship is not None:
                        relationships.append(semantics_schema.Relationship(**relationship).__dict__)

        return relationships

    def extract_descriptions_from_models(self):
        """Extracts descriptions from node models where description is not present in semantic models

        Only needs to run if semantic models are present in the manifest.json."""
        if len(self.semantic_models) > 0:
            for model in self.node_models:
                for model_n, semantic_model in enumerate(self.semantic_models):
                    if semantic_model.node_relation.relation_name == model.relation_name:

                        if semantic_model.description is None and model.description is not None:
                            semantic_model.description = model.description

                        for column in model.manifest_columns:
                            for i, dimension in enumerate(semantic_model.dimensions):
                                if column.name == dimension.expr:
                                    if dimension.description is None and column.description is not None:
                                        self.semantic_models[model_n].dimensions[i].description = column.description
                                        break
                            for i, entity in enumerate(semantic_model.entities):
                                if column.name == entity.expr:
                                    if entity.description is None and column.description is not None:
                                        self.semantic_models[model_n].entities[i].description = column.description
                                        break
                            for i, measure in enumerate(semantic_model.measures):
                                if column.name == measure.expr:
                                    if measure.description is None and column.description is not None:
                                        self.semantic_models[model_n].measures[i].description = column.description
                                        break


    def convert(self, semantic_view_name: str = None, semantic_view_description: str = None, parse_snowflake_columns: bool = True) -> dict[str, Any]:
        """Converts Manifest to Semantic Models or Objects of table metadata.

        Only return a SemanticModel if semantic models are present
        Otherwise, we return just table and column names for remaining workflow steps

        Args:
            semantic_view_name: Name of the semantic view to be created.
            semantic_view_description: Description of the semantic view to be created.
            parse_snowflake_columns: Whether to parse snowflake columns.
                                     Only used if semantic models are not present in the manifest.json.
                                     If set to True, descriptions of Snowflake columns will be added to the metadata.

        Returns:
            SemanticModel or list of objects of table metadata.
        """
        if len(self.semantic_models) > 0:
            self.extract_descriptions_from_models()

            return semantics_schema.SemanticModel(
                name=semantic_view_name if semantic_view_name is not None else "<placeholder>",
                description=semantic_view_description if semantic_view_description is not None else "<placeholder>",
                tables=[model.to_dict() for model in self.semantic_models],
                metrics=[metric.to_dict() for metric in self.metrics],
                relationships=self.get_relationships(),
            ).__dict__
        else:
            return [model.convert_to_object(connection=self.connection, parse_snowflake_columns=parse_snowflake_columns) for model in self.node_models]

    def to_dict(self, semantic_view_name: str, semantic_view_description: str) -> Dict[str, Any]:
        x = self.convert(semantic_view_name, semantic_view_description)
        d = {}
        for key, value in x.items():
            if value is not None and value != []:
                d[key] = value
        return d

    def generate_yaml(self, semantic_view_name: str, semantic_view_description: str) -> str:
        if len(self.semantic_models) == 0:
            raise Exception("Semantic models are not present in the manifest.json. Please use the convert method to get a list of tables and columns."
                            "If using the stored procedure, please use the SNOWPARSER_DBT_GET_OBJECTS stored procedure instead.")
        else:
            return yaml.dump(self.to_dict(semantic_view_name, semantic_view_description),
                             default_flow_style=False, sort_keys=False)

    def get_all_measures(self) -> List[Measure]:
        return [measure for model in self.semantic_models for measure in model.measures]

    def get_all_metrics(self) -> List[SimpleMetric]:
        return [metric for metric in self.metrics]

    def get_all_semantic_models(self) -> List[SemanticModel]:
        return self.semantic_models
