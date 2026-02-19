"""Core semantic model types: facts, dimensions, measures, metrics, relationships."""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, Field


class DataType(StrEnum):
    STRING = "string"
    JSON = "json"
    INT = "int"
    FLOAT = "float"
    DATE = "date"
    TIME = "time"
    TIME_TZ = "time_tz"
    TIMESTAMP = "timestamp"
    TIMESTAMP_TZ = "timestamp_tz"
    BOOLEAN = "boolean"


class AggregationType(StrEnum):
    SUM = "sum"
    COUNT = "count"
    COUNT_DISTINCT = "count_distinct"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    ANY_VALUE = "any_value"
    MEDIAN = "median"
    MODE = "mode"
    LISTAGG = "listagg"


class JoinType(StrEnum):
    LEFT = "left"
    INNER = "inner"
    RIGHT = "right"
    FULL = "full"


class Cardinality(StrEnum):
    MANY_TO_ONE = "many-to-one"
    ONE_TO_ONE = "one-to-one"
    MANY_TO_MANY = "many-to-many"


class TimeGrain(StrEnum):
    YEAR = "year"
    QUARTER = "quarter"
    MONTH = "month"
    WEEK = "week"
    DAY = "day"
    HOUR = "hour"
    MINUTE = "minute"
    SECOND = "second"


class DataColumnRef(BaseModel):
    """Reference to a data object column by dataObject + column pair."""

    view: str | None = Field(None, alias="dataObject")
    column: str | None = None

    model_config = {"populate_by_name": True}


class DataObjectColumn(BaseModel):
    """A column within a data object (maps to a database column or expression)."""

    label: str
    code: str
    abstract_type: DataType = Field(alias="abstractType")
    sql_type: str | None = Field(None, alias="sqlType")
    sql_precision: int | None = Field(None, alias="sqlPrecision")
    sql_scale: int | None = Field(None, alias="sqlScale")
    comment: str | None = None

    model_config = {"populate_by_name": True}


class DataObjectJoin(BaseModel):
    """Join definition on a data object, connecting it to another data object."""

    join_type: Cardinality = Field(alias="joinType")
    join_to: str = Field(alias="joinTo")
    columns_from: list[str] = Field(alias="columnsFrom")
    columns_to: list[str] = Field(alias="columnsTo")
    secondary: bool = False
    path_name: str | None = Field(None, alias="pathName")

    model_config = {"populate_by_name": True}


class DataObject(BaseModel):
    """A database table or view with its columns and joins."""

    label: str
    code: str
    database: str
    schema_name: str = Field(alias="schema")
    columns: dict[str, DataObjectColumn] = {}
    joins: list[DataObjectJoin] = []
    comment: str | None = None

    @property
    def qualified_code(self) -> str:
        """Full qualified table reference: database.schema.code."""
        return f"{self.database}.{self.schema_name}.{self.code}"

    model_config = {"populate_by_name": True}


class Dimension(BaseModel):
    """A named dimension referencing a data object column."""

    label: str
    view: str = Field(alias="dataObject")
    column: str = ""
    result_type: DataType = Field(alias="resultType")
    time_grain: TimeGrain | None = Field(None, alias="timeGrain")
    format: str | None = None

    model_config = {"populate_by_name": True}


class FilterValue(BaseModel):
    """A typed value used in measure filters."""

    data_type: DataType = Field(alias="dataType")
    is_null: bool | None = Field(None, alias="isNull")
    value_string: str | None = Field(None, alias="valueString")
    value_int: int | None = Field(None, alias="valueInt")
    value_float: float | None = Field(None, alias="valueFloat")
    value_date: str | None = Field(None, alias="valueDate")
    value_boolean: bool | None = Field(None, alias="valueBoolean")

    model_config = {"populate_by_name": True}


class MeasureFilter(BaseModel):
    """Filter applied to a measure."""

    column: DataColumnRef | None = None
    operator: str
    values: list[FilterValue] = []

    model_config = {"populate_by_name": True}


class WithinGroup(BaseModel):
    """WITHIN GROUP ordering clause for LISTAGG measures."""

    column: DataColumnRef
    order: str = "ASC"

    model_config = {"populate_by_name": True}


class Measure(BaseModel):
    """An aggregation measure with optional expression template."""

    label: str
    columns: list[DataColumnRef] = []
    result_type: DataType = Field(alias="resultType")
    aggregation: str
    expression: str | None = None
    distinct: bool = False
    total: bool = False
    filter: MeasureFilter | None = None
    format: str | None = None
    allow_fan_out: bool = Field(False, alias="allowFanOut")
    delimiter: str | None = None
    within_group: WithinGroup | None = Field(None, alias="withinGroup")

    model_config = {"populate_by_name": True}


class Metric(BaseModel):
    """A composite metric combining measures via an expression.

    The expression references measures by name using ``{[Measure Name]}`` syntax.
    """

    label: str
    expression: str
    format: str | None = None

    model_config = {"populate_by_name": True}


class Relationship(BaseModel):
    """A join relationship between two tables (from spec §3.5)."""

    from_field: str = Field(alias="from")
    to_field: str = Field(alias="to")
    on: str | None = None
    type: JoinType = JoinType.LEFT
    cardinality: Cardinality = Cardinality.MANY_TO_ONE

    model_config = {"populate_by_name": True}


class SemanticModel(BaseModel):
    """Complete semantic model parsed from OBML YAML."""

    version: float = 1.0
    data_objects: dict[str, DataObject] = Field(default={}, alias="dataObjects")
    dimensions: dict[str, Dimension] = {}
    measures: dict[str, Measure] = {}
    metrics: dict[str, Metric] = {}

    model_config = {"populate_by_name": True}


class Fact(BaseModel):
    """A fact table definition (from spec §3.2)."""

    name: str
    description: str = ""
    table: str
    schema_name: str = Field("", alias="schema")
    grain: str
    dimensions: list[str] = []
    measures: list[str] = []
    relationships: list[Relationship] = []

    model_config = {"populate_by_name": True}
