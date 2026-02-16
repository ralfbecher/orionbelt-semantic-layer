"""Reference resolution: resolves dimension→table, measure→expression references."""

from __future__ import annotations

import re
from typing import Any

from orionbelt.models.errors import SemanticError, ValidationResult
from orionbelt.models.semantic import (
    DataColumnRef,
    DataObject,
    DataObjectColumn,
    DataObjectJoin,
    Dimension,
    FilterValue,
    Measure,
    MeasureFilter,
    Metric,
    SemanticModel,
)
from orionbelt.parser.loader import SourceMap


class ReferenceResolver:
    """Resolves all references in a raw YAML model to a fully-typed SemanticModel."""

    def resolve(
        self,
        raw: dict[str, Any],
        source_map: SourceMap | None = None,
    ) -> tuple[SemanticModel, ValidationResult]:
        """Resolve raw YAML dict into a validated SemanticModel.

        Returns (model, validation_result). If there are errors,
        the model may be partially populated.
        """
        errors: list[SemanticError] = []
        warnings: list[SemanticError] = []

        # Parse data objects
        data_objects: dict[str, DataObject] = {}
        raw_objects = raw.get("dataObjects", {})
        if not isinstance(raw_objects, dict):
            errors.append(
                SemanticError(
                    code="DATA_OBJECT_PARSE_ERROR",
                    message="'dataObjects' must be a YAML mapping, not a list or scalar",
                    path="dataObjects",
                )
            )
            raw_objects = {}
        for name, raw_obj in raw_objects.items():
            try:
                obj_columns: dict[str, DataObjectColumn] = {}
                for fname, fdata in raw_obj.get("columns", {}).items():
                    obj_columns[fname] = DataObjectColumn(
                        label=fname,
                        code=fdata.get("code", fname),
                        abstract_type=fdata.get("abstractType", "string"),
                        sql_type=fdata.get("sqlType"),
                        sql_precision=fdata.get("sqlPrecision"),
                        sql_scale=fdata.get("sqlScale"),
                        comment=fdata.get("comment"),
                    )

                obj_joins: list[DataObjectJoin] = []
                for jdata in raw_obj.get("joins", []):
                    obj_joins.append(
                        DataObjectJoin(
                            join_type=jdata["joinType"],
                            join_to=jdata["joinTo"],
                            columns_from=jdata["columnsFrom"],
                            columns_to=jdata["columnsTo"],
                        )
                    )

                data_objects[name] = DataObject(
                    label=name,
                    code=raw_obj.get("code", ""),
                    database=raw_obj.get("database", ""),
                    schema_name=raw_obj.get("schema", ""),
                    columns=obj_columns,
                    joins=obj_joins,
                    comment=raw_obj.get("comment"),
                )
            except Exception as e:
                span = source_map.get(f"dataObjects.{name}") if source_map else None
                errors.append(
                    SemanticError(
                        code="DATA_OBJECT_PARSE_ERROR",
                        message=f"Failed to parse data object '{name}': {e}",
                        path=f"dataObjects.{name}",
                        span=span,
                    )
                )

        # Build global column lookup: column_name → (object_name, column)
        global_columns: dict[str, tuple[str, DataObjectColumn]] = {}
        for obj_name, obj in data_objects.items():
            for col_name, col_obj in obj.columns.items():
                global_columns[col_name] = (obj_name, col_obj)

        # Parse dimensions
        dimensions: dict[str, Dimension] = {}
        raw_dims = raw.get("dimensions", {})
        if not isinstance(raw_dims, dict):
            errors.append(
                SemanticError(
                    code="DIMENSION_PARSE_ERROR",
                    message="'dimensions' must be a YAML mapping, not a list or scalar",
                    path="dimensions",
                )
            )
            raw_dims = {}
        for name, raw_dim in raw_dims.items():
            try:
                data_object = raw_dim.get("dataObject")
                column = raw_dim.get("column")

                # Validate the data object exists
                if data_object and data_object not in data_objects:
                    span = source_map.get(f"dimensions.{name}") if source_map else None
                    errors.append(
                        SemanticError(
                            code="UNKNOWN_DATA_OBJECT",
                            message=(
                                f"Dimension '{name}' references unknown data object '{data_object}'"
                            ),
                            path=f"dimensions.{name}",
                            span=span,
                            suggestions=_suggest_similar(data_object, list(data_objects.keys())),
                        )
                    )

                # Validate the column exists in the data object
                if (
                    data_object
                    and column
                    and data_object in data_objects
                    and column not in data_objects[data_object].columns
                ):
                    span = source_map.get(f"dimensions.{name}") if source_map else None
                    errors.append(
                        SemanticError(
                            code="UNKNOWN_COLUMN",
                            message=(
                                f"Dimension '{name}' references unknown column "
                                f"'{column}' in data object '{data_object}'"
                            ),
                            path=f"dimensions.{name}",
                            span=span,
                            suggestions=_suggest_similar(
                                column, list(data_objects[data_object].columns.keys())
                            ),
                        )
                    )

                dimensions[name] = Dimension(
                    label=name,
                    view=data_object or "",
                    column=column or "",
                    result_type=raw_dim.get("resultType", "string"),
                    time_grain=raw_dim.get("timeGrain"),
                    format=raw_dim.get("format"),
                )
            except Exception as e:
                span = source_map.get(f"dimensions.{name}") if source_map else None
                errors.append(
                    SemanticError(
                        code="DIMENSION_PARSE_ERROR",
                        message=f"Failed to parse dimension '{name}': {e}",
                        path=f"dimensions.{name}",
                        span=span,
                    )
                )

        # Parse measures
        measures: dict[str, Measure] = {}
        raw_measures = raw.get("measures", {})
        if not isinstance(raw_measures, dict):
            errors.append(
                SemanticError(
                    code="MEASURE_PARSE_ERROR",
                    message="'measures' must be a YAML mapping, not a list or scalar",
                    path="measures",
                )
            )
            raw_measures = {}
        for name, raw_meas in raw_measures.items():
            try:
                measure_columns: list[DataColumnRef] = []
                for fdata in raw_meas.get("columns", []):
                    measure_columns.append(
                        DataColumnRef(
                            view=fdata.get("dataObject"),
                            column=fdata.get("column"),
                        )
                    )

                # Resolve expression field references
                expression = raw_meas.get("expression")
                if expression:
                    self._validate_expression_refs(
                        name, expression, global_columns, errors, source_map
                    )

                mfilter = None
                raw_filter = raw_meas.get("filter")
                if raw_filter:
                    filter_values = []
                    for vdata in raw_filter.get("values", []):
                        filter_values.append(
                            FilterValue(
                                data_type=vdata.get("dataType", "string"),
                                is_null=vdata.get("isNull"),
                                value_string=vdata.get("valueString"),
                                value_int=vdata.get("valueInt"),
                                value_float=vdata.get("valueFloat"),
                                value_date=vdata.get("valueDate"),
                                value_boolean=vdata.get("valueBoolean"),
                            )
                        )
                    filter_column = None
                    if "column" in raw_filter:
                        filter_column = DataColumnRef(
                            view=raw_filter["column"].get("dataObject"),
                            column=raw_filter["column"].get("column"),
                        )
                    mfilter = MeasureFilter(
                        column=filter_column,
                        operator=raw_filter.get("operator", "equals"),
                        values=filter_values,
                    )

                measures[name] = Measure(
                    label=name,
                    columns=measure_columns,
                    result_type=raw_meas.get("resultType", "float"),
                    aggregation=raw_meas.get("aggregation", "sum"),
                    expression=expression,
                    distinct=raw_meas.get("distinct", False),
                    total=raw_meas.get("total", False),
                    filter=mfilter,
                    format=raw_meas.get("format"),
                    allow_fan_out=raw_meas.get("allowFanOut", False),
                )
            except Exception as e:
                span = source_map.get(f"measures.{name}") if source_map else None
                errors.append(
                    SemanticError(
                        code="MEASURE_PARSE_ERROR",
                        message=f"Failed to parse measure '{name}': {e}",
                        path=f"measures.{name}",
                        span=span,
                    )
                )

        # Parse metrics
        metrics: dict[str, Metric] = {}
        raw_metrics = raw.get("metrics", {})
        if not isinstance(raw_metrics, dict):
            errors.append(
                SemanticError(
                    code="METRIC_PARSE_ERROR",
                    message="'metrics' must be a YAML mapping, not a list or scalar",
                    path="metrics",
                )
            )
            raw_metrics = {}
        for name, raw_metric in raw_metrics.items():
            try:
                # Validate measure references in expression
                expression = raw_metric.get("expression", "")
                self._validate_metric_expression_refs(
                    name, expression, measures, errors, source_map
                )

                metrics[name] = Metric(
                    label=name,
                    expression=expression,
                    format=raw_metric.get("format"),
                )
            except Exception as e:
                span = source_map.get(f"metrics.{name}") if source_map else None
                errors.append(
                    SemanticError(
                        code="METRIC_PARSE_ERROR",
                        message=f"Failed to parse metric '{name}': {e}",
                        path=f"metrics.{name}",
                        span=span,
                    )
                )

        model = SemanticModel(
            version=raw.get("version", 1.0),
            data_objects=data_objects,
            dimensions=dimensions,
            measures=measures,
            metrics=metrics,
        )

        result = ValidationResult(
            valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
        )

        return model, result

    def _validate_expression_refs(
        self,
        measure_name: str,
        expression: str,
        global_columns: dict[str, tuple[str, DataObjectColumn]],
        errors: list[SemanticError],
        source_map: SourceMap | None,
    ) -> None:
        """Validate {[Column]} references in a measure expression."""
        # Check column refs {[Column Name]}
        named_refs = re.findall(r"\{\[([^\]]+)\]\}", expression)
        for col_name in named_refs:
            if col_name not in global_columns:
                span = source_map.get(f"measures.{measure_name}.expression") if source_map else None
                errors.append(
                    SemanticError(
                        code="UNKNOWN_COLUMN_IN_EXPRESSION",
                        message=(
                            f"Measure '{measure_name}' expression references unknown column "
                            f"'{col_name}'"
                        ),
                        path=f"measures.{measure_name}.expression",
                        span=span,
                    )
                )

    def _validate_metric_expression_refs(
        self,
        metric_name: str,
        expression: str,
        measures: dict[str, Measure],
        errors: list[SemanticError],
        source_map: SourceMap | None,
    ) -> None:
        """Validate {[Measure Name]} references in a metric expression."""
        # Extract all {[Name]} references from the expression
        named_refs = re.findall(r"\{\[([^\]]+)\]\}", expression)
        for ref_name in named_refs:
            if ref_name not in measures:
                span = source_map.get(f"metrics.{metric_name}.expression") if source_map else None
                errors.append(
                    SemanticError(
                        code="UNKNOWN_MEASURE_REF",
                        message=(f"Metric '{metric_name}' references unknown measure '{ref_name}'"),
                        path=f"metrics.{metric_name}.expression",
                        span=span,
                        suggestions=_suggest_similar(ref_name, list(measures.keys())),
                    )
                )


def _suggest_similar(name: str, candidates: list[str], max_suggestions: int = 3) -> list[str]:
    """Suggest similar names for 'did you mean?' messages."""
    name_lower = name.lower()
    scored = []
    for candidate in candidates:
        # Simple Levenshtein-like scoring
        candidate_lower = candidate.lower()
        if name_lower in candidate_lower or candidate_lower in name_lower:
            scored.append((0, candidate))
        else:
            # Count common characters
            common = sum(1 for c in name_lower if c in candidate_lower)
            scored.append((len(name) + len(candidate) - 2 * common, candidate))
    scored.sort(key=lambda x: x[0])
    return [s[1] for s in scored[:max_suggestions]]
