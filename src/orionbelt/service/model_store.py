"""In-memory model registry — core service layer reusable by MCP and REST API."""

from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass, field

from orionbelt.compiler.pipeline import CompilationPipeline, CompilationResult
from orionbelt.models.query import QueryObject
from orionbelt.models.semantic import SemanticModel
from orionbelt.parser.loader import TrackedLoader, YAMLSafetyError
from orionbelt.parser.resolver import ReferenceResolver
from orionbelt.parser.validator import SemanticValidator

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class LoadResult:
    """Result of loading a model into the store."""

    model_id: str
    data_objects: int
    dimensions: int
    measures: int
    metrics: int
    warnings: list[str]


@dataclass
class DataObjectInfo:
    """Summary of a data object for LLM consumption."""

    label: str
    code: str
    columns: list[str]
    join_targets: list[str]


@dataclass
class DimensionInfo:
    """Summary of a dimension."""

    name: str
    result_type: str
    data_object: str
    column: str
    time_grain: str | None


@dataclass
class MeasureInfo:
    """Summary of a measure."""

    name: str
    result_type: str
    aggregation: str
    expression: str | None


@dataclass
class MetricInfo:
    """Summary of a metric."""

    name: str
    expression: str


@dataclass
class ModelDescription:
    """Structured summary of a loaded model — designed for LLM consumption."""

    model_id: str
    data_objects: list[DataObjectInfo]
    dimensions: list[DimensionInfo]
    measures: list[MeasureInfo]
    metrics: list[MetricInfo]


@dataclass
class ModelSummary:
    """Short summary for listing models."""

    model_id: str
    data_objects: int
    dimensions: int
    measures: int
    metrics: int


@dataclass
class ErrorInfo:
    """A single validation error or warning."""

    code: str
    message: str
    path: str | None = None
    suggestions: list[str] = field(default_factory=list)


@dataclass
class ValidationSummary:
    """Result of validating a model without storing it."""

    valid: bool
    errors: list[ErrorInfo]
    warnings: list[ErrorInfo]


# ---------------------------------------------------------------------------
# ModelStore
# ---------------------------------------------------------------------------


class ModelStore:
    """In-memory model registry.  Thread-safe via ``threading.Lock``.

    Models are keyed by short UUID (8-char hex).  All parsing, validation,
    and compilation infrastructure is instantiated internally, following the
    same singleton pattern as ``api/deps.py``.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._models: dict[str, SemanticModel] = {}

        # Internal pipeline singletons (stateless, safe to share).
        self._loader = TrackedLoader()
        self._resolver = ReferenceResolver()
        self._validator = SemanticValidator()
        self._pipeline = CompilationPipeline()

    # -- helpers -------------------------------------------------------------

    @staticmethod
    def _new_id() -> str:
        return uuid.uuid4().hex[:8]

    def _parse_and_validate(
        self, yaml_str: str
    ) -> tuple[SemanticModel, list[ErrorInfo], list[ErrorInfo]]:
        """Parse YAML, resolve references, run semantic validation.

        Returns ``(model, errors, warnings)``.
        """
        errors: list[ErrorInfo] = []
        warnings: list[ErrorInfo] = []

        # 1. Parse YAML
        try:
            raw, source_map = self._loader.load_string(yaml_str)
        except YAMLSafetyError as exc:
            errors.append(ErrorInfo(code="YAML_SAFETY_ERROR", message=str(exc)))
            return SemanticModel(), errors, warnings
        except Exception as exc:
            errors.append(ErrorInfo(code="YAML_PARSE_ERROR", message=str(exc)))
            return SemanticModel(), errors, warnings

        # 2. Resolve references
        model, resolution = self._resolver.resolve(raw, source_map)
        for e in resolution.errors:
            errors.append(
                ErrorInfo(
                    code=e.code,
                    message=e.message,
                    path=e.path,
                    suggestions=list(e.suggestions),
                )
            )
        for w in resolution.warnings:
            warnings.append(
                ErrorInfo(
                    code=w.code,
                    message=w.message,
                    path=w.path,
                    suggestions=list(w.suggestions),
                )
            )

        # 3. Semantic validation
        sem_errors = self._validator.validate(model)
        for e in sem_errors:
            errors.append(
                ErrorInfo(
                    code=e.code,
                    message=e.message,
                    path=e.path,
                    suggestions=list(e.suggestions),
                )
            )

        return model, errors, warnings

    # -- public API ----------------------------------------------------------

    def load_model(self, yaml_str: str) -> LoadResult:
        """Parse, validate, and store a model.  Returns id + summary.

        Raises ``ValueError`` if the model has validation errors.
        """
        model, errors, warnings = self._parse_and_validate(yaml_str)
        if errors:
            msgs = "; ".join(e.message for e in errors)
            raise ValueError(f"Model validation failed: {msgs}")

        model_id = self._new_id()
        with self._lock:
            self._models[model_id] = model

        return LoadResult(
            model_id=model_id,
            data_objects=len(model.data_objects),
            dimensions=len(model.dimensions),
            measures=len(model.measures),
            metrics=len(model.metrics),
            warnings=[w.message for w in warnings],
        )

    def get_model(self, model_id: str) -> SemanticModel:
        """Look up a loaded model.  Raises ``KeyError`` if not found."""
        with self._lock:
            try:
                return self._models[model_id]
            except KeyError:
                raise KeyError(f"No model loaded with id '{model_id}'") from None

    def describe(self, model_id: str) -> ModelDescription:
        """Return a structured summary suitable for LLM consumption."""
        model = self.get_model(model_id)

        data_objects = [
            DataObjectInfo(
                label=obj.label,
                code=obj.qualified_code,
                columns=list(obj.columns.keys()),
                join_targets=[j.join_to for j in obj.joins],
            )
            for obj in model.data_objects.values()
        ]

        dimensions = [
            DimensionInfo(
                name=dim.label,
                result_type=dim.result_type.value,
                data_object=dim.view,
                column=dim.column,
                time_grain=dim.time_grain.value if dim.time_grain else None,
            )
            for dim in model.dimensions.values()
        ]

        measures = [
            MeasureInfo(
                name=m.label,
                result_type=m.result_type.value,
                aggregation=m.aggregation,
                expression=m.expression,
            )
            for m in model.measures.values()
        ]

        metrics = [
            MetricInfo(name=met.label, expression=met.expression) for met in model.metrics.values()
        ]

        return ModelDescription(
            model_id=model_id,
            data_objects=data_objects,
            dimensions=dimensions,
            measures=measures,
            metrics=metrics,
        )

    def list_models(self) -> list[ModelSummary]:
        """Return a short summary for every loaded model."""
        with self._lock:
            items = list(self._models.items())

        return [
            ModelSummary(
                model_id=mid,
                data_objects=len(m.data_objects),
                dimensions=len(m.dimensions),
                measures=len(m.measures),
                metrics=len(m.metrics),
            )
            for mid, m in items
        ]

    def remove_model(self, model_id: str) -> None:
        """Unload a model.  Raises ``KeyError`` if not found."""
        with self._lock:
            try:
                del self._models[model_id]
            except KeyError:
                raise KeyError(f"No model loaded with id '{model_id}'") from None

    def compile_query(
        self,
        model_id: str,
        query: QueryObject,
        dialect: str,
    ) -> CompilationResult:
        """Compile a query against a loaded model."""
        model = self.get_model(model_id)
        return self._pipeline.compile(query, model, dialect)

    def validate(self, yaml_str: str) -> ValidationSummary:
        """Validate a YAML model string without storing it."""
        _model, errors, warnings = self._parse_and_validate(yaml_str)
        return ValidationSummary(
            valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
        )
