"""Fanout detection: identifies join paths that cause row multiplication."""

from __future__ import annotations

import re

from orionbelt.compiler.graph import JoinStep
from orionbelt.compiler.resolution import ResolvedQuery
from orionbelt.models.semantic import Cardinality, SemanticModel


class FanoutError(Exception):
    """Raised when a join path causes row multiplication (fanout) for a measure."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


def _step_causes_fanout(step: JoinStep) -> bool:
    """Check whether a single join step causes fanout.

    - many-to-many: always fanout
    - many-to-one + reversed (traversed as one-to-many): fanout
    - one-to-one: never fanout
    - many-to-one (forward): never fanout
    """
    if step.cardinality == Cardinality.MANY_TO_MANY:
        return True
    return step.cardinality == Cardinality.MANY_TO_ONE and step.reversed


def detect_fanout(resolved: ResolvedQuery, model: SemanticModel) -> None:
    """Check all measures for fanout and raise ``FanoutError`` if detected.

    For each measure (and each metric component), skip if
    ``allow_fan_out=True`` on the model measure.  Walk
    ``resolved.join_steps`` — if any step causes fanout for that
    measure's source object, collect an error.
    """
    if not resolved.join_steps:
        return

    errors: list[str] = []

    # Build a set of measure names to check (direct + metric components)
    measures_to_check: list[str] = []
    for m in resolved.measures:
        if m.component_measures:
            measures_to_check.extend(m.component_measures)
        else:
            measures_to_check.append(m.name)

    # Deduplicate while preserving order
    seen: set[str] = set()
    unique_measures: list[str] = []
    for name in measures_to_check:
        if name not in seen:
            seen.add(name)
            unique_measures.append(name)

    # Build global column lookup for expression-based measures
    global_columns: dict[str, str] = {}
    for obj_name, obj in model.data_objects.items():
        for col_name in obj.columns:
            global_columns[col_name] = obj_name

    for measure_name in unique_measures:
        model_measure = model.measures.get(measure_name)
        if model_measure is None:
            continue
        if model_measure.allow_fan_out:
            continue

        # Determine which data objects this measure references
        source_objects: set[str] = set()
        for cref in model_measure.columns:
            if cref.view:
                source_objects.add(cref.view)
        if model_measure.expression:
            col_refs = re.findall(r"\{\[([^\]]+)\]\}", model_measure.expression)
            for col_name in col_refs:
                if col_name in global_columns:
                    source_objects.add(global_columns[col_name])

        if not source_objects:
            continue

        # Check each join step for fanout
        for step in resolved.join_steps:
            if _step_causes_fanout(step):
                # Determine which side gets row multiplication.
                # When reversed, from_object/to_object represent the
                # declared direction (swapped); the actual traversal
                # origin (whose rows get multiplied) is to_object.
                multiplied_object = step.to_object if step.reversed else step.from_object
                if multiplied_object in source_objects:
                    errors.append(
                        f"Measure '{measure_name}' has fanout: "
                        f"join from '{step.from_object}' to '{step.to_object}' "
                        f"({step.cardinality.value}"
                        f"{', reversed' if step.reversed else ''}"
                        f") causes row multiplication"
                    )

    if errors:
        raise FanoutError("; ".join(errors))
