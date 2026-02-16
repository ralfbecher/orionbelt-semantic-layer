"""Semantic validation: cycles, ambiguous joins, reference integrity (spec §3.8)."""

from __future__ import annotations

from collections import deque

from orionbelt.models.errors import SemanticError
from orionbelt.models.semantic import SemanticModel


class SemanticValidator:
    """Validates semantic rules from spec §3.8."""

    def validate(self, model: SemanticModel) -> list[SemanticError]:
        errors: list[SemanticError] = []
        errors.extend(self._check_unique_identifiers(model))
        errors.extend(self._check_unique_column_names(model))
        errors.extend(self._check_no_cyclic_joins(model))
        errors.extend(self._check_no_multipath_joins(model))
        errors.extend(self._check_measures_resolve(model))
        errors.extend(self._check_join_targets_exist(model))
        errors.extend(self._check_references_resolve(model))
        return errors

    def _check_unique_identifiers(self, model: SemanticModel) -> list[SemanticError]:
        """Ensure no duplicate names across data objects, dimensions, measures, metrics."""
        errors: list[SemanticError] = []
        all_names: dict[str, str] = {}  # name -> type

        def _register(name: str, kind: str, path: str) -> None:
            existing = all_names.get(name)
            if existing is not None:
                errors.append(
                    SemanticError(
                        code="DUPLICATE_IDENTIFIER",
                        message=(
                            f"{kind.title()} '{name}' conflicts with existing "
                            f"{existing} '{name}'"
                        ),
                        path=path,
                    )
                )
            all_names[name] = kind

        for name in model.data_objects:
            _register(name, "dataObject", f"dataObjects.{name}")

        for name in model.dimensions:
            _register(name, "dimension", f"dimensions.{name}")

        for name in model.measures:
            _register(name, "measure", f"measures.{name}")

        for name in model.metrics:
            _register(name, "metric", f"metrics.{name}")

        return errors

    def _check_unique_column_names(self, model: SemanticModel) -> list[SemanticError]:
        """Ensure column names are globally unique across all data objects."""
        errors: list[SemanticError] = []
        col_locations: dict[str, str] = {}  # col_name -> first object_name

        for obj_name, obj in model.data_objects.items():
            for col_name in obj.columns:
                if col_name in col_locations:
                    errors.append(
                        SemanticError(
                            code="DUPLICATE_COLUMN_NAME",
                            message=(
                                f"Column '{col_name}' in data object '{obj_name}' "
                                f"conflicts with same column in '{col_locations[col_name]}'. "
                                f"Column names must be globally unique."
                            ),
                            path=f"dataObjects.{obj_name}.columns.{col_name}",
                        )
                    )
                else:
                    col_locations[col_name] = obj_name

        return errors

    def _check_no_cyclic_joins(self, model: SemanticModel) -> list[SemanticError]:
        """Detect cyclic join paths."""
        errors: list[SemanticError] = []

        # Build adjacency list from joins
        adj: dict[str, set[str]] = {}
        for obj_name, obj in model.data_objects.items():
            if obj_name not in adj:
                adj[obj_name] = set()
            for join in obj.joins:
                adj[obj_name].add(join.join_to)

        # DFS cycle detection
        visited: set[str] = set()
        rec_stack: set[str] = set()

        def _dfs(node: str, path: list[str]) -> None:
            visited.add(node)
            rec_stack.add(node)
            for neighbor in adj.get(node, set()):
                if neighbor not in visited:
                    _dfs(neighbor, path + [neighbor])
                elif neighbor in rec_stack:
                    if neighbor in path:
                        cycle = path[path.index(neighbor) :] + [neighbor]
                    else:
                        cycle = [node, neighbor]
                    errors.append(
                        SemanticError(
                            code="CYCLIC_JOIN",
                            message=f"Cyclic join detected: {' -> '.join(cycle)}",
                            path=f"dataObjects.{node}.joins",
                        )
                    )
            rec_stack.discard(node)

        for node in adj:
            if node not in visited:
                _dfs(node, [node])

        return errors

    def _check_no_multipath_joins(self, model: SemanticModel) -> list[SemanticError]:
        """Detect multiple distinct paths between any pair of nodes in the join DAG.

        Only flags true diamonds where both paths go through intermediaries.
        A direct edge from start to target is canonical, so an additional
        indirect path (e.g. Purchases→Suppliers direct + Purchases→Products→Suppliers)
        is not ambiguous and is not flagged.
        """
        errors: list[SemanticError] = []

        # Build adjacency list from joins
        adj: dict[str, list[str]] = {}
        for obj_name, obj in model.data_objects.items():
            if obj_name not in adj:
                adj[obj_name] = []
            for join in obj.joins:
                adj[obj_name].append(join.join_to)

        reported: set[tuple[str, str]] = set()

        for start in adj:
            if not adj[start]:
                continue
            # BFS from start; track first parent that reached each node
            direct_neighbors: set[str] = set()
            first_parent: dict[str, str] = {}
            queue: deque[tuple[str, str]] = deque()
            for neighbor in adj[start]:
                if neighbor == start:
                    continue
                direct_neighbors.add(neighbor)
                if neighbor not in first_parent:
                    first_parent[neighbor] = start
                    queue.append((neighbor, start))

            while queue:
                node, parent = queue.popleft()
                for neighbor in adj.get(node, []):
                    if neighbor == start:
                        continue
                    if neighbor not in first_parent:
                        first_parent[neighbor] = node
                        queue.append((neighbor, node))
                    elif first_parent[neighbor] != node:
                        # Skip if target has a direct edge from start —
                        # the direct join is the canonical path.
                        if neighbor in direct_neighbors:
                            continue
                        pair = (start, neighbor)
                        if pair not in reported:
                            reported.add(pair)
                            errors.append(
                                SemanticError(
                                    code="MULTIPATH_JOIN",
                                    message=(
                                        f"Multiple join paths from '{start}' to "
                                        f"'{neighbor}' (via '{first_parent[neighbor]}' "
                                        f"and '{node}'). "
                                        f"Join paths must be unambiguous."
                                    ),
                                    path=f"dataObjects.{start}.joins",
                                )
                            )

        return errors

    def _check_measures_resolve(self, model: SemanticModel) -> list[SemanticError]:
        """Ensure measure column references resolve to actual data object columns."""
        errors: list[SemanticError] = []
        for name, measure in model.measures.items():
            for i, col_ref in enumerate(measure.columns):
                obj_name = col_ref.view
                col_name = col_ref.column
                if obj_name and obj_name not in model.data_objects:
                    errors.append(
                        SemanticError(
                            code="UNKNOWN_DATA_OBJECT",
                            message=(
                                f"Measure '{name}' column[{i}] references "
                                f"unknown data object '{obj_name}'"
                            ),
                            path=f"measures.{name}.columns[{i}]",
                        )
                    )
                elif obj_name and col_name:
                    obj = model.data_objects[obj_name]
                    if col_name not in obj.columns:
                        errors.append(
                            SemanticError(
                                code="UNKNOWN_COLUMN",
                                message=(
                                    f"Measure '{name}' column[{i}] references "
                                    f"unknown column '{col_name}' in data object '{obj_name}'"
                                ),
                                path=f"measures.{name}.columns[{i}]",
                            )
                        )
        return errors

    def _check_join_targets_exist(self, model: SemanticModel) -> list[SemanticError]:
        """Ensure join targets reference existing data objects."""
        errors: list[SemanticError] = []
        for obj_name, obj in model.data_objects.items():
            for i, join in enumerate(obj.joins):
                if len(join.columns_from) != len(join.columns_to):
                    errors.append(
                        SemanticError(
                            code="JOIN_COLUMN_COUNT_MISMATCH",
                            message=(
                                f"Data object '{obj_name}' join[{i}] has "
                                f"{len(join.columns_from)} columnsFrom and "
                                f"{len(join.columns_to)} columnsTo"
                            ),
                            path=f"dataObjects.{obj_name}.joins[{i}]",
                        )
                    )
                if join.join_to not in model.data_objects:
                    errors.append(
                        SemanticError(
                            code="UNKNOWN_JOIN_TARGET",
                            message=(
                                f"Data object '{obj_name}' join[{i}] references "
                                f"unknown data object '{join.join_to}'"
                            ),
                            path=f"dataObjects.{obj_name}.joins[{i}]",
                        )
                    )
                else:
                    # Validate join columns exist
                    for col_name in join.columns_from:
                        if col_name not in obj.columns:
                            errors.append(
                                SemanticError(
                                    code="UNKNOWN_JOIN_COLUMN",
                                    message=(
                                        f"Data object '{obj_name}' join[{i}] columnsFrom "
                                        f"references unknown column '{col_name}'"
                                    ),
                                    path=f"dataObjects.{obj_name}.joins[{i}].columnsFrom",
                                )
                            )
                    target_obj = model.data_objects[join.join_to]
                    for col_name in join.columns_to:
                        if col_name not in target_obj.columns:
                            errors.append(
                                SemanticError(
                                    code="UNKNOWN_JOIN_COLUMN",
                                    message=(
                                        f"Data object '{obj_name}' join[{i}] columnsTo "
                                        f"references unknown column '{col_name}' "
                                        f"in data object '{join.join_to}'"
                                    ),
                                    path=f"dataObjects.{obj_name}.joins[{i}].columnsTo",
                                )
                            )
        return errors

    def _check_references_resolve(self, model: SemanticModel) -> list[SemanticError]:
        """Ensure dimension references resolve."""
        errors: list[SemanticError] = []
        for name, dim in model.dimensions.items():
            obj_name = dim.view
            col_name = dim.column
            if obj_name and obj_name not in model.data_objects:
                errors.append(
                    SemanticError(
                        code="UNKNOWN_DATA_OBJECT",
                        message=f"Dimension '{name}' references unknown data object '{obj_name}'",
                        path=f"dimensions.{name}",
                    )
                )
            elif obj_name and col_name:
                obj = model.data_objects[obj_name]
                if col_name not in obj.columns:
                    errors.append(
                        SemanticError(
                            code="UNKNOWN_COLUMN",
                            message=(
                                f"Dimension '{name}' references unknown column "
                                f"'{col_name}' in data object '{obj_name}'"
                            ),
                            path=f"dimensions.{name}",
                        )
                    )
        return errors
