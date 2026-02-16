"""Join graph: data objects as nodes, joins as edges. Uses networkx for path resolution."""

from __future__ import annotations

from dataclasses import dataclass

import networkx as nx

from orionbelt.ast.nodes import BinaryOp, ColumnRef, Expr
from orionbelt.ast.nodes import JoinType as ASTJoinType
from orionbelt.models.errors import SemanticError
from orionbelt.models.semantic import Cardinality, SemanticModel


@dataclass
class JoinStep:
    """A single step in a resolved join path."""

    from_object: str
    to_object: str
    from_columns: list[str]
    to_columns: list[str]
    join_type: ASTJoinType
    cardinality: Cardinality


class JoinGraph:
    """Graph of data objects (nodes) and relationships (edges) for join path resolution."""

    def __init__(self, model: SemanticModel) -> None:
        self._graph: nx.Graph[str] = nx.Graph()
        self._directed: nx.DiGraph[str] = nx.DiGraph()
        self._model = model
        self._build(model)

    def _build(self, model: SemanticModel) -> None:
        """Build the graph from the semantic model."""
        for name in model.data_objects:
            self._graph.add_node(name)
            self._directed.add_node(name)

        for obj_name, obj in model.data_objects.items():
            for join in obj.joins:
                if join.join_to in model.data_objects:
                    self._graph.add_edge(
                        obj_name,
                        join.join_to,
                        columns_from=join.columns_from,
                        columns_to=join.columns_to,
                        cardinality=join.join_type,
                        source_object=obj_name,
                    )
                    self._directed.add_edge(
                        obj_name,
                        join.join_to,
                        columns_from=join.columns_from,
                        columns_to=join.columns_to,
                        cardinality=join.join_type,
                    )

    def find_join_path(self, from_objects: set[str], to_objects: set[str]) -> list[JoinStep]:
        """Find a minimal join path connecting all required data objects.

        Uses shortest path for each target object from the set of source objects.
        """
        steps: list[JoinStep] = []
        visited_edges: set[tuple[str, str]] = set()

        all_targets = to_objects - from_objects
        source_list = list(from_objects)

        for target in sorted(all_targets):
            best_path: list[str] | None = None
            for source in source_list:
                try:
                    path = nx.shortest_path(self._graph, source, target)
                    if best_path is None or len(path) < len(best_path):
                        best_path = path
                except nx.NetworkXNoPath:
                    continue

            if best_path is None:
                continue

            for i in range(len(best_path) - 1):
                edge = (best_path[i], best_path[i + 1])
                rev_edge = (best_path[i + 1], best_path[i])
                if edge in visited_edges or rev_edge in visited_edges:
                    continue
                visited_edges.add(edge)

                edge_data = self._graph.edges[edge]
                source_object = edge_data.get("source_object", edge[0])

                if source_object == edge[0]:
                    step = JoinStep(
                        from_object=edge[0],
                        to_object=edge[1],
                        from_columns=edge_data["columns_from"],
                        to_columns=edge_data["columns_to"],
                        join_type=ASTJoinType.LEFT,
                        cardinality=edge_data["cardinality"],
                    )
                else:
                    step = JoinStep(
                        from_object=edge[1],
                        to_object=edge[0],
                        from_columns=edge_data["columns_to"],
                        to_columns=edge_data["columns_from"],
                        join_type=ASTJoinType.LEFT,
                        cardinality=edge_data["cardinality"],
                    )
                steps.append(step)

            # Add target to sources for subsequent lookups
            source_list.append(target)

        return steps

    def build_join_condition(self, step: JoinStep) -> Expr:
        """Build the ON clause expression for a join step."""
        conditions: list[Expr] = []
        for from_c, to_c in zip(step.from_columns, step.to_columns, strict=True):
            # Resolve to physical column names
            from_obj = self._model.data_objects.get(step.from_object)
            to_obj = self._model.data_objects.get(step.to_object)
            if from_obj and from_c in from_obj.columns:
                from_col = from_obj.columns[from_c].code
            else:
                from_col = from_c
            to_col = to_obj.columns[to_c].code if to_obj and to_c in to_obj.columns else to_c
            conditions.append(
                BinaryOp(
                    left=ColumnRef(name=from_col, table=step.from_object),
                    op="=",
                    right=ColumnRef(name=to_col, table=step.to_object),
                )
            )

        result: Expr = conditions[0]
        for cond in conditions[1:]:
            result = BinaryOp(left=result, op="AND", right=cond)
        return result

    def detect_cycles(self) -> list[list[str]]:
        """Detect cyclic join paths."""
        try:
            cycles = list(nx.simple_cycles(self._directed))
            return cycles
        except nx.NetworkXError:
            return []

    def validate_deterministic(self) -> list[SemanticError]:
        """Ensure join paths are deterministic (no ambiguity)."""
        errors: list[SemanticError] = []
        # Check for multiple edges between the same pair of nodes
        for u, v in self._graph.edges():
            if self._graph.number_of_edges(u, v) > 1:
                errors.append(
                    SemanticError(
                        code="AMBIGUOUS_JOIN",
                        message=f"Multiple join paths between '{u}' and '{v}'",
                        path=f"dataObjects.{u}.joins",
                    )
                )
        return errors
