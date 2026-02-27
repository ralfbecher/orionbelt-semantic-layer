"""Tests for fanout detection."""

from __future__ import annotations

import pytest

from orionbelt.ast.nodes import ColumnRef, FunctionCall
from orionbelt.ast.nodes import JoinType as ASTJoinType
from orionbelt.compiler.fanout import FanoutError, _step_causes_fanout, detect_fanout
from orionbelt.compiler.graph import JoinStep
from orionbelt.compiler.pipeline import CompilationPipeline
from orionbelt.compiler.resolution import ResolvedDimension, ResolvedMeasure, ResolvedQuery
from orionbelt.models.query import QueryObject, QuerySelect
from orionbelt.models.semantic import (
    Cardinality,
    DataObject,
    DataObjectColumn,
    DataObjectJoin,
    DataType,
    Dimension,
    Measure,
    Metric,
    SemanticModel,
)

# -- helpers -----------------------------------------------------------------


def _make_model(
    *,
    cardinality: Cardinality = Cardinality.MANY_TO_ONE,
    allow_fan_out: bool = False,
    add_metric: bool = False,
    measure_on_customers: bool = False,
) -> SemanticModel:
    """Build a minimal two-object model with configurable join cardinality.

    When ``measure_on_customers=True``, the measure references Customers
    instead of Orders, simulating a reversed-traversal fanout scenario.
    """
    orders = DataObject(
        label="Orders",
        code="ORDERS",
        database="WH",
        schema_name="PUBLIC",
        columns={
            "Order ID": DataObjectColumn(
                label="Order ID", code="ORDER_ID", abstract_type=DataType.STRING
            ),
            "Customer ID": DataObjectColumn(
                label="Customer ID", code="CUSTOMER_ID", abstract_type=DataType.STRING
            ),
            "Amount": DataObjectColumn(label="Amount", code="AMOUNT", abstract_type=DataType.FLOAT),
        },
        joins=[
            DataObjectJoin(
                join_type=cardinality,
                join_to="Customers",
                columns_from=["Customer ID"],
                columns_to=["Cust ID"],
            )
        ],
    )
    customers = DataObject(
        label="Customers",
        code="CUSTOMERS",
        database="WH",
        schema_name="PUBLIC",
        columns={
            "Cust ID": DataObjectColumn(
                label="Cust ID", code="CUST_ID", abstract_type=DataType.STRING
            ),
            "Country": DataObjectColumn(
                label="Country", code="COUNTRY", abstract_type=DataType.STRING
            ),
            "Revenue": DataObjectColumn(
                label="Revenue", code="REVENUE", abstract_type=DataType.FLOAT
            ),
        },
    )

    if measure_on_customers:
        measure_obj, measure_col = "Customers", "Revenue"
    else:
        measure_obj, measure_col = "Orders", "Amount"

    measures: dict[str, Measure] = {
        "Total Revenue": Measure(
            label="Total Revenue",
            columns=[{"dataObject": measure_obj, "column": measure_col}],
            result_type=DataType.FLOAT,
            aggregation="sum",
            allow_fan_out=allow_fan_out,
        ),
    }
    metrics: dict[str, Metric] = {}
    if add_metric:
        measures["Order Count"] = Measure(
            label="Order Count",
            columns=[{"dataObject": measure_obj, "column": measure_col}],
            result_type=DataType.INT,
            aggregation="count",
        )
        metrics["Revenue per Order"] = Metric(
            label="Revenue per Order",
            expression="{[Total Revenue]} / {[Order Count]}",
        )

    return SemanticModel(
        data_objects={"Orders": orders, "Customers": customers},
        dimensions={
            "Customer Country": Dimension(
                label="Customer Country",
                view="Customers",
                column="Country",
                result_type=DataType.STRING,
            ),
        },
        measures=measures,
        metrics=metrics,
    )


def _make_resolved(
    model: SemanticModel,
    *,
    reversed_step: bool = False,
    cardinality: Cardinality = Cardinality.MANY_TO_ONE,
    measure_names: list[str] | None = None,
    component_measures: list[str] | None = None,
) -> ResolvedQuery:
    """Build a minimal resolved query with one join step.

    When ``reversed_step=False`` (forward): traversal goes Orders→Customers,
    matching the declared join direction.  ``from_object="Orders"``,
    ``to_object="Customers"``.

    When ``reversed_step=True`` (reversed): traversal goes Customers→Orders
    (against declared direction).  ``find_join_path`` swaps from/to to keep
    the declared direction: ``from_object="Orders"``, ``to_object="Customers"``.
    The measure is on Customers (``to_object``), which is the actual traversal
    origin — the side whose rows get multiplied.
    """
    if measure_names is None:
        measure_names = ["Total Revenue"]

    # When reversed, the measure's source object is the actual traversal
    # origin (to_object in the JoinStep), which is Customers.
    measure_source = "Customers" if reversed_step else "Orders"

    measures: list[ResolvedMeasure] = []
    for mname in measure_names:
        model_m = model.measures.get(mname)
        if model_m:
            measures.append(
                ResolvedMeasure(
                    name=mname,
                    aggregation=model_m.aggregation,
                    expression=FunctionCall(
                        name=model_m.aggregation.upper(),
                        args=[ColumnRef(name="AMOUNT", table=measure_source)],
                    ),
                )
            )
        elif mname in model.metrics:
            measures.append(
                ResolvedMeasure(
                    name=mname,
                    aggregation="",
                    expression=ColumnRef(name=mname),
                    component_measures=component_measures or [],
                    is_expression=True,
                )
            )

    # JoinStep from_object/to_object always follow declared direction.
    # reversed=True indicates actual traversal is to_object→from_object.
    join_steps = [
        JoinStep(
            from_object="Orders",
            to_object="Customers",
            from_columns=["Customer ID"],
            to_columns=["Cust ID"],
            join_type=ASTJoinType.LEFT,
            cardinality=cardinality,
            reversed=reversed_step,
        ),
    ]

    return ResolvedQuery(
        dimensions=[
            ResolvedDimension(
                name="Customer Country",
                object_name="Customers",
                column_name="Country",
                source_column="COUNTRY",
            ),
        ],
        measures=measures,
        base_object=measure_source,
        required_objects={"Orders", "Customers"},
        join_steps=join_steps,
        measure_source_objects={measure_source},
    )


# -- _step_causes_fanout unit tests -----------------------------------------


class TestStepCausesFanout:
    def test_many_to_one_forward_safe(self) -> None:
        step = JoinStep(
            from_object="A",
            to_object="B",
            from_columns=["x"],
            to_columns=["y"],
            join_type=ASTJoinType.LEFT,
            cardinality=Cardinality.MANY_TO_ONE,
            reversed=False,
        )
        assert _step_causes_fanout(step) is False

    def test_many_to_one_reversed_fanout(self) -> None:
        step = JoinStep(
            from_object="B",
            to_object="A",
            from_columns=["y"],
            to_columns=["x"],
            join_type=ASTJoinType.LEFT,
            cardinality=Cardinality.MANY_TO_ONE,
            reversed=True,
        )
        assert _step_causes_fanout(step) is True

    def test_one_to_one_forward_safe(self) -> None:
        step = JoinStep(
            from_object="A",
            to_object="B",
            from_columns=["x"],
            to_columns=["y"],
            join_type=ASTJoinType.LEFT,
            cardinality=Cardinality.ONE_TO_ONE,
            reversed=False,
        )
        assert _step_causes_fanout(step) is False

    def test_one_to_one_reversed_safe(self) -> None:
        step = JoinStep(
            from_object="B",
            to_object="A",
            from_columns=["y"],
            to_columns=["x"],
            join_type=ASTJoinType.LEFT,
            cardinality=Cardinality.ONE_TO_ONE,
            reversed=True,
        )
        assert _step_causes_fanout(step) is False

    def test_many_to_many_fanout(self) -> None:
        step = JoinStep(
            from_object="A",
            to_object="B",
            from_columns=["x"],
            to_columns=["y"],
            join_type=ASTJoinType.LEFT,
            cardinality=Cardinality.MANY_TO_MANY,
            reversed=False,
        )
        assert _step_causes_fanout(step) is True


# -- detect_fanout integration tests ----------------------------------------


class TestDetectFanout:
    def test_safe_many_to_one_forward(self) -> None:
        """many-to-one in declared direction: no fanout."""
        model = _make_model(cardinality=Cardinality.MANY_TO_ONE)
        resolved = _make_resolved(model, reversed_step=False, cardinality=Cardinality.MANY_TO_ONE)
        detect_fanout(resolved, model)  # should not raise

    def test_fanout_many_to_one_reversed(self) -> None:
        """many-to-one traversed in reverse = one-to-many: fanout."""
        model = _make_model(cardinality=Cardinality.MANY_TO_ONE, measure_on_customers=True)
        resolved = _make_resolved(model, reversed_step=True, cardinality=Cardinality.MANY_TO_ONE)
        with pytest.raises(FanoutError, match="fanout"):
            detect_fanout(resolved, model)

    def test_safe_one_to_one(self) -> None:
        """one-to-one in any direction: no fanout."""
        model = _make_model(cardinality=Cardinality.ONE_TO_ONE)
        resolved = _make_resolved(model, reversed_step=True, cardinality=Cardinality.ONE_TO_ONE)
        detect_fanout(resolved, model)  # should not raise

    def test_fanout_many_to_many(self) -> None:
        """many-to-many always causes fanout."""
        model = _make_model(cardinality=Cardinality.MANY_TO_MANY)
        resolved = _make_resolved(model, reversed_step=False, cardinality=Cardinality.MANY_TO_MANY)
        with pytest.raises(FanoutError, match="fanout"):
            detect_fanout(resolved, model)

    def test_allow_fan_out_suppresses_error(self) -> None:
        """allowFanOut: true on the measure suppresses the fanout error."""
        model = _make_model(
            cardinality=Cardinality.MANY_TO_ONE,
            allow_fan_out=True,
            measure_on_customers=True,
        )
        resolved = _make_resolved(model, reversed_step=True, cardinality=Cardinality.MANY_TO_ONE)
        detect_fanout(resolved, model)  # should not raise

    def test_metric_with_fanout_component(self) -> None:
        """A metric whose component measure has fanout should raise."""
        model = _make_model(
            cardinality=Cardinality.MANY_TO_ONE,
            add_metric=True,
            measure_on_customers=True,
        )
        resolved = _make_resolved(
            model,
            reversed_step=True,
            cardinality=Cardinality.MANY_TO_ONE,
            measure_names=["Revenue per Order"],
            component_measures=["Total Revenue", "Order Count"],
        )
        with pytest.raises(FanoutError, match="fanout"):
            detect_fanout(resolved, model)

    def test_no_join_steps_no_error(self) -> None:
        """No join steps means no fanout check needed."""
        model = _make_model()
        resolved = ResolvedQuery(
            measures=[
                ResolvedMeasure(
                    name="Total Revenue",
                    aggregation="sum",
                    expression=FunctionCall(
                        name="SUM",
                        args=[ColumnRef(name="AMOUNT", table="Orders")],
                    ),
                )
            ],
            base_object="Orders",
        )
        detect_fanout(resolved, model)  # should not raise


# -- Pipeline integration test ----------------------------------------------


class TestPipelineFanout:
    def test_pipeline_raises_fanout_error(self) -> None:
        """CompilationPipeline should propagate FanoutError.

        The join is declared as Orders many-to-one Customers.
        The measure is on Customers (dimension table) and the dimension
        is on Orders (fact table).  Resolution selects Customers as the
        base object (measure source).  The join path Customers→Orders
        reverses the declared direction, creating a one-to-many fanout.
        """
        orders = DataObject(
            label="Orders",
            code="ORDERS",
            database="WH",
            schema_name="PUBLIC",
            columns={
                "Order ID": DataObjectColumn(
                    label="Order ID", code="ORDER_ID", abstract_type=DataType.STRING
                ),
                "Customer ID": DataObjectColumn(
                    label="Customer ID", code="CUSTOMER_ID", abstract_type=DataType.STRING
                ),
            },
            joins=[
                DataObjectJoin(
                    join_type=Cardinality.MANY_TO_ONE,
                    join_to="Customers",
                    columns_from=["Customer ID"],
                    columns_to=["Cust ID"],
                )
            ],
        )
        customers = DataObject(
            label="Customers",
            code="CUSTOMERS",
            database="WH",
            schema_name="PUBLIC",
            columns={
                "Cust ID": DataObjectColumn(
                    label="Cust ID", code="CUST_ID", abstract_type=DataType.STRING
                ),
                "Revenue": DataObjectColumn(
                    label="Revenue", code="REVENUE", abstract_type=DataType.FLOAT
                ),
            },
        )

        model = SemanticModel(
            data_objects={"Orders": orders, "Customers": customers},
            dimensions={
                "Order ID": Dimension(
                    label="Order ID",
                    view="Orders",
                    column="Order ID",
                    result_type=DataType.STRING,
                ),
            },
            measures={
                "Cust Revenue": Measure(
                    label="Cust Revenue",
                    columns=[{"dataObject": "Customers", "column": "Revenue"}],
                    result_type=DataType.FLOAT,
                    aggregation="sum",
                ),
            },
        )

        query = QueryObject(
            select=QuerySelect(
                dimensions=["Order ID"],
                measures=["Cust Revenue"],
            )
        )

        pipeline = CompilationPipeline()
        with pytest.raises(FanoutError, match="fanout"):
            pipeline.compile(query, model, "postgres")
