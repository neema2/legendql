import unittest

from dialect.purerelation.dialect import NonExecutablePureRuntime
from model.metamodel import SelectionClause, ReferenceExpression, FilterClause, ExtendClause, GroupByClause, \
    LimitClause, IntegerLiteral, JoinClause, InnerJoinType
from ql.legendql import LegendQL


class TestPureRelationDialect(unittest.TestCase):

    def setUp(self):
        pass

    def test_simple_select(self):
        runtime = NonExecutablePureRuntime("local::DuckDuckDatabase")
        data_frame = (LegendQL.create("table")
         .select(SelectionClause([ReferenceExpression("column", "col")]))
         .bind(runtime))
        pure_relation = data_frame.executable_to_string()
        self.assertEqual("#>{local::DuckDuckDatabase.table}#->select(~[col])", pure_relation)

    def test_simple_select_with_filter(self):
        runtime = NonExecutablePureRuntime("local::DuckDuckDatabase")
        data_frame = (LegendQL.create("table")
         .select(SelectionClause([ReferenceExpression("column", "col")]))
         .filter(FilterClause(ReferenceExpression("column", "col")))
         .bind(runtime))
        pure_relation = data_frame.executable_to_string()
        self.assertEqual("#>{local::DuckDuckDatabase.table}#->select(~[col])->TODO", pure_relation)

    def test_simple_select_with_extend(self):
        runtime = NonExecutablePureRuntime("local::DuckDuckDatabase")
        data_frame = (LegendQL.create("table")
         .select(SelectionClause([ReferenceExpression("column", "col")]))
         .extend(ExtendClause([ReferenceExpression("column2", "col2")]))
         .bind(runtime))
        pure_relation = data_frame.executable_to_string()
        self.assertEqual("#>{local::DuckDuckDatabase.table}#->select(~[col])->extend(~[col2])", pure_relation)

    def test_simple_select_with_groupBy(self):
        runtime = NonExecutablePureRuntime("local::DuckDuckDatabase")
        data_frame = (LegendQL.create("table")
         .select(SelectionClause([ReferenceExpression("column", "col")]))
         .groupBy(GroupByClause([ReferenceExpression("column2", "col2")]))
         .bind(runtime))
        pure_relation = data_frame.executable_to_string()
        self.assertEqual("#>{local::DuckDuckDatabase.table}#->select(~[col])->groupBy(~[col2])", pure_relation)

    def test_simple_select_with_limit(self):
        runtime = NonExecutablePureRuntime("local::DuckDuckDatabase")
        data_frame = (LegendQL.create("table")
          .select(SelectionClause([ReferenceExpression("column", "col")]))
          .limit(LimitClause(IntegerLiteral(10)))
          .bind(runtime))
        pure_relation = data_frame.executable_to_string()
        self.assertEqual("#>{local::DuckDuckDatabase.table}#->select(~[col])->limit(10)", pure_relation)

    def test_simple_select_with_join(self):
        runtime = NonExecutablePureRuntime("local::DuckDuckDatabase")

        join_query = (LegendQL.create("table2").select(SelectionClause([ReferenceExpression("column2", "col2")])))

        data_frame = (LegendQL.create("table")
          .select(SelectionClause([ReferenceExpression("column", "col")]))
          .join(join_query, InnerJoinType())
          .bind(runtime))
        pure_relation = data_frame.executable_to_string()
        self.assertEqual("#>{local::DuckDuckDatabase.table}#->select(~[col])->join(#>{local::DuckDuckDatabase.table2}#->select(~[col2]), JoinKind.INNER)", pure_relation)