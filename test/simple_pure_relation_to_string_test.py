import unittest

from dialect.purerelation.dialect import NonExecutablePureRuntime
from model.metamodel import SelectionClause, SelectionExpression, FilterClause, ExtendClause, GroupByClause, \
    LimitClause, IntegerLiteral, JoinClause, InnerJoinType, BinaryExpression, ReferenceExpression, LiteralExpression, \
    EqualsBinaryOperator, OperandExpression, AliasExpression, ExtendExpression, GroupByExpression, FunctionExpression, \
    CountFunction
from ql.legendql import LegendQL


class TestPureRelationDialect(unittest.TestCase):

    def setUp(self):
        pass

    def test_simple_select(self):
        runtime = NonExecutablePureRuntime("local::DuckDuckRuntime")
        data_frame = (LegendQL.create("local::DuckDuckDatabase", "table")
         .select(SelectionClause([SelectionExpression("col", "column")]))
         .bind(runtime))
        pure_relation = data_frame.executable_to_string()
        self.assertEqual("#>{local::DuckDuckDatabase.table}#->from(local::DuckDuckRuntime)->select(~[column])", pure_relation)

    def test_simple_select_with_filter(self):
        runtime = NonExecutablePureRuntime("local::DuckDuckRuntime")
        data_frame = (LegendQL.create("local::DuckDuckDatabase", "table")
         .select(SelectionClause([SelectionExpression("col", "column")]))
         .filter(FilterClause(BinaryExpression(OperandExpression(ReferenceExpression("a", "column")), OperandExpression(LiteralExpression(IntegerLiteral(1))), EqualsBinaryOperator())))
         .bind(runtime))
        pure_relation = data_frame.executable_to_string()
        self.assertEqual("#>{local::DuckDuckDatabase.table}#->from(local::DuckDuckRuntime)->select(~[column])->filter(a | $a.column==1)", pure_relation)

    def test_simple_select_with_extend(self):
        runtime = NonExecutablePureRuntime("local::DuckDuckRuntime")
        data_frame = (LegendQL.create("local::DuckDuckDatabase", "table")
         .select(SelectionClause([SelectionExpression("col", "column")]))
         .extend(ExtendClause([ExtendExpression("a", ReferenceExpression("a", "column"))]))
         .bind(runtime))
        pure_relation = data_frame.executable_to_string()
        self.assertEqual("#>{local::DuckDuckDatabase.table}#->from(local::DuckDuckRuntime)->select(~[column])->extend(~a:a | [$a.column])", pure_relation)

    def test_simple_select_with_groupBy(self):
        runtime = NonExecutablePureRuntime("local::DuckDuckRuntime")
        data_frame = (LegendQL.create("local::DuckDuckDatabase", "table")
         .select(SelectionClause([SelectionExpression("col", "column")]))
         .groupBy(GroupByClause([SelectionExpression("col", "column")], [GroupByExpression("count", ReferenceExpression("a", "column"), FunctionExpression(CountFunction(), [AliasExpression("a")]))]))
         .bind(runtime))
        pure_relation = data_frame.executable_to_string()
        self.assertEqual("#>{local::DuckDuckDatabase.table}#->from(local::DuckDuckRuntime)->select(~[column])->groupBy(~[column], ~[count: a | $a.column : a | $a->count()])", pure_relation)

    def test_simple_select_with_limit(self):
        runtime = NonExecutablePureRuntime("local::DuckDuckRuntime")
        data_frame = (LegendQL.create("local::DuckDuckDatabase", "table")
          .select(SelectionClause([SelectionExpression("col", "column")]))
          .limit(LimitClause(IntegerLiteral(10)))
          .bind(runtime))
        pure_relation = data_frame.executable_to_string()
        self.assertEqual("#>{local::DuckDuckDatabase.table}#->from(local::DuckDuckRuntime)->select(~[column])->limit(10)", pure_relation)

    def test_simple_select_with_join(self):
        runtime = NonExecutablePureRuntime("local::DuckDuckRuntime")

        join_query = (LegendQL.create("local::DuckDuckDatabase", "table2").select(SelectionClause([SelectionExpression("col2", "column2")])))

        data_frame = (LegendQL.create("local::DuckDuckDatabase", "table")
          .select(SelectionClause([SelectionExpression("col", "column")]))
          .join(join_query, InnerJoinType())
          .bind(runtime))
        pure_relation = data_frame.executable_to_string()
        self.assertEqual("#>{local::DuckDuckDatabase.table}#->from(local::DuckDuckRuntime)->select(~[column])->join(#>{local::DuckDuckDatabase.table2}#->select(~[column2]), JoinKind.INNER)", pure_relation)
