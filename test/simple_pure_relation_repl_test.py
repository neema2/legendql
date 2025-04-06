import unittest

from model.metamodel import SelectionClause, SelectionExpression, FilterClause, BinaryExpression, OperandExpression, \
    ReferenceExpression, LiteralExpression, IntegerLiteral, EqualsBinaryOperator, ExtendClause, \
    GroupByClause, AliasExpression, LimitClause, ExtendExpression, GroupByExpression, FunctionExpression, CountFunction, \
    InnerJoinType, JoinExpression
from ql.legendql import LegendQL
from runtime.pure.repl_utils import is_repl_running, send_to_repl, load_csv_to_repl
from runtime.pure.runtime import ReplRuntime


class TestPureRelationDialect(unittest.TestCase):

    def setUp(self):
        if not is_repl_running():
            self.skipTest("REPL is not running")
        load_csv_to_repl("data/employees.csv", "local::DuckDuckConnection", "employees")
        load_csv_to_repl("data/departments.csv", "local::DuckDuckConnection", "departments")

    def tearDown(self):
        send_to_repl("drop local::DuckDuckConnection employees")
        send_to_repl("drop local::DuckDuckConnection departments")

    def test_simple_select(self):
        runtime = ReplRuntime("local::DuckDuckRuntime")
        data_frame = (LegendQL.from_db("local::DuckDuckDatabase", "employees")
         .select(SelectionClause([SelectionExpression("id", "id"), SelectionExpression("departmentId", "departmentId"), SelectionExpression("first", "first"), SelectionExpression("last", "last")]))
         .bind(runtime))
        results = data_frame.eval()
        self.assertEqual("""> +--------+--------------+------------+------------+
|   id   | departmentId |   first    |    last    |
| BIGINT |    BIGINT    | VARCHAR(0) | VARCHAR(0) |
+--------+--------------+------------+------------+
|   1    |      1       |    John    |     Doe    |
|   2    |      1       |    Jane    |     Doe    |
+--------+--------------+------------+------------+
2 rows -- 4 columns""", results[:results.rfind("columns") + 7])

    def test_complex_query(self):
        runtime = ReplRuntime("local::DuckDuckRuntime")
        data_frame = (LegendQL.from_db("local::DuckDuckDatabase", "employees")
         .filter(FilterClause(BinaryExpression(OperandExpression(ReferenceExpression("r", "departmentId")), OperandExpression(LiteralExpression(IntegerLiteral(1))), EqualsBinaryOperator())))
         .select(SelectionClause([SelectionExpression("departmentId", "departmentId")]))
         .extend(ExtendClause([ExtendExpression("newCol", ReferenceExpression("x", "departmentId"))]))
         .group_by(GroupByClause([SelectionExpression("newCol", "newCol")], [GroupByExpression("count", ReferenceExpression("x", "newCol"), FunctionExpression(CountFunction(), [AliasExpression("x")]))]))
         .limit(LimitClause(IntegerLiteral(1)))
         .join("local::DuckDuckDatabase", "departments", InnerJoinType(), JoinExpression(BinaryExpression(OperandExpression(ReferenceExpression("a", "newCol")), OperandExpression(ReferenceExpression("b", "id")), EqualsBinaryOperator())))
         .select(SelectionClause([SelectionExpression("id", "id")]))
         .bind(runtime))
        results = data_frame.eval()
        self.assertEqual("""> +--------+
|   id   |
| BIGINT |
+--------+
|   1    |
+--------+
1 rows -- 1 columns""", results[:results.rfind("columns") + 7])
