import unittest

from dialect.purerelation.dialect import NonExecutablePureRuntime
from model.metamodel import SelectionClause, ReferenceExpression
from ql.legendql import LegendQL


class TestPureRelationDialect(unittest.TestCase):

    def setUp(self):
        pass

    def test_simple_select_clause(self):
        runtime = NonExecutablePureRuntime("local::DuckDuckDatabase")
        data_frame = (LegendQL.create("table")
         .select(SelectionClause([ReferenceExpression("column", "col")]))
         .bind(runtime))
        pure_relation = data_frame.executable_to_string()
        self.assertEqual(pure_relation, "#>{local::DuckDuckDatabase.table}#->select(~[col])")