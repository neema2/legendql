import unittest

from dialect.purerelation.dialect import NonExecutablePureRuntime
from model.metamodel import SelectionClause, ReferenceExpression
from ql.legendql import LegendQL


class TestPureRelationDialect(unittest.TestCase):
    """Test cases for column aliasing using walrus operator."""

    def setUp(self):
        pass

    def test_simple_select_clause(self):
        runtime = NonExecutablePureRuntime("local::DuckDuckDatabase", "table")
        data_frame = (LegendQL.create()
         .select(SelectionClause([ReferenceExpression("column", "col")]))
         .bind(runtime))
        pure_relation = data_frame.executable_to_string()
        self.assertEqual(pure_relation, "#>{local::DuckDuckDatabase.table}#->select(~[col])")