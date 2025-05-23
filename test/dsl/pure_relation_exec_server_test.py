import json
import unittest

from model.schema import Table, Database
from ql.legendql import LegendQL
from runtime.pure.db.duckdb import DuckDBDatabaseType
from runtime.pure.executionserver.runtime import ExecutionServerRuntime
from test.executionserver.testutils import TestExecutionServer


class TestExecutionServerEvaluation(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.execution_server = TestExecutionServer("../executionserver")
        cls.execution_server.start()

    @classmethod
    def tearDownClass(cls):
        cls.execution_server.stop()

    def test_execution_against_execution_server(self):
        table = Table("employees", {"id": int, "departmentId": int, "first": str, "last": str})
        database = Database("local::DuckDuckDatabase", [table])
        runtime = ExecutionServerRuntime("local::DuckDuckRuntime", DuckDBDatabaseType("/Users/ahauser/.legend/repl/duck"), "http://localhost:6300", database)
        data_frame = (LegendQL.from_table(database, table)
                      .select(lambda r: [r.id, r.departmentId, r.first, r.last])
                      .bind(runtime))

        result = data_frame.eval()
        print(json.dumps(result, indent=4))
