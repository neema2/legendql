from dataclasses import dataclass
from datetime import date
from typing import Type

from model.schema import Schema
from runtime.pure.executionserver.runtime import DatabaseType

@dataclass
class DuckDBDatabaseType(DatabaseType):
    path: str

    def generate_pure_runtime(self, name: str) -> str:
        return f"""
###Runtime
Runtime {name}
{{
  mappings:
  [
  ];
  connections:
  [
    local::DuckDuckDatabase:
    [
      connection: local::DuckDuckConnection
    ]
  ];
}}
"""

    def generate_pure_connection(self) -> str:
        return f"""
###Connection       
RelationalDatabaseConnection local::DuckDuckConnection
{{
  type: DuckDB;
  specification: DuckDB
  {{
    path: '{self.path}';
  }};
  auth: Test;
}}    
"""

    def generate_pure_database(self, schema: Schema) -> str:
        columns = []
        for (col, typ) in schema.columns.items():
            columns.append(f"{col} {self._python_type_to_db_type(typ)}")
        table = f"""
  Table {schema.table}
  (
    {",\n".join(columns)}
  )
"""
        return f"""
###Relational
Database {schema.database}
(
  {table}
)
"""

    def _python_type_to_db_type(self, typ: Type):
        if typ == str:
            return "VARCHAR(0)"
        if typ == int:
            return "BIGINT"
        if type == date:
            return "DATE"
        raise ValueError(f"Unsupported type {typ}")