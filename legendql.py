from __future__ import annotations

from typing import Callable, Type, Dict, List

import parser
from functions import *
from metamodel import SelectClause, ExtendClause, FilterClause, GroupByClause, JoinClause, Query, JoinType


@dataclass
class LegendQL:

    def __init__(self, query: Query):
        self.query = query

    @classmethod
    def from_(cls, name: str, columns: Dict[str, Type]) -> LegendQL:
        return LegendQL(Query(name, columns))

    def let(self, df : LegendQL) -> LegendQL:
        # CommonTableExpression ("with" in SQL)
        return self

    def recurse(self, df : LegendQL) -> LegendQL:
        # CommonTableExpression ("with recursive" in SQL)
        return self

    def distinct(self) -> LegendQL:
        return self

    def select(self, columns: Callable) -> LegendQL:
        clause = SelectClause(parser.Parser.parse(columns, self.query))
        self.query.clauses.append(clause)

        return self

    def extend(self, columns: Callable) -> LegendQL:
        clause = ExtendClause(parser.Parser.parse(columns, self.query))
        self.query.clauses.append(clause)

        return self

    def filter(self, condition: Callable) -> LegendQL:
        clause = FilterClause(parser.Parser.parse(condition, self.query))
        self.query.clauses.append(clause)

        return self

    def group_by(self, aggregate: Callable) -> LegendQL:
        clause = GroupByClause(parser.Parser.parse(aggregate, self.query))
        self.query.clauses.append(clause)

        return self

    def join(self, query: Query, condition: Callable, select: Optional[Callable] = None) -> LegendQL:
        return self

    def left_join(self, lq: LegendQL, join: Callable) -> LegendQL:

        expr = parser.Parser.parse_join(join, self.query, lq.query)

        if isinstance(expr, Expression):
            clause = JoinClause(right=lq.query, condition=expr, type=JoinType())
            self.query.clauses.append(clause)
        elif isinstance(expr, List) and len(expr) == 2:
            clause = JoinClause(right=lq.query, condition=expr[0], type=JoinType())
            self.query.clauses.append(clause)

            clause = SelectClause([expr[1]])
            self.query.clauses.append(clause)
        else:
            raise ValueError(f"Badly formed Join: {join} {expr}")

        return self

    def right_join(self, table: LegendQL, condition: Callable, select: Optional[Callable]) -> LegendQL:
        return self

    def outer_join(self, table: LegendQL, condition: Callable, select: Optional[Callable]) -> LegendQL:
        return self

    def asof_join(self, table: LegendQL, condition: Callable, select: Optional[Callable]) -> LegendQL:
        return self

    def order_by(self, columns: Callable) -> LegendQL:
        return self

    def qualify(self, condition: Callable) -> LegendQL:
        # example: qualify(lambda df, x: (df.row_num <= 2) and (x.salary > 50000))
        return self

    def limit(self, limit: int) -> LegendQL:
        return self

    def offset(self, offset: int) -> LegendQL:
        return self

    def to_sql(self, dialect: str = "duckdb") -> str:
        return ""

    def validate_column(self, column_name: str) -> bool:
        """
        Validate that a column exists in the schema.

        Args:
            column_name: The name of the column to validate

        Returns:
            True if the column exists, False otherwise
        """
        return column_name in self.query.columns

# [for func in inspect.getmembers(LegendQL, inspect.isfunction)]