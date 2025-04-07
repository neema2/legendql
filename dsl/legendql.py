from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Type, Dict, List

from dsl import parser
from dsl.dsl_functions import *
from dsl.metamodel import FromClause, WithClause
from metamodel import SelectClause, ExtendClause, FilterClause, GroupByClause, JoinClause, JoinType, Expression, Clause
from dsl.schema import Schema


class LegendQL:

    schema: Schema
    _clauses: List[Clause]

    def __init__(self, schema: Schema, from_clause: FromClause):
        self.schema = schema
        self._clauses = [from_clause]

    @classmethod
    def from_(cls, name: str, columns: Dict[str, Type]) -> LegendQL:
        return LegendQL(Schema(name, columns), FromClause(name, name))

    def let(self, name: str, cte: LegendQL) -> LegendQL:
        # CommonTableExpression ("with" in SQL)
        self._clauses.append(WithClause(name, name))

        return self

    def recurse(self, df : LegendQL) -> LegendQL:
        # CommonTableExpression ("with recursive" in SQL)
        return self

    def distinct(self) -> LegendQL:
        return self

    def select(self, columns: Callable) -> LegendQL:
        clause = SelectClause(parser.Parser.parse(columns, self.schema))
        self._clauses.append(clause)

        return self

    def extend(self, columns: Callable) -> LegendQL:
        clause = ExtendClause(parser.Parser.parse(columns, self.schema))
        self._clauses.append(clause)

        return self

    def filter(self, condition: Callable) -> LegendQL:
        clause = FilterClause(parser.Parser.parse(condition, self.schema))
        self._clauses.append(clause)

        return self

    def group_by(self, aggr: Callable) -> LegendQL:
        clause = GroupByClause(parser.Parser.parse(aggr, self.schema))
        self._clauses.append(clause)

        return self

    def join(self, lq: LegendQL, join: Callable) -> LegendQL:
        return self

    def left_join(self, lq: LegendQL, join: Callable) -> LegendQL:

        expr = parser.Parser.parse_join(join, self.schema, lq.schema)

        if isinstance(expr, Expression):
            clause = JoinClause(right=lq.schema, condition=expr, type=JoinType())
            self._clauses.append(clause)
        elif isinstance(expr, List) and len(expr) == 2:
            clause = JoinClause(right=lq.schema, condition=expr[0], type=JoinType())
            self._clauses.append(clause)

            clause = SelectClause([expr[1]])
            self._clauses.append(clause)
        else:
            raise ValueError(f"Badly formed Join: {join} {expr}")

        return self

    def right_join(self, lq: LegendQL, join: Callable) -> LegendQL:
        return self

    def outer_join(self, lq: LegendQL, join: Callable) -> LegendQL:
        return self

    def asof_join(self, lq: LegendQL, join: Callable) -> LegendQL:
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

# [for func in inspect.getmembers(LegendQL, inspect.isfunction)]