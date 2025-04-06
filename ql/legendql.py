from __future__ import annotations
from dataclasses import dataclass
from typing import List

from model.metamodel import SelectionClause, Runtime, DataFrame, FilterClause, ExtendClause, GroupByClause, \
    LimitClause, JoinClause, JoinType, JoinExpression, Clause, FromClause


@dataclass
class LegendQL:
    _clauses: List[Clause]

    @classmethod
    def from_db(cls, database: str, table: str) -> LegendQL:
        return LegendQL([FromClause(database, table)])

    def bind[R: Runtime](self, runtime: R) -> DataFrame:
        return DataFrame(runtime, self._clauses)

    def eval[R: Runtime](self, runtime: R) -> DataFrame:
        return self.bind(runtime).eval()

    def select(self, select: SelectionClause) -> LegendQL:
        self._clauses.append(select)
        return self

    def extend(self, extend: ExtendClause) -> LegendQL:
        self._clauses.append(extend)
        return self

    def filter(self, filter_clause: FilterClause) -> LegendQL:
        self._clauses.append(filter_clause)
        return self

    def group_by(self, group_by: GroupByClause) -> LegendQL:
        self._clauses.append(group_by)
        return self

    def limit(self, limit: LimitClause) -> LegendQL:
        self._clauses.append(limit)
        return self

    def join(self, database: str, table: str, join_type: JoinType, on_clause: JoinExpression) -> LegendQL:
        self._clauses.append(JoinClause(FromClause(database, table), join_type, on_clause))
        return self
