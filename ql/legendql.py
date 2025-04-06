from __future__ import annotations
from dataclasses import dataclass

from model.metamodel import Query, SelectionClause, Runtime, DataFrame, FilterClause, ExtendClause, GroupByClause, \
    LimitClause, JoinClause, JoinType, SourcedExecutable, JoinExpression


@dataclass
class LegendQL:
    _executable: SourcedExecutable = None

    @classmethod
    def create(cls, database: str, table: str) -> LegendQL:
        return LegendQL(SourcedExecutable(database, table, Query()))

    def bind[R: Runtime](self, runtime: R) -> DataFrame:
        return DataFrame(runtime, self._executable)

    def eval[R: Runtime](self, runtime: R) -> DataFrame:
        return self.bind(runtime).eval()

    def select(self, select: SelectionClause) -> LegendQL:
        self._executable.executable.select = select
        return self

    def extend(self, extend: ExtendClause) -> LegendQL:
        self._executable.executable.extend = (self._executable.executable.extend if self._executable.executable.extend else [])
        self._executable.executable.extend.append(extend)
        return self

    def filter(self, filter: FilterClause) -> LegendQL:
        self._executable.executable.filter = filter
        return self

    def groupBy(self, group_by: GroupByClause) -> LegendQL:
        self._executable.executable.groupBy = group_by
        return self

    def limit(self, limit: LimitClause) -> LegendQL:
        self._executable.executable.limit = limit
        return self

    def join(self, query: LegendQL, join_type: JoinType, on_clause: JoinExpression) -> LegendQL:
        self._executable.executable.join = SourcedExecutable(query._executable.database, query._executable.table, JoinClause(query._executable.executable, join_type, on_clause))
        return self
