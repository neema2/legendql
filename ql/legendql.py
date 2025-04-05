from __future__ import annotations
from dataclasses import dataclass

from model.metamodel import Query, SelectionClause, Runtime, DataFrame, FilterClause, ExtendClause, GroupByClause, \
    LimitClause, JoinClause, JoinType, Executable


@dataclass
class LegendQL:
    _executable: Executable = None

    @classmethod
    def create(cls, table: str):
        return LegendQL(Query(table))

    def bind[R: Runtime](self, runtime: R) -> DataFrame:
        return DataFrame(runtime, self._executable)

    def eval[R: Runtime](self, runtime: R) -> DataFrame:
        return self.bind(runtime).eval()

    def select(self, select: SelectionClause):
        if not isinstance(self._executable, Query):
            raise TypeError("Need to rationalize joins... you've already done one")
        self._executable.select = select
        return self

    def extend(self, extend: ExtendClause):
        if not isinstance(self._executable, Query):
            raise TypeError("Need to rationalize joins... you've already done one")
        self._executable.extend = (self._executable.extend if self._executable.extend else [])
        self._executable.extend.append(extend)
        return self

    def filter(self, filter: FilterClause):
        if not isinstance(self._executable, Query):
            raise TypeError("Need to rationalize joins... you've already done one")
        self._executable.filter = filter
        return self

    def groupBy(self, group_by: GroupByClause):
        if not isinstance(self._executable, Query):
            raise TypeError("Need to rationalize joins... you've already done one")
        self._executable.groupBy = group_by
        return self

    def limit(self, limit: LimitClause):
        if not isinstance(self._executable, Query):
            raise TypeError("Need to rationalize joins... you've already done one")
        self._executable.limit = limit
        return self

    def join(self, query: LegendQL, join_type: JoinType):
        self._executable = JoinClause(self._executable, query._executable, join_type)
        return self
