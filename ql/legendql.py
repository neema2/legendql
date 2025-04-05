from __future__ import annotations
from dataclasses import dataclass

from model.metamodel import Query, SelectionClause, Runtime, DataFrame, FilterClause, ExtendClause, GroupByClause, \
    LimitClause, JoinClause, JoinType, Executable, RootQuery


@dataclass
class LegendQL:
    _executable: Executable = None

    @classmethod
    def create(cls, database: str, table: str):
        return LegendQL(RootQuery(Query(database, table)))

    def bind[R: Runtime](self, runtime: R) -> DataFrame:
        return DataFrame(runtime, self._executable)

    def eval[R: Runtime](self, runtime: R) -> DataFrame:
        return self.bind(runtime).eval()

    def select(self, select: SelectionClause):
        if not isinstance(self._executable, RootQuery):
            raise TypeError("Need to rationalize joins... you've already done one")
        self._executable.query.select = select
        return self

    def extend(self, extend: ExtendClause):
        if not isinstance(self._executable, RootQuery):
            raise TypeError("Need to rationalize joins... you've already done one")
        self._executable.query.extend = (self._executable.query.extend if self._executable.query.extend else [])
        self._executable.query.extend.append(extend)
        return self

    def filter(self, filter: FilterClause):
        if not isinstance(self._executable, RootQuery):
            raise TypeError("Need to rationalize joins... you've already done one")
        self._executable.query.filter = filter
        return self

    def groupBy(self, group_by: GroupByClause):
        if not isinstance(self._executable, RootQuery):
            raise TypeError("Need to rationalize joins... you've already done one")
        self._executable.query.groupBy = group_by
        return self

    def limit(self, limit: LimitClause):
        if not isinstance(self._executable, RootQuery):
            raise TypeError("Need to rationalize joins... you've already done one")
        self._executable.query.limit = limit
        return self

    def join(self, query: LegendQL, join_type: JoinType):
        if not isinstance(self._executable, RootQuery) and not isinstance(query._executable, RootQuery):
            raise TypeError("Need to rationalize joins... you've already done one")
        self._executable = JoinClause(self._executable, query._executable.query, join_type)
        return self
