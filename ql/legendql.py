from dataclasses import dataclass

from model.metamodel import Query, SelectionClause, Runtime, DataFrame

@dataclass
class LegendQL:
    query: Query = None

    @classmethod
    def create(cls, table: str):
        return LegendQL(Query(table))

    def bind[R: Runtime](self, runtime: R) -> DataFrame:
        return DataFrame(runtime, self.query)

    def eval[R: Runtime](self, runtime: R) -> DataFrame:
        return self.bind(runtime).eval()

    def select(self, select: SelectionClause):
        self.query.select = select
        return self
