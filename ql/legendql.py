from model.metamodel import Query, SelectionClause, Runtime, DataFrame


class LegendQL:
    query: Query = Query()

    @classmethod
    def create(cls):
        return LegendQL()

    def bind[R: Runtime](self, runtime: R) -> DataFrame:
        return DataFrame(runtime, self.query)

    def eval[R: Runtime](self, runtime: R) -> DataFrame:
        return self.bind(runtime).eval()

    def select(self, select: SelectionClause):
        self.query.select = select
        return self
