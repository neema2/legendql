from dialect.purerelation.dialect import PureRuntime
from model.metamodel import Executable, Runtime, Results


class ReplRuntime(PureRuntime):
    results: Results = None

    def eval(self, executable: Executable) -> Results:
        self.results = self.runtime.eval(executable)
        return self