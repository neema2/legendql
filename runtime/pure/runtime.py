from dialect.purerelation.dialect import PureRuntime
from model.metamodel import Executable, Results
from runtime.pure.repl_utils import send_to_repl


class ReplRuntime(PureRuntime):
    results: Results = None

    def eval(self, executable: Executable) -> str:
        return send_to_repl(self.executable_to_string(executable))
