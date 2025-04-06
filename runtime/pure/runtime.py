from typing import List

from dialect.purerelation.dialect import PureRuntime
from model.metamodel import Clause
from runtime.pure.repl_utils import send_to_repl


class ReplRuntime(PureRuntime):
    def eval(self, clauses: List[Clause]) -> str:
        return send_to_repl(self.executable_to_string(clauses))
