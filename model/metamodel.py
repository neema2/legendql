from abc import ABC, abstractmethod
from typing import List
from dataclasses import dataclass

class Query(ABC):
    pass

class Literal(ABC):
    @abstractmethod
    def value(self):
        pass

@dataclass
class IntegerLiteral(Literal):
    val: int
    def __init__(self, val):
        self.val = val

    def value(self):
        return self.value

class Function(ABC):
    pass

class Expression(ABC):
    pass

class Operator(ABC):
    pass

class UnaryOperator(Operator):
    pass

class BinaryOperator(Operator):
    pass

@dataclass
class OperandExpression(Expression):
    expression: Expression

@dataclass
class UnaryExpression(Expression):
    operator: UnaryOperator
    expression: OperandExpression

@dataclass
class BinaryExpression(Expression):
    left: OperandExpression
    right: OperandExpression
    operator: BinaryOperator

@dataclass
class LiteralExpression(Expression):
    literal: Literal

@dataclass
class ReferenceExpression(Expression):
    name: str
    alias: str

@dataclass
class FunctionExpression(Expression):
    function: Function
    parameters: List[Expression]

class Clause(ABC):
    pass

@dataclass
class FilterClause(Clause):
    expression: Expression

@dataclass
class SelectionClause(Clause):
    expressions: List[Expression]

@dataclass
class ExtendClause(Clause):
    expressions: List[Expression]

@dataclass
class GroupByClause(Clause):
    expressions: List[Expression]
    having: Expression

@dataclass
class DistinctClause(Clause):
    expressions: List[Expression]

@dataclass
class LimitClause(Clause):
    value: IntegerLiteral

class JoinType:
    pass

@dataclass
class JoinClause(Clause):
    left: Query
    right: Query
    type: JoinType
