from abc import ABC, abstractmethod
from enum import Enum
from typing import List, Dict, Type
from dataclasses import dataclass

class Query(ABC):
    def __init__(self, name: str, columns: Dict[str, Type]):
        self.name = name
        self.columns = columns
        self.clauses: List[Clause] = []

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

@dataclass
class BooleanLiteral(Literal):
    val: bool
    def __init__(self, val):
        self.val = val

    def value(self):
        return self.value

@dataclass
class StringLiteral(Literal):
    val: str
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

@dataclass
class BinaryOperator(Operator):
    value: str

@dataclass
class OperandExpression(Expression):
    expression: Expression

@dataclass
class UnaryExpression(Expression):
    operator: UnaryOperator
    expression: OperandExpression

@dataclass
class BinaryExpression(Expression):
    left: Expression
    right: Expression
    operator: BinaryOperator

@dataclass
class LiteralExpression(Expression):
    value: Literal

@dataclass
class ReferenceExpression(Expression):
    name: str
    alias: str

@dataclass
class FunctionExpression(Expression):
    parameters: List[Expression]

@dataclass
class ColumnExpression(Expression):
    name: str
    expression: Expression

@dataclass
class ColumnReference(Expression):
    name: str
    table: str

@dataclass
class IfExpression(Expression):
    test: Expression
    body: Expression
    orelse: Expression

@dataclass
class NotExpression(Expression):
    expression: Expression

class Sort(Enum):
    ASC = "ASC"
    DESC = "DESC"

@dataclass
class SortExpression(Expression):
    direction: Sort
    expression: Expression

class Clause(ABC):
    pass

@dataclass
class FilterClause(Clause):
    expression: Expression

@dataclass
class SelectClause(Clause):
    expressions: List[Expression]

@dataclass
class ExtendClause(Clause):
    expressions: List[Expression]

@dataclass
class GroupByClause(Clause):
    expressions: List[Expression]
    #having: Expression

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
    #left: Query
    right: Query
    condition: Expression
    type: JoinType