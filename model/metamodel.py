from abc import ABC, abstractmethod
from typing import List

class Query(ABC):
    pass

class Literal(ABC):
    @abstractmethod
    def value(self):
        pass

class IntegerLiteral(Literal):
    val = 0
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

class OperandExpression(Expression):
    expression: Expression
    def __init__(self, expression: Expression):
        self.expression = expression

class UnaryExpression(Expression):
    operator: UnaryOperator
    expression: OperandExpression
    def __init__(self, operator: UnaryOperator, expression: OperandExpression):
        self.operator = operator
        self.expression = expression

class BinaryExpression(Expression):
    left: OperandExpression
    right: OperandExpression
    operator: BinaryOperator
    def __init__(self, left: OperandExpression, right: OperandExpression, operator: BinaryOperator):
        self.left = left
        self.right = right
        self.operator = operator

class LiteralExpression(Expression):
    literal: Literal
    def __init__(self, literal: Literal):
        self.literal = literal

class ReferenceExpression(Expression):
    name: str
    alias: str
    def __init__(self, name: str, alias: str):
        self.name = name
        self.alias = alias

class FunctionExpression(Expression):
    function: Function
    parameters: List[Expression]
    def __init__(self, function: Function, parameters: List[Expression]):
        self.function = function
        self.parameters = parameters

class Clause(ABC):
    pass

class FilterClause(Clause):
    expression: Expression
    def __init__(self, expression: Expression):
        self.expression = expression

class SelectionClause(Clause):
    expressions: List[Expression]
    def __init__(self, expressions: List[Expression]):
        self.expressions = expressions

class ExtendClause(Clause):
    expressions: List[Expression]
    def __init__(self, expressions: List[Expression]):
        self.expressions = expressions

class GroupByClause(Clause):
    expressions: List[Expression]
    having: Expression
    def __init__(self, expressions: List[Expression], having: Expression):
        self.expressions = expressions
        self.having = having

class DistinctClause(Clause):
    expressions: List[Expression]
    def __init__(self, expressions: List[Expression]):
        self.expressions = expressions

class LimitClause(Clause):
    value: IntegerLiteral
    def __init__(self, value: IntegerLiteral):
        self.value = value

class JoinType:
    pass

class JoinClause(Clause):
    left: Query
    right: Query
    type: JoinType
    def __init__(self, left: Query, right: Query, type: JoinType):
        self.left = left
        self.right = right
        self.type = type
