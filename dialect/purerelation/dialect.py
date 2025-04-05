from abc import ABC
from dataclasses import dataclass

from model.metamodel import ExecutionVisitor, JoinClause, LimitClause, DistinctClause, GroupByClause, ExtendClause, \
    SelectionClause, FilterClause, FunctionExpression, ReferenceExpression, LiteralExpression, BinaryExpression, \
    UnaryExpression, OperandExpression, BooleanLiteral, StringLiteral, IntegerLiteral, Query, Runtime, Executable, \
    Results, OrBinaryOperator, AndBinaryOperator, LessThanEqualsBinaryOperator, LessThanBinaryOperator, \
    GreaterThanEqualsBinaryOperator, GreaterThanBinaryOperator, NotEqualsBinaryOperator, EqualsBinaryOperator, \
    NotUnaryOperator

@dataclass
class PureRuntime(Runtime, ABC):
    database: str
    table: str

    def executable_to_string(self, executable: Executable) -> str:
        visitor = PureRelationExpressionVisitor()
        return executable.visit(visitor, self.visit(visitor, ""))

class NonExecutablePureRuntime(PureRuntime):
    def eval(self, executable: Executable) -> Results:
        raise NotImplementedError

class PureRelationExpressionVisitor(ExecutionVisitor):

    def visit_runtime(self, val: PureRuntime, parameter: str) -> str:
        return "#>{" + val.database + "." + val.table + "}#"

    def visit_query(self, val: Query, parameter: str) -> str:
        #TODO: AJH: add in the rest
        return parameter + "->" + self.visit_selection_clause(val.select, "")

    def visit_integer_literal(self, val: IntegerLiteral, parameter: str) -> str:
        raise NotImplementedError

    def visit_string_literal(self, val: StringLiteral, parameter: str) -> str:
        raise NotImplementedError

    def visit_boolean_literal(self, val: BooleanLiteral, parameter: str) -> str:
        raise NotImplementedError

    def visit_operand_expression(self, val: OperandExpression, parameter: str) -> str:
        raise NotImplementedError
    
    def visit_unary_expression(self, val: UnaryExpression, parameter: str) -> str:
        raise NotImplementedError

    def visit_binary_expression(self, val: BinaryExpression, parameter: str) -> str:
        raise NotImplementedError
    
    def visit_not_unary_operator(self, val: NotUnaryOperator, parameter: str) -> str:
        raise NotImplementedError

    def visit_equals_binary_operator(self, val: EqualsBinaryOperator, parameter: str) -> str:
        raise NotImplementedError

    def visit_not_equals_binary_operator(self, val: NotEqualsBinaryOperator, parameter: str) -> str:
        raise NotImplementedError

    def visit_greater_than_binary_operator(self, val: GreaterThanBinaryOperator, parameter: str) -> str:
        raise NotImplementedError

    def visit_greater_than_equals_operator(self, val: GreaterThanEqualsBinaryOperator, parameter: str) -> str:
        raise NotImplementedError

    def visit_less_than_binary_operator(self, val: LessThanBinaryOperator, parameter: str) -> str:
        raise NotImplementedError

    def visit_less_than_equals_binary_operator(self, val: LessThanEqualsBinaryOperator, parameter: str) -> str:
        raise NotImplementedError

    def visit_and_binary_operator(self, val: AndBinaryOperator, parameter: str) -> str:
        raise NotImplementedError

    def visit_or_binary_operator(self, val: OrBinaryOperator, parameter: str) -> str:
        raise NotImplementedError
    
    def visit_literal_expression(self, val: LiteralExpression, parameter: str) -> str:
        raise NotImplementedError

    def visit_reference_expression(self, val: ReferenceExpression, parameter: str) -> str:
        return val.alias

    def visit_function_expression(self, val: FunctionExpression, parameter: str) -> str:
        raise NotImplementedError

    def visit_filter_clause(self, val: FilterClause, parameter: str) -> str:
        raise NotImplementedError

    def visit_selection_clause(self, val: SelectionClause, parameter: str) -> str:
        return "select(~[" + ", ".join(map(lambda expr: expr.visit(self, ""), val.expressions)) + "])"

    def visit_extend_clause(self, val: ExtendClause, parameter: str) -> str:
        raise NotImplementedError

    def visit_group_by_clause(self, val: GroupByClause, parameter: str) -> str:
        raise NotImplementedError

    def visit_distinct_clause(self, val: DistinctClause, parameter: str) -> str:
        raise NotImplementedError

    def visit_limit_clause(self, val: LimitClause, parameter: str) -> str:
        raise NotImplementedError

    def visit_join_clause(self, val: JoinClause, parameter: str) -> str:
        raise NotImplementedError
