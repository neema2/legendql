from abc import ABC
from dataclasses import dataclass

from model.metamodel import ExecutionVisitor, JoinClause, LimitClause, DistinctClause, GroupByClause, ExtendClause, \
    SelectionClause, FilterClause, FunctionExpression, ReferenceExpression, LiteralExpression, BinaryExpression, \
    UnaryExpression, OperandExpression, BooleanLiteral, StringLiteral, IntegerLiteral, Query, Runtime, Executable, \
    Results, OrBinaryOperator, AndBinaryOperator, LessThanEqualsBinaryOperator, LessThanBinaryOperator, \
    GreaterThanEqualsBinaryOperator, GreaterThanBinaryOperator, NotEqualsBinaryOperator, EqualsBinaryOperator, \
    NotUnaryOperator, InnerJoinType, LeftJoinType, RootQuery


@dataclass
class PureRuntime(Runtime, ABC):
    name: str

    def executable_to_string(self, executable: Executable) -> str:
        visitor = PureRelationExpressionVisitor(self)
        return executable.visit(visitor, "")

class NonExecutablePureRuntime(PureRuntime):
    def eval(self, executable: Executable) -> str:
        raise NotImplementedError()

@dataclass
class PureRelationExpressionVisitor(ExecutionVisitor):
    runtime: PureRuntime

    def visit_runtime(self, val: PureRuntime, parameter: str) -> str:
        return val.name

    def visit_root_query(self, val: RootQuery, parameter: str) -> str:
        return self.visit_query_with_runtime(val.query, parameter, self.runtime)

    def visit_query(self, val: Query, parameter: str) -> str:
        return self.visit_query_with_runtime(val, parameter)

    def visit_query_with_runtime(self, val: Query, parameter: str, runtime: PureRuntime = None) -> str:
        from_clause = "->from(" + runtime.visit(self, "") + ")" if runtime else ""
        select = "#>{" + val.database + "." + val.table + "}#" + from_clause + "->" + self.visit_selection_clause(val.select, "") if val.select else ""
        extend = "->".join(map(lambda expr: "->" + expr.visit(self, ""), val.extend)) if val.extend else ""
        filter_expr = "->" + val.filter.visit(self, "") if val.filter else ""
        group_by = "->" + val.groupBy.visit(self, "") if val.groupBy else ""
        limit = "->" + val.limit.visit(self, "") if val.limit else ""
        join = "->" + val.join.visit(self, "") if val.join else ""
        return select + extend + filter_expr + group_by + limit + join

    def visit_integer_literal(self, val: IntegerLiteral, parameter: str) -> str:
        return str(val.value())

    def visit_string_literal(self, val: StringLiteral, parameter: str) -> str:
        return "'" + val.value() + "'"

    def visit_boolean_literal(self, val: BooleanLiteral, parameter: str) -> str:
        return str(val.value())

    def visit_operand_expression(self, val: OperandExpression, parameter: str) -> str:
        return val.expression.visit(self, "")
    
    def visit_unary_expression(self, val: UnaryExpression, parameter: str) -> str:
        return val.operator.visit(self, "") + val.expression.visit(self, "")

    def visit_binary_expression(self, val: BinaryExpression, parameter: str) -> str:
        return val.left.visit(self, "") + val.operator.visit(self, "") + val.right.visit(self, "")
    
    def visit_not_unary_operator(self, val: NotUnaryOperator, parameter: str) -> str:
        return "!"

    def visit_equals_binary_operator(self, val: EqualsBinaryOperator, parameter: str) -> str:
        return "=="

    def visit_not_equals_binary_operator(self, val: NotEqualsBinaryOperator, parameter: str) -> str:
        return "!="

    def visit_greater_than_binary_operator(self, val: GreaterThanBinaryOperator, parameter: str) -> str:
        return ">"

    def visit_greater_than_equals_operator(self, val: GreaterThanEqualsBinaryOperator, parameter: str) -> str:
        return ">="

    def visit_less_than_binary_operator(self, val: LessThanBinaryOperator, parameter: str) -> str:
        return "<"

    def visit_less_than_equals_binary_operator(self, val: LessThanEqualsBinaryOperator, parameter: str) -> str:
        return "<="

    def visit_and_binary_operator(self, val: AndBinaryOperator, parameter: str) -> str:
        return "and"

    def visit_or_binary_operator(self, val: OrBinaryOperator, parameter: str) -> str:
        return "or"
    
    def visit_literal_expression(self, val: LiteralExpression, parameter: str) -> str:
        return val.literal.visit(self, "")

    def visit_reference_expression(self, val: ReferenceExpression, parameter: str) -> str:
        return val.alias

    def visit_function_expression(self, val: FunctionExpression, parameter: str) -> str:
        #TODO: AJH: need to actually model functions
        return "TODO"

    def visit_filter_clause(self, val: FilterClause, parameter: str) -> str:
        #TODO: AJH: need to fix FilterClause to capture aliases
        return "TODO"

    def visit_selection_clause(self, val: SelectionClause, parameter: str) -> str:
        return "select(~[" + ", ".join(map(lambda expr: expr.visit(self, ""), val.expressions)) + "])"

    def visit_extend_clause(self, val: ExtendClause, parameter: str) -> str:
        return "extend(~[" + ", ".join(map(lambda expr: expr.visit(self, ""), val.expressions)) + "])"

    def visit_group_by_clause(self, val: GroupByClause, parameter: str) -> str:
        having = ", " + val.having.visit(self, "") if val.having else ""
        return "groupBy(~[" + ", ".join(map(lambda expr: expr.visit(self, ""), val.expressions)) + "]" + having + ")"

    def visit_distinct_clause(self, val: DistinctClause, parameter: str) -> str:
        return "distinct(~[" + ", ".join(map(lambda expr: expr.visit(self, ""), val.expressions)) + "])"

    def visit_limit_clause(self, val: LimitClause, parameter: str) -> str:
        return "limit(" + val.value.visit(self, "") + ")"

    def visit_join_clause(self, val: JoinClause, parameter: str) -> str:
        return val.left.visit(self, "") + "->join(" + val.right.visit(self, "") + ", " + val.join_type.visit(self, "") + ")"

    def visit_inner_join_type[P, T](self, val: InnerJoinType, parameter: P) -> T:
        return "JoinKind.INNER"

    def visit_left_join_type[P, T](self, val: LeftJoinType, parameter: P) -> T:
        return "JoinKind.LEFT"
