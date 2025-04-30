"""
Lambda parser for the cloud-dataframe DSL.

This module provides utilities for parsing Python lambda functions
and converting them to SQL expressions.
"""
import ast
import importlib
import inspect
from _ast import operator
from typing import Callable, List, Union

from functions import AggregateFunction, OverFunction, RowsFunction, RangeFunction, StringConcatFunction
from metamodel import Expression, ColumnExpression, BinaryExpression, BinaryOperator, \
    ColumnReference, BooleanLiteral, IfExpression, NotExpression, SortExpression, Sort, Query

class Parser:

    @staticmethod
    def parse(func: Callable, query: Query) -> Union[Expression, List[Expression]]:
        """
        Parse a lambda function and convert it to an Expression or list of Expressions.

        Args:
            func: The lambda function to parse. Can be:
                - A lambda that returns a boolean expression (e.g., lambda x: x.age > 30)
                - A lambda that returns a column reference (e.g., lambda x: x.name)
                - A lambda that returns an array of column references (e.g., lambda x: [x.name, x.age])
                - A lambda that returns tuples with sort direction (e.g., lambda x: [(x.department, Sort.DESC)])
            query: the current LegendQL query context

        Returns:
            An Expression or list of Expressions representing the lambda function
        """
        # Get the source code of the lambda function
        lambda_node = Parser._get_lambda_node(func)
        lambda_args = lambda_node.args.args

        if len(lambda_args) != 1:
            raise ValueError(f"Lambda MUST exactly 1 argument: {lambda_args}")

        alias = {lambda_args[0].arg: query.name}

        # Parse the lambda body
        result = Parser._parse_expression(lambda_node.body, alias)

        # [lambda_body, lambda_args] = _get_lambda_source(func)
        # alias = {lambda_args[0].arg: query.name}
        # result = Parser._parse_expression(lambda_body, alias)

        return result

    @staticmethod
    def parse_join(func: Callable, query: Query, join: Query) -> Union[Expression, List[Expression]]:
        """
        Specifically parse the JOIN lambda function and convert it to an Expression or list of Expressions.

        Args:
            func: The lambda function to parse. Must be a 2 argument lambda:
                - A lambda that returns a boolean expression (e.g., lambda x, y: x.col1 == y.col2)
            query: the current LegendQL query context
            join: the LegendQL query context we want to join to

        Returns:
            An Expression representing the lambda function
        """
        # Get the source code of the lambda function
        lambda_node = Parser._get_lambda_node(func)
        lambda_args = lambda_node.args.args

        if len(lambda_args) != 2:
            raise ValueError(f"Join Lambda MUST have 2 arguments: {lambda_args}")

        # map the lambda args to the table names for the query
        alias = {lambda_args[0].arg: query.name, lambda_args[1].arg: join.name}

        # Parse the lambda body
        result = Parser._parse_expression(lambda_node.body, alias)

        return result

    @staticmethod
    def _get_lambda_node(func):
        try:
            source_lines, _ = inspect.getsourcelines(func)
            source_text = ''.join(source_lines).strip()

            funcs = ['asof_join', 'distinct', 'extend', 'filter', 'group_by', 'join', 'left_join', 'let', 'limit',
                     'offset', 'order_by', 'outer_join', 'qualify', 'recurse', 'right_join', 'select']
            for func in funcs:
                idx = source_text.find(f"{func}(")
                if idx != -1:
                    source_text = source_text[idx + len(func) + 1:len(source_text) - 1]
                    break

            # Parse the source code using ast
            source_ast = ast.parse(source_text)
            lambda_node = next((node for node in ast.walk(source_ast) if isinstance(node, ast.Lambda)), None)

            if not lambda_node:
                raise ValueError("Could not find lambda expression in source code")

        except Exception:
            raise ValueError("Error getting Lambda")
        return lambda_node

    @staticmethod
    def _parse_expression(node: ast.AST, alias: dict) -> Union[Expression, List[Expression]]:
        """
        Parse an AST node and convert it to an Expression or list of Expressions.

        Args:
            node: The AST node to parse
            alias: lambda args mapped to table aliases

        Returns:
            An Expression or list of Expressions representing the AST node,
            or list containing tuples of (Expression, sort_direction) for order_by clauses
        """
        if node is None:
            raise ValueError("node in Parser._parse_expression is None")

        if isinstance(node, ast.NamedExpr):
            # column rename or new column using := (walrus) operator
            if isinstance(node.target, ast.Name):
                target_name = node.target.id
            else:
                raise ValueError("Rename column must be valid column name")

            expr = Parser._parse_expression(node.value, alias)

            if isinstance(expr, Expression):
                return ColumnExpression(name=target_name, expression=expr)
            else:
                raise ValueError(f"Unknown expression: {node.value}, {expr}")

        if isinstance(node, ast.Compare):
            # Handle comparison operations (e.g., x > 5, y == 'value')
            left = Parser._parse_expression(node.left, alias)

            # We only handle the first comparator for simplicity
            # In a real implementation, we would handle multiple comparators
            op = node.ops[0]
            right = Parser._parse_expression(node.comparators[0], alias)

            comp_op = Parser._get_comparison_operator(op)

            # Ensure left and right are Expression objects, not lists or tuples
            if isinstance(left, list) or isinstance(left, tuple):
                raise ValueError(f"Unsupported Compare object {left}")
            if isinstance(right, list) or isinstance(right, tuple):
                raise ValueError(f"Unsupported Compare object {right}")

            return BinaryExpression(left=left, operator=comp_op, right=right)

        elif isinstance(node, ast.BinOp):
            # Handle binary operations (e.g., x + y, x - y, x * y)
            left = Parser._parse_expression(node.left, alias)
            right = Parser._parse_expression(node.right, alias)

            comp_op = Parser._get_binary_operator(node.op)

            # Ensure left and right are Expression objects, not lists or tuples
            if isinstance(left, list) or isinstance(left, tuple):
                raise ValueError(f"Unsupported BinOp object {left}")
            if isinstance(right, list) or isinstance(right, tuple):
                raise ValueError(f"Unsupported BinOp object {right}")

            return BinaryExpression(left=left, operator=comp_op, right=right)

        elif isinstance(node, ast.BoolOp):
            # Handle boolean operations (e.g., x and y, x or y)
            values = [Parser._parse_expression(val, alias) for val in node.values]

            # Combine the values with the appropriate operator
            comp_op = BinaryOperator("AND") if isinstance(node.op, ast.And) else BinaryOperator("OR")

            # Ensure all values are Expression objects, not lists or tuples
            processed_values = []
            for val in values:
                if isinstance(val, list) or isinstance(val, tuple):
                    raise ValueError(f"Unsupported BoolOp object {val}")
                else:
                    processed_values.append(val)

            # Start with the first two values
            result = BinaryExpression(left=processed_values[0], operator=comp_op, right=processed_values[1])

            # Add the remaining values
            for value in processed_values[2:]:
                result = BinaryExpression(left=result, operator=comp_op, right=value)

            return result

        elif isinstance(node, ast.Attribute):
            # Handle column references (e.g. x.column_name)
            if isinstance(node.value, ast.Name):
                # validate the column name
                #if not lq.validate_column(node.attr):
                #    raise ValueError(f"Column '{node.attr}' not found in table schema '{lq.schema}'")

                return ColumnReference(name=node.attr, table=alias[node.value.id])
            else:
                raise ValueError(f"Unsupported Column Reference {node.value}")

        elif isinstance(node, ast.Constant):
            # Handle literal values (e.g., 5, 'value', True)
            from metamodel import LiteralExpression
            return LiteralExpression(value=node.value)

        elif isinstance(node, ast.Name):
            # Handle variable names (e.g., x, y)
            if alias.get(node.id):
                # This is the lambda parameter itself
                raise ValueError(f"Cannot reference the lambda parameter by itself {node.id}")
            elif node.id == "True":
                from metamodel import LiteralExpression
                return LiteralExpression(value=BooleanLiteral(True))
            elif node.id == "False":
                from metamodel import LiteralExpression
                return LiteralExpression(value=BooleanLiteral(False))
            else:
                # This is a variable reference
                # In a real implementation, we would handle this more robustly
                return ColumnReference(name=node.id, table='')

        elif isinstance(node, ast.UnaryOp):
            # Handle unary operations (e.g., not x)
            operand = Parser._parse_expression(node.operand, alias)

            # Ensure operand is an Expression object, not a list or tuple
            if isinstance(operand, list) or isinstance(operand, tuple):
                # Use a fallback for list/tuple values in unary operations
                raise ValueError(f"Unsupported expression to UnaryOp: {operand}")

            if isinstance(node.op, ast.Not):
                return NotExpression(operand)
            elif isinstance(node.op, ast.USub):
                if isinstance(node.operand, ast.Constant):
                    return operand
                else:
                    #sort
                    return SortExpression(direction=Sort.DESC, expression=operand)
            else:
                # Other unary operations (e.g., +, -)
                # In a real implementation, we would handle this more robustly
                return operand

        elif isinstance(node, ast.IfExp):
            # Handle conditional expressions (e.g., x if y else z)
            # In a real implementation, we would handle this more robustly
            test = Parser._parse_expression(node.test, alias)
            body = Parser._parse_expression(node.body, alias)
            orelse = Parser._parse_expression(node.orelse, alias)

            # Ensure all values are Expression objects, not lists or tuples
            if isinstance(test, list) or isinstance(test, tuple):
                raise ValueError(f"Unsupported IfExp: {test}")
            if isinstance(body, list) or isinstance(body, tuple):
                raise ValueError(f"Unsupported IfExp: {body}")
            if isinstance(orelse, list) or isinstance(orelse, tuple):
                raise ValueError(f"Unsupported IfExp: {orelse}")

            # Create a CASE WHEN expression
            return IfExpression(test=test, body=body, orelse=orelse)

        elif isinstance(node, ast.List):
            # Handle tuples and lists (e.g., (1, 2, 3), [1, 2, 3])
            # This is used for array returns in lambdas like lambda x: [x.name, x.age]
            elements = []
            for elt in node.elts:
                elements.append(Parser._parse_expression(elt, alias))
            return elements

        elif isinstance(node, ast.Tuple):
            # Handle Join with rename ( x.col1 == y.col1, [ (x_col1 := x.col1 ), (y_col1 := y.col1 ) ]
            elements = []
            for elt in node.elts:
                elements.append(Parser._parse_expression(elt, alias))
            return elements

        elif isinstance(node, ast.Call):
            # Handle function calls (e.g., sum(x.col1 - x.col2))

            args_list = []
            kwargs = {}

            if isinstance(node.func, ast.Name):
                # Parse the arguments to the function

                for arg in node.args:
                    parsed_arg = Parser._parse_expression(arg, alias)
                    args_list.append(parsed_arg)

                # Handle keyword arguments
                kwargs = {}
                for kw in node.keywords:
                    parsed_kw = Parser._parse_expression(kw.value, alias)
                    kwargs[kw.arg] = parsed_kw

            else:
                ValueError(f"Unsupported function type: {node.func}")

            #if node.func.id not in known_functions:
            #    ValueError(f"Unknown function name: {node.func.id}")

            # very brittle, lots more checks needed here
            if node.func.id == 'over':
                return OverFunction(
                    args_list[0], args_list[1],
                    sort=kwargs.get("sort"), frame=kwargs.get("frame"), qualify=kwargs.get("qualify"))
            elif node.func.id == "aggregate":
                return AggregateFunction(args_list[0], args_list[1], having=kwargs.get("having"))
            elif node.func.id == "rows":
                return RowsFunction(start=args_list[0], end=args_list[1], parameters=[])
            elif node.func.id == "range":
                return RangeFunction(start=args_list[0], end=args_list[1], parameters=[])
            else:
                module = importlib.import_module("functions")
                class_ = getattr(module, f"{node.func.id.title()}Function")
                instance = class_(args_list)
                return instance

        elif isinstance(node, ast.JoinedStr):
            # Handle fstring (e.g. f"hello{blah}")
            # In a real implementation, we would handle this more robustly
            expr = []
            for value in node.values:
                if isinstance(value, ast.Constant):
                    expr.append(Parser._parse_expression(value, alias))
                elif isinstance(value, ast.FormattedValue):
                    if value.format_spec is not None:
                        raise ValueError(f"Format Spec Not Supported: {value.format_spec}")
                    else:
                        expr.append(Parser._parse_expression(value.value, alias))

            return StringConcatFunction(expr)

        elif isinstance(node, ast.Subscript):
            # Handle subscript operations (e.g., x[0], x['key'])
            # In a real implementation, we would handle this more robustly
            raise ValueError(f"Unsupported expression: {node}")

        elif isinstance(node, ast.Dict):
            # Handle dictionaries (e.g., {'a': 1, 'b': 2})
            # In a real implementation, we would handle this more robustly
            raise ValueError(f"Unsupported expression: {node}")

        elif isinstance(node, ast.Set):
            # Handle sets (e.g., {1, 2, 3})
            # In a real implementation, we would handle this more robustly
            raise ValueError(f"Unsupported expression: {node}")

        elif isinstance(node, ast.ListComp) or isinstance(node, ast.SetComp) or isinstance(node, ast.DictComp) or isinstance(node, ast.GeneratorExp):
            # Handle comprehensions (e.g., [x for x in y], {x: y for x in z})
            # In a real implementation, we would handle this more robustly
            raise ValueError(f"Unsupported expression: {node}")

        else:
            # Throw
            raise ValueError(f"Unsupported expression: {node}")

    @staticmethod
    def _get_comparison_operator(op: ast.cmpop) -> BinaryOperator:
        """
        Convert an AST comparison operator to a SQL operator.

        Args:
            op: The AST comparison operator

        Returns:
            The equivalent SQL operator
        """
        if isinstance(op, ast.Eq):
            return BinaryOperator("=")
        elif isinstance(op, ast.NotEq):
            return BinaryOperator("!=")
        elif isinstance(op, ast.Lt):
            return BinaryOperator("<")
        elif isinstance(op, ast.LtE):
            return BinaryOperator("<=")
        elif isinstance(op, ast.Gt):
            return BinaryOperator(">")
        elif isinstance(op, ast.GtE):
            return BinaryOperator(">=")
        elif isinstance(op, ast.In):
            return BinaryOperator("IN")
        elif isinstance(op, ast.NotIn):
            return BinaryOperator("NOT IN")
        elif isinstance(op, ast.Is):
            return BinaryOperator("IS")
        elif isinstance(op, ast.IsNot):
            return BinaryOperator("IS NOT")
        else:
            raise ValueError(f"Unsupported comparison operator {op}")

    @staticmethod
    def _get_binary_operator(op: operator) -> BinaryOperator:
        # Map Python operators to Binary operators
        if isinstance(op, ast.Add):
            return BinaryOperator("+")
        elif isinstance(op, ast.Sub):
            return BinaryOperator("-")
        elif isinstance(op, ast.Mult):
            return BinaryOperator("*")
        elif isinstance(op, ast.Div):
            return BinaryOperator("/")
        elif isinstance(op, ast.Mod):
            return BinaryOperator("%")
        elif isinstance(op, ast.Pow):
            return BinaryOperator("^")
        elif isinstance(op, ast.BitOr):
            return BinaryOperator("|")
        elif isinstance(op, ast.BitAnd):
            return BinaryOperator("&")
        else:
            raise ValueError(f"Unsupported binary operator {op}")
