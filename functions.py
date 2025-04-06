from dataclasses import dataclass
from typing import Optional, Union

from metamodel import Expression, FunctionExpression

@dataclass
class AggregationFunction(FunctionExpression):
    pass

@dataclass
class ScalarFunction(FunctionExpression):
    pass

@dataclass
class LeftFunction(ScalarFunction):
    pass

@dataclass
class StringConcatFunction(ScalarFunction):
    pass

@dataclass
class AvgFunction(AggregationFunction):
    pass

@dataclass
class CountFunction(AggregationFunction):
    pass

@dataclass
class SumFunction(AggregationFunction):
    pass

@dataclass
class AggregateFunction(FunctionExpression):
    columns: Union[Expression, list[Expression]]
    functions: Union[Expression, list[Expression]]
    having: Optional[Expression] = None

    def __init__(self,
                 columns: Union[Expression, list[Expression]],
                 functions: Union[Expression, list[Expression]],
                 having: Optional[Expression] = None):
        self.parameters = []
        self.columns = columns
        self.functions = functions
        self.having = having

@dataclass
class UnboundedFunction(FunctionExpression):
    pass

@dataclass
class Frame(FunctionExpression):
    start: Union[int, UnboundedFunction, Expression]
    end: Union[int, UnboundedFunction, Expression]

@dataclass
class RowsFunction(Frame):
    pass

@dataclass
class RangeFunction(Frame):
    pass

@dataclass
class OverFunction(FunctionExpression):
    columns: Union[Expression, list[Expression]]
    functions: Union[Expression, list[Expression]]
    sort: Optional[Union[Expression, list[Expression]]]
    frame: Optional[Frame]
    qualify: Optional[Expression] = None

    def __init__(self,
                 columns: Union[Expression, list[Expression]],
                 functions: Union[Expression, list[Expression]],
                 sort: Optional[Union[Expression, list[Expression]]],
                 frame: Optional[Frame],
                 qualify: Optional[Expression] = None):
        self.parameters = []
        self.columns = columns
        self.functions = functions
        self.sort = sort
        self.frame = frame
        self.qualify = qualify


left = LeftFunction
avg = AvgFunction
count = CountFunction
sum = SumFunction
aggregate = AggregateFunction
over = OverFunction
unbounded = UnboundedFunction
rows = RowsFunction
range = RangeFunction
