from dataclasses import dataclass
from typing import Optional, Union

from metamodel import Expression, FunctionExpression, ColumnExpression

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
    functions: Union[FunctionExpression, list[FunctionExpression]]
    filter: Optional[Expression] = None

    def __init__(self,
                 columns: Union[Expression, list[Expression]],
                 functions: Union[ColumnExpression, list[ColumnExpression]],
                 filter: Optional[Expression] = None):
        self.parameters = []
        self.columns = columns
        self.functions = functions
        self.filter = filter

@dataclass
class UnboundedFunction(FunctionExpression):
    pass

@dataclass
class Frame(FunctionExpression):
    start: Union[int, UnboundedFunction]
    end: Union[int, UnboundedFunction]

@dataclass
class RowsFunction(Frame):
    pass

@dataclass
class RangeFunction(Frame):
    pass

@dataclass
class OverFunction(FunctionExpression):
    columns: Union[Expression, list[Expression]]
    functions: Union[FunctionExpression, list[FunctionExpression]]
    sort: Optional[Union[Expression, list[Expression]]]
    frame: Optional[Frame]
    filter: Optional[Expression] = None

    def __init__(self,
                 columns: Union[Expression, list[Expression]],
                 functions: Union[FunctionExpression, list[FunctionExpression]],
                 sort: Optional[Union[Expression, list[Expression]]],
                 frame: Optional[Frame],
                 filter: Optional[Expression] = None):
        self.parameters = []
        self.columns = columns
        self.functions = functions
        self.sort = sort
        self.frame = frame
        self.filter = filter


left = LeftFunction
avg = AvgFunction
count = CountFunction
sum = SumFunction
aggregate = AggregateFunction
over = OverFunction
unbounded = UnboundedFunction
rows = RowsFunction
range = RangeFunction
