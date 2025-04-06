from dataclasses import dataclass

from dsl.dsl_functions import count
from dsl.metamodel import Function


@dataclass
class AggregationFunction(Function):
    # sum(), min(), max(), count(), avg()
    pass

@dataclass
class ScalarFunction(Function):
    # date_diff(), left(), abs() ..
    pass

@dataclass
class WindowFunction(Function):
    # rank(), row_number(), first(), last() ..
    pass

@dataclass
class RankFunction(WindowFunction):
    pass

@dataclass
class RowNumberFunction(WindowFunction):
    pass

@dataclass
class LeadFunction(WindowFunction):
    pass

@dataclass
class LagFunction(WindowFunction):
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
class OverFunction(ScalarFunction):
    pass

@dataclass
class RowsFunction(ScalarFunction):
    pass

@dataclass
class RangeFunction(ScalarFunction):
    pass

@dataclass
class UnboundedFunction(ScalarFunction):
    pass

@dataclass
class AggregateFunction(ScalarFunction):
    pass