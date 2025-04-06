import unittest

from functions import StringConcatFunction, AggregateFunction, SumFunction, CountFunction, OverFunction, AvgFunction, \
    UnboundedFunction, RowsFunction
from legendql import LegendQL
from parser import Parser
from metamodel import *


class ParserTest(unittest.TestCase):

    def test_join(self):
        lq = LegendQL.from_("employee", {})
        jq = LegendQL.from_("department", {})

        join = lambda e, d: e.dept_id == d.id
        p = Parser.parse_join(join, lq.query, jq.query)

        self.assertEqual(p, BinaryExpression(
            left=ColumnReference(name='dept_id', table='employee'),
            right=ColumnReference(name='id', table='department'),
            operator=BinaryOperator(value='=')))


    def test_filter(self):
        lq = LegendQL.from_("employee", {})
        filter = lambda e: e.start_date > '2021-01-01'
        p = Parser.parse(filter, lq.query)

        self.assertEqual(p, BinaryExpression(
            left=ColumnReference(name='start_date', table='employee'),
            right=LiteralExpression(value='2021-01-01'),
            operator=BinaryOperator(value='>')))  # add assertion here


    def test_nested_filter(self):
        lq = LegendQL.from_("employee", {})
        filter = lambda e: (e.start_date > '2021-01-01') or (e.start_date < '2000-02-02') and (e.salary < 1_000_000)
        p = Parser.parse(filter, lq.query)

        self.assertEqual(p, BinaryExpression(
            left=BinaryExpression(
                left=ColumnReference(name='start_date', table='employee'),
                right=LiteralExpression(value='2021-01-01'),
                operator=BinaryOperator(value='>')),
            right=BinaryExpression(
                left=BinaryExpression(
                    left=ColumnReference(name='start_date', table='employee'),
                    right=LiteralExpression(value='2000-02-02'),
                    operator=BinaryOperator(value='<')),
                right=BinaryExpression(
                    left=ColumnReference(name='salary', table='employee'),
                    right=LiteralExpression(value=1000000),
                    operator=BinaryOperator(value='<')),
                operator=BinaryOperator(value='AND')),
            operator=BinaryOperator(value='OR')))


    def test_extend(self):
        lq = LegendQL.from_("employee", {})
        extend = lambda e: [
            (gross_salary := e.salary + 10),
            (gross_cost := gross_salary + e.benefits)]

        p = Parser.parse(extend, lq.query)

        self.assertEqual(p, [
            ColumnExpression(
                name='gross_salary',
                expression=BinaryExpression(
                    left=ColumnReference(name='salary', table='employee'),
                    right=LiteralExpression(value=10),
                    operator=BinaryOperator(value='+'))),
            ColumnExpression(
                name='gross_cost',
                expression=BinaryExpression(
                    left=ColumnReference(name='gross_salary', table=''),
                    right=ColumnReference(name='benefits', table='employee'),
                    operator=BinaryOperator(value='+')))
        ])


    def test_sort(self):
        lq = LegendQL.from_("employee", {})
        sort = lambda e: [e.sum_gross_cost, -e.country]
        p = Parser.parse(sort, lq.query)

        self.assertEqual(p, [
            ColumnReference(name='sum_gross_cost', table='employee'),
            SortExpression(direction=Sort.DESC, expression=ColumnReference(name='country', table='employee'))
        ])


    def test_fstring(self):
        lq = LegendQL.from_("employee", {})
        fstring = lambda e: (id := f"{e.title}_{e.country}")
        p = Parser.parse(fstring, lq.query)

        self.assertEqual(p, ColumnExpression(
            name='id',
            expression=FunctionExpression(
                function=StringConcatFunction(),
                parameters=[
                    ColumnReference(name='title', table='employee'),
                    LiteralExpression(value='_'),
                    ColumnReference(name='country', table='employee')])))


    def test_aggregate(self):
        lq = LegendQL.from_("employee", {})
        group = lambda r: aggregate(
            [r.id, r.name],
            [sum_salary := sum(r.salary + 1), count_dept := count(r.department_name)],
            having=r.sum_salary > 100_000)

        p = Parser.parse(group, lq.query)
        print(p)

        f = FunctionExpression(
            function=AggregateFunction(),
            parameters=[
                [ColumnReference(name='id', table='employee'),
                 ColumnReference(name='name', table='employee')],
                [ColumnExpression(
                    name='sum_salary',
                    expression=FunctionExpression(
                        function=SumFunction(),
                        parameters=[BinaryExpression(
                            left=ColumnReference(name='salary', table='employee'),
                            right=LiteralExpression(value=1),
                            operator=BinaryOperator(value='+'))])),
                 ColumnExpression(
                    name='count_dept',
                    expression=FunctionExpression(
                        function=CountFunction(),
                        parameters=[ColumnReference(name='department_name', table='employee')]))],
                BinaryExpression(
                    left=ColumnReference(name='sum_salary', table='employee'),
                    right=LiteralExpression(value=100000),
                    operator=BinaryOperator(value='>'))])
        print(f)

        self.assertEqual(str(p), str(f))

    def test_window(self):
        lq = LegendQL.from_("employee", {})
        window = lambda r: (avg_val :=
                            over(r.location, avg(r.salary), sort=[r.emp_name, -r.location], frame=rows(0, unbounded())))

        p = Parser.parse(window, lq.query)
        print(p)

        f = ColumnExpression(
            name='avg_val',
            expression=FunctionExpression(
                function=OverFunction(),
                parameters=[
                    ColumnReference(name='location', table='employee'),
                    FunctionExpression(
                        function=AvgFunction(),
                        parameters=[ColumnReference(name='salary', table='employee')]),
                    [ColumnReference(name='emp_name', table='employee'),
                     SortExpression(
                         direction=Sort.DESC,
                         expression=ColumnReference(name='location', table='employee'))],
                    FunctionExpression(
                        function=RowsFunction(),
                        parameters=[LiteralExpression(value=0),
                                    FunctionExpression(function=UnboundedFunction(), parameters=[])])]))

        self.assertEqual(p, f)

if __name__ == '__main__':
    unittest.main()

