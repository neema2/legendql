import unittest

from functions import AggregateFunction, SumFunction, CountFunction, AvgFunction, UnboundedFunction, OverFunction, \
    RowsFunction, StringConcatFunction
from legendql import LegendQL
from parser import Parser
from metamodel import BinaryExpression, ColumnReference, LiteralExpression, BinaryOperator, ColumnExpression, \
    SortExpression, Sort

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
        print(p)

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
            expression=StringConcatFunction(
                parameters=[
                    ColumnReference(name='title', table='employee'),
                    LiteralExpression(value='_'),
                    ColumnReference(name='country', table='employee')])))

    def test_aggregate(self):
        lq = LegendQL.from_("employee", {})
        group = lambda r: aggregate(
            [r.id, r.name],
            [sum_salary := sum(r.salary + 1), count_dept := count(r.department_name)],
            filter=r.sum_salary > 100_000)

        p = Parser.parse(group, lq.query)

        self.assertEqual(p, AggregateFunction(
            columns=[ColumnReference(name='id', table='employee'), ColumnReference(name='name', table='employee')],
            functions=[
                ColumnExpression(
                    name='sum_salary',
                    expression=SumFunction(
                        parameters=[BinaryExpression(
                            left=ColumnReference(name='salary', table='employee'),
                            right=LiteralExpression(value=1),
                            operator=BinaryOperator(value='+'))])),
                ColumnExpression(
                    name='count_dept',
                    expression=CountFunction(
                        parameters=[ColumnReference(name='department_name', table='employee')]))],
            filter=BinaryExpression(
                left=ColumnReference(name='sum_salary', table='employee'),
                right=LiteralExpression(value=100000),
                operator=BinaryOperator(value='>'))))

    def test_window(self):
        lq = LegendQL.from_("employee", {})
        window = lambda r: (avg_val :=
                            over(r.location, avg(r.salary), sort=[r.emp_name, -r.location], frame=rows(0, unbounded())))

        p = Parser.parse(window, lq.query)

        self.assertEqual(p, ColumnExpression(
            name='avg_val',
            expression=OverFunction(
                ColumnReference(name='location', table='employee'),
                AvgFunction(parameters=[ColumnReference(name='salary', table='employee')]),
                sort=[
                    ColumnReference(name='emp_name', table='employee'),
                    SortExpression(direction=Sort.DESC, expression=ColumnReference(name='location', table='employee'))
                ],
                frame = RowsFunction(
                    parameters=[],
                    start=LiteralExpression(value=0),
                    end=UnboundedFunction(parameters=[])),
                filter = None)))

if __name__ == '__main__':
    unittest.main()

