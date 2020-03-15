use super::int128;
use encoding::*;
use node::*;
use std::borrow::Borrow;
use std::mem;
use zetasql::any_resolved_aggregate_scan_base_proto::Node::*;
use zetasql::any_resolved_alter_object_stmt_proto::Node::*;
use zetasql::any_resolved_create_statement_proto::Node::*;
use zetasql::any_resolved_create_table_stmt_base_proto::Node::*;
use zetasql::any_resolved_expr_proto::Node::*;
use zetasql::any_resolved_function_call_base_proto::Node::*;
use zetasql::any_resolved_non_scalar_function_call_base_proto::Node::*;
use zetasql::any_resolved_scan_proto::Node::*;
use zetasql::any_resolved_statement_proto::Node::*;
use zetasql::value_proto::Value::*;
use zetasql::*;

pub fn convert(q: &AnyResolvedStatementProto) -> Plan {
    Converter::new().any_stmt(q)
}

struct Converter {
    next_column_id: i64,
}

fn root(operator: Operator) -> Plan {
    return Root(operator);
}

fn unary(operator: Operator, input: Plan) -> Plan {
    return Unary(operator, Box::new(input));
}

fn binary(operator: Operator, left: Plan, right: Plan) -> Plan {
    return Binary(operator, Box::new(left), Box::new(right));
}

impl Converter {
    fn new() -> Converter {
        Converter { next_column_id: -1 }
    }

    fn any_stmt(&mut self, q: &AnyResolvedStatementProto) -> Plan {
        match q.node.get() {
            ResolvedQueryStmtNode(q) => self.query(q),
            ResolvedCreateStatementNode(q) => self.create(q),
            ResolvedDropStmtNode(q) => self.drop(q),
            ResolvedInsertStmtNode(q) => self.insert(q),
            ResolvedDeleteStmtNode(q) => self.delete(q),
            ResolvedUpdateStmtNode(q) => self.update(q),
            ResolvedRenameStmtNode(q) => self.rename(q),
            ResolvedCreateDatabaseStmtNode(q) => self.create_database(q),
            ResolvedAlterObjectStmtNode(q) => self.alter(q),
            other => panic!("{:?}", other),
        }
    }

    fn query(&mut self, q: &ResolvedQueryStmtProto) -> Plan {
        self.any_resolved_scan(q.query.get())
    }

    fn any_resolved_scan(&mut self, q: &AnyResolvedScanProto) -> Plan {
        match q.node.get() {
            ResolvedSingleRowScanNode(q) => self.single_row(q),
            ResolvedTableScanNode(q) => self.table_scan(q),
            ResolvedJoinScanNode(q) => self.join(q),
            ResolvedFilterScanNode(q) => self.filter(q),
            ResolvedSetOperationScanNode(q) => self.set_operation(q),
            ResolvedOrderByScanNode(q) => self.order_by(q),
            ResolvedLimitOffsetScanNode(q) => self.limit_offset(q),
            ResolvedWithRefScanNode(q) => self.with_ref(q),
            ResolvedProjectScanNode(q) => self.project(q),
            ResolvedWithScanNode(q) => self.with(q),
            ResolvedAggregateScanBaseNode(q) => match q.node.get() {
                ResolvedAggregateScanNode(q) => self.aggregate(q),
                other => panic!("{:?}", other),
            },
            other => panic!("{:?}", other),
        }
    }
    fn single_row(&mut self, q: &ResolvedSingleRowScanProto) -> Plan {
        root(LogicalSingleGet)
    }

    fn table_scan(&mut self, q: &ResolvedTableScanProto) -> Plan {
        root(LogicalGet(Table::from(q.table.get())))
    }

    fn join(&mut self, q: &ResolvedJoinScanProto) -> Plan {
        let left = self.any_resolved_scan(q.left_scan.get());
        let right = self.any_resolved_scan(q.right_scan.get());
        let mut input = Root(Leaf); // TODO this is clearly wrong
        let predicates = match &q.join_expr {
            Some(expr) => self.predicate(expr.borrow(), &mut input),
            None => vec![],
        };
        match q.join_type.get().borrow() {
            // Inner
            0 => binary(LogicalInnerJoin(predicates), left, right),
            // Left
            1 => binary(LogicalRightJoin(predicates), right, left),
            // Right
            2 => binary(LogicalRightJoin(predicates), left, right),
            // Full
            3 => binary(LogicalOuterJoin(predicates), left, right),
            // Invalid
            other => panic!("{:?}", other),
        }
    }

    fn filter(&mut self, q: &ResolvedFilterScanProto) -> Plan {
        let mut input = self.any_resolved_scan(q.input_scan.get());
        let predicates = self.predicate(q.filter_expr.get(), &mut input);
        unary(LogicalFilter(predicates), input)
    }

    fn predicate(&mut self, x: &AnyResolvedExprProto, outer: &mut Plan) -> Vec<Scalar> {
        match self.predicate_and(x, outer) {
            Some(ps) => ps,
            None => vec![self.expr(x, outer)],
        }
    }

    fn predicate_and(&mut self, x: &AnyResolvedExprProto, outer: &mut Plan) -> Option<Vec<Scalar>> {
        let x = match x.node.get() {
            ResolvedFunctionCallBaseNode(x) => x,
            _ => return None,
        };
        let x = match x.node.get() {
            ResolvedFunctionCallNode(x) => x,
            _ => return None,
        };
        let x = x.parent.get();
        if x.function.get().name.get() != "ZetaSQL:$and" {
            return None;
        }
        Some(self.exprs(&x.argument_list, outer))
    }

    fn set_operation(&mut self, q: &ResolvedSetOperationScanProto) -> Plan {
        // Note that this nests the operations backwards.
        // For example, `a U b U c` will be nested as (c (b a)).
        // This is important for `a EXCEPT b`, which needs to be nested as
        // (EXCEPT b a) so the build side is on the left.
        let head = &q.input_item_list[0];
        let tail = &q.input_item_list[1..];
        let mut right = self.any_resolved_scan(head.scan.get());
        for input in tail {
            let op = self.set_operation_operation(*q.op_type.get());
            let left = self.any_resolved_scan(input.scan.get());
            right = binary(op, left, right);
        }
        right
    }

    fn set_operation_operation(&mut self, i: i32) -> Operator {
        match i {
            // UnionAll
            0 => LogicalUnion,
            // UnionDistinct
            1 => panic!("UNION DISTINCT is not supported"), // TODO
            // IntersectAll
            2 => LogicalIntersect,
            // IntersectDistinct
            3 => panic!("INTERSECT DISTINCT is not supported"), // TODO
            // ExceptAll
            4 => LogicalExcept,
            // ExceptDistinct
            5 => panic!("EXCEPT DISTINCT is not supported"), // TODO
            // Other
            other => panic!("{:?}", other),
        }
    }

    fn order_by(&mut self, q: &ResolvedOrderByScanProto) -> Plan {
        let input = self.any_resolved_scan(q.input_scan.get().borrow());
        let mut list = vec![];
        for x in &q.order_by_item_list {
            let column = Column::from(&x.column_ref.get().column.get());
            let desc = x.is_descending.unwrap_or(false);
            list.push(Sort { column, desc });
        }
        unary(LogicalSort(list), input)
    }

    fn limit_offset(&mut self, q: &ResolvedLimitOffsetScanProto) -> Plan {
        let input = self.any_resolved_scan(q.input_scan.get().borrow());
        let limit = self.int_literal(q.limit.get().borrow());
        let offset = match &q.offset {
            Some(offset) => self.int_literal(offset),
            None => 0,
        };
        unary(LogicalLimit { limit, offset }, input)
    }

    fn int_literal(&mut self, x: &AnyResolvedExprProto) -> i64 {
        match x {
            AnyResolvedExprProto {
                node:
                    Some(ResolvedLiteralNode(ResolvedLiteralProto {
                        value:
                            Some(ValueWithTypeProto {
                                value:
                                    Some(ValueProto {
                                        value: Some(Int64Value(x)),
                                        ..
                                    }),
                                ..
                            }),
                        ..
                    })),
                ..
            } => *x,
            other => panic!("{:?}", other),
        }
    }

    fn project(&mut self, q: &ResolvedProjectScanProto) -> Plan {
        if q.expr_list.len() == 0 {
            self.any_resolved_scan(q.input_scan.get())
        } else {
            let mut input = self.any_resolved_scan(q.input_scan.get());
            let mut list = vec![];
            for x in &q.expr_list {
                list.push(self.computed_column(x, &mut input));
            }
            unary(LogicalProject(list), input)
        }
    }

    fn computed_column(
        &mut self,
        x: &ResolvedComputedColumnProto,
        input: &mut Plan,
    ) -> (Scalar, Column) {
        let value = self.expr(x.expr.get(), input);
        let column = Column::from(x.column.get());
        (value, column)
    }

    fn with(&mut self, q: &ResolvedWithScanProto) -> Plan {
        let mut right = self.any_resolved_scan(q.query.get().borrow());
        for i in (0..q.with_entry_list.len()).rev() {
            match &q.with_entry_list[i] {
                ResolvedWithEntryProto {
                    with_query_name: Some(name),
                    with_subquery: Some(query),
                    ..
                } => {
                    let left = self.any_resolved_scan(&query);
                    right = binary(LogicalWith(name.clone()), left, right);
                }
                other => panic!("{:?}", other),
            }
        }
        right
    }

    fn with_ref(&mut self, q: &ResolvedWithRefScanProto) -> Plan {
        root(LogicalGetWith(q.with_query_name.get().clone()))
    }

    fn aggregate(&mut self, q: &ResolvedAggregateScanProto) -> Plan {
        let q = q.parent.get();
        let mut input = self.any_resolved_scan(q.input_scan.get());
        let mut project = vec![];
        let mut group_by = vec![];
        let mut aggregate = vec![];
        for c in &q.group_by_list {
            group_by.push(self.compute(c, &mut project, &mut input));
        }
        for c in &q.aggregate_list {
            let expr = self.reduce(c, &mut project, &mut input);
            let column = Column::from(&c.column.get());
            aggregate.push((expr, column));
        }
        if project.len() > 0 {
            input = unary(LogicalProject(project), input);
        }
        unary(LogicalAggregate(group_by, aggregate), input)
    }

    fn compute(
        &mut self,
        compute: &ResolvedComputedColumnProto,
        project: &mut Vec<(Scalar, Column)>,
        input: &mut Plan,
    ) -> Column {
        let expr = compute.expr.get();
        let column = compute.column.get();
        project.push((self.expr(expr, input), Column::from(column)));
        Column::from(column)
    }

    fn reduce(
        &mut self,
        aggregate: &ResolvedComputedColumnProto,
        project: &mut Vec<(Scalar, Column)>,
        input: &mut Plan,
    ) -> Aggregate {
        let function = match aggregate.expr.get().node.get() {
            ResolvedFunctionCallBaseNode(function) => function,
            other => panic!("{:?}", other),
        };
        let function = match function.node.get() {
            ResolvedNonScalarFunctionCallBaseNode(function) => function,
            other => panic!("{:?}", other),
        };
        let function = match function.node.get() {
            ResolvedAggregateFunctionCallNode(function) => function,
            other => panic!("{:?}", other),
        };
        let function = function.parent.get();
        let distinct = function.distinct.unwrap_or(false);
        let ignore_nulls = function.null_handling_modifier.unwrap_or(0) == 1;
        let function = function.parent.get();
        let arguments = &function.argument_list;
        let function = function.function.get().name.get().clone();
        let argument = if arguments.len() == 0 {
            None
        } else if arguments.len() == 1 {
            let argument = self.expr(arguments.first().get(), input);
            let column = self.create_column(
                String::from("$proj"),
                function_name(&function),
                argument.typ(),
            );
            project.push((argument, column.clone()));
            Some(column.clone())
        } else {
            panic!("expected 1 or 0 arguments but found {:?}", arguments.len());
        };
        Aggregate::from(function, distinct, ignore_nulls, argument)
    }

    fn create(&mut self, q: &AnyResolvedCreateStatementProto) -> Plan {
        match q.node.get() {
            ResolvedCreateIndexStmtNode(q) => self.create_index(q),
            ResolvedCreateTableStmtBaseNode(AnyResolvedCreateTableStmtBaseProto {
                node: Some(ResolvedCreateTableAsSelectStmtNode(q)),
            }) => self.create_table_as(q),
            ResolvedCreateTableStmtBaseNode(AnyResolvedCreateTableStmtBaseProto {
                node: Some(ResolvedCreateTableStmtNode(q)),
            }) => self.create_table(q),
            other => panic!("{:?}", other),
        }
    }

    fn create_index(&mut self, q: &ResolvedCreateIndexStmtProto) -> Plan {
        unimplemented!()
    }

    fn create_table_as(&mut self, q: &ResolvedCreateTableAsSelectStmtProto) -> Plan {
        unimplemented!()
    }

    fn create_table(&mut self, q: &ResolvedCreateTableStmtProto) -> Plan {
        unimplemented!()
    }

    fn drop(&mut self, q: &ResolvedDropStmtProto) -> Plan {
        unimplemented!()
    }

    fn insert(&mut self, q: &ResolvedInsertStmtProto) -> Plan {
        unimplemented!()
    }

    fn delete(&mut self, q: &ResolvedDeleteStmtProto) -> Plan {
        unimplemented!()
    }

    fn update(&mut self, q: &ResolvedUpdateStmtProto) -> Plan {
        unimplemented!()
    }

    fn rename(&mut self, q: &ResolvedRenameStmtProto) -> Plan {
        unimplemented!()
    }

    fn create_database(&mut self, q: &ResolvedCreateDatabaseStmtProto) -> Plan {
        unimplemented!()
    }

    fn alter(&mut self, q: &AnyResolvedAlterObjectStmtProto) -> Plan {
        let q = match q {
            AnyResolvedAlterObjectStmtProto { node: Some(q) } => q,
            other => panic!("{:?}", other),
        };
        match q {
            ResolvedAlterTableStmtNode(q) => self.alter_table(q),
            other => panic!("{:?}", other),
        }
    }

    fn alter_table(&mut self, q: &ResolvedAlterTableStmtProto) -> Plan {
        unimplemented!()
    }

    fn exprs(&mut self, xs: &Vec<AnyResolvedExprProto>, outer: &mut Plan) -> Vec<Scalar> {
        let mut list = vec![];
        for x in xs {
            list.push(self.expr(x, outer));
        }
        list
    }

    fn expr(&mut self, x: &AnyResolvedExprProto, outer: &mut Plan) -> Scalar {
        match x.node.get() {
            ResolvedLiteralNode(x) => {
                let value = x.value.get().value.get();
                let typ = x.value.get().r#type.get();
                Scalar::Literal(literal(value, typ))
            }
            ResolvedColumnRefNode(x) => self.column(x.column.get()),
            ResolvedFunctionCallBaseNode(x) => self.function_call(x, outer),
            ResolvedCastNode(x) => self.cast(x),
            ResolvedSubqueryExprNode(x) => self.subquery_expr(x, outer),
            other => panic!("{:?}", other),
        }
    }

    fn column(&mut self, x: &ResolvedColumnProto) -> Scalar {
        Scalar::Column(Column::from(x))
    }

    fn function_call(&mut self, x: &AnyResolvedFunctionCallBaseProto, outer: &mut Plan) -> Scalar {
        let (function, arguments) = match x {
            AnyResolvedFunctionCallBaseProto {
                node:
                    Some(ResolvedFunctionCallNode(ResolvedFunctionCallProto {
                        parent:
                            Some(ResolvedFunctionCallBaseProto {
                                function:
                                    Some(FunctionRefProto {
                                        name: Some(function),
                                    }),
                                argument_list,
                                ..
                            }),
                        ..
                    })),
            } => (function, argument_list),
            other => panic!("{:?}", other),
        };
        let function = Function::from(function.clone());
        let arguments = self.exprs(arguments, outer);
        Scalar::Call(function, arguments)
    }

    fn cast(&mut self, x: &ResolvedCastProto) -> Scalar {
        unimplemented!()
    }

    fn subquery_expr(&mut self, x: &ResolvedSubqueryExprProto, outer: &mut Plan) -> Scalar {
        let subquery_type = x.subquery_type.get();
        let subquery = x.subquery.get();
        let corr = self.any_resolved_scan(subquery);
        let column = single_column(subquery);
        match subquery_type {
            // Scalar
            0 => {
                *outer = binary(
                    LogicalSingleJoin(vec![]),
                    corr,
                    mem::replace(outer, Root(Leaf)),
                );
                Scalar::Column(Column::from(column))
            }
            // Array
            1 => unimplemented!(),
            // Exists
            2 => unimplemented!(),
            // In
            3 => {
                let mark = self.create_column("$mark".to_string(), "$in".to_string(), Type::Bool);
                let inx = match x {
                    ResolvedSubqueryExprProto {
                        in_expr: Some(x), ..
                    } => self.expr(x, outer),
                    other => panic!("{:?}", other),
                };
                let sel = self.column(column);
                let args = vec![inx, sel];
                let equals = Scalar::Call(Function::Equal, args);
                let predicates = vec![equals];
                *outer = binary(
                    LogicalMarkJoin(predicates, mark.clone()),
                    corr,
                    mem::replace(outer, Root(Leaf)),
                );
                Scalar::Column(mark.clone())
            }
            other => panic!("{:?}", other),
        }
    }

    fn create_column(&mut self, table: String, name: String, typ: Type) -> Column {
        let column = Column {
            id: self.next_column_id,
            name,
            typ,
        };
        self.next_column_id -= 1;
        column
    }
}

fn single_column(q: &AnyResolvedScanProto) -> &ResolvedColumnProto {
    let q = match q.node.get() {
        ResolvedSingleRowScanNode(q) => q.parent.get(),
        ResolvedTableScanNode(q) => q.parent.get(),
        ResolvedJoinScanNode(q) => q.parent.get(),
        ResolvedArrayScanNode(q) => q.parent.get(),
        ResolvedFilterScanNode(q) => q.parent.get(),
        ResolvedSetOperationScanNode(q) => q.parent.get(),
        ResolvedOrderByScanNode(q) => q.parent.get(),
        ResolvedLimitOffsetScanNode(q) => q.parent.get(),
        ResolvedWithRefScanNode(q) => q.parent.get(),
        ResolvedAnalyticScanNode(q) => q.parent.get(),
        ResolvedSampleScanNode(q) => q.parent.get(),
        ResolvedProjectScanNode(q) => q.parent.get(),
        ResolvedWithScanNode(q) => q.parent.get(),
        ResolvedTvfscanNode(q) => q.parent.get(),
        ResolvedRelationArgumentScanNode(q) => q.parent.get(),
        ResolvedAggregateScanBaseNode(q) => single_column_aggregate(q),
        other => panic!("{:?}", other),
    };
    &q.column_list[0]
}

fn single_column_aggregate(q: &AnyResolvedAggregateScanBaseProto) -> &ResolvedScanProto {
    match q.node.get() {
        ResolvedAggregateScanNode(q) => q.parent.get().parent.get(),
        other => panic!("{:?}", other),
    }
}

fn function_name(name: &String) -> String {
    format!("${}", name.trim_start_matches("ZetaSQL:"))
}

fn literal(value: &ValueProto, typ: &TypeProto) -> Value {
    let value = match value {
        ValueProto { value: Some(value) } => value,
        other => panic!("{:?}", other),
    };
    match value {
        Int64Value(x) => Value::Int64(*x),
        BoolValue(x) => Value::Bool(*x),
        DoubleValue(x) => Value::Double(*x),
        StringValue(x) => Value::String(x.clone()),
        BytesValue(x) => Value::Bytes(x.clone()),
        DateValue(x) => Value::Date(date_value(*x)),
        TimestampValue(x) => Value::Timestamp(timestamp_value(x)),
        ArrayValue(x) => Value::Array(array_value(&x.element, element_type(typ))),
        StructValue(x) => Value::Struct(struct_value(&x.field, field_types(typ))),
        NumericValue(x) => Value::Numeric(int128::decode(x)),
        other => panic!("{:?}", other),
    }
}

fn element_type(typ: &TypeProto) -> &TypeProto {
    let typ = match typ {
        TypeProto {
            array_type: Some(typ),
            ..
        } => typ,
        other => panic!("{:?}", other),
    };
    match typ.borrow() {
        ArrayTypeProto {
            element_type: Some(typ),
        } => &*typ,
        other => panic!("{:?}", other),
    }
}

fn field_types(typ: &TypeProto) -> &Vec<StructFieldProto> {
    match typ {
        TypeProto {
            struct_type: Some(StructTypeProto { field }),
            ..
        } => field,
        other => panic!("{:?}", other),
    }
}

fn date_value(date: i32) -> chrono::Date<chrono::Utc> {
    unimplemented!()
}

fn timestamp_value(time: &prost_types::Timestamp) -> chrono::DateTime<chrono::Utc> {
    unimplemented!()
}

fn array_value(values: &Vec<ValueProto>, typ: &TypeProto) -> Vec<Value> {
    let mut list = vec![];
    for v in values {
        list.push(literal(&v, &typ));
    }
    list
}

fn struct_value(values: &Vec<ValueProto>, types: &Vec<StructFieldProto>) -> Vec<(String, Value)> {
    let mut list = vec![];
    for i in 0..list.len() {
        list.push(struct_field(&values[i], &types[i]));
    }
    list
}

fn struct_field(value: &ValueProto, typ: &StructFieldProto) -> (String, Value) {
    match typ {
        StructFieldProto {
            field_name: Some(name),
            field_type: Some(typ),
        } => {
            let literal = literal(value, typ);
            (name.clone(), literal)
        }
        other => panic!("{:?}", other),
    }
}

trait Getter<T> {
    fn get(&self) -> &T;
}

impl<T> Getter<T> for Option<T> {
    fn get(&self) -> &T {
        match self {
            Some(value) => value,
            None => panic!("None"),
        }
    }
}
