use super::int128;
use encoding::*;
use node::*;
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

pub fn convert(q: AnyResolvedStatementProto) -> Plan {
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

    fn any_stmt(&mut self, q: AnyResolvedStatementProto) -> Plan {
        match q.node.unwrap() {
            ResolvedQueryStmtNode(q) => self.query(q),
            ResolvedCreateStatementNode(q) => match q.node.unwrap() {
                ResolvedCreateIndexStmtNode(q) => self.create_index(q),
                ResolvedCreateTableStmtBaseNode(q) => match q.node.unwrap() {
                    ResolvedCreateTableAsSelectStmtNode(q) => self.create_table_as(q),
                    ResolvedCreateTableStmtNode(q) => self.create_table(q),
                    other => panic!("{:?} not supported", other),
                },
                other => panic!("{:?} not supported", other),
            },
            ResolvedDropStmtNode(q) => self.drop(q),
            ResolvedInsertStmtNode(q) => self.insert(q),
            ResolvedDeleteStmtNode(q) => self.delete(q),
            ResolvedUpdateStmtNode(q) => self.update(q),
            ResolvedRenameStmtNode(q) => self.rename(q),
            ResolvedCreateDatabaseStmtNode(q) => self.create_database(q),
            ResolvedAlterObjectStmtNode(q) => match q.node.unwrap() {
                ResolvedAlterTableStmtNode(q) => self.alter_table(q),
                other => panic!("{:?} not supported", other),
            },
            other => panic!("{:?} not supported", other),
        }
    }

    fn query(&mut self, q: ResolvedQueryStmtProto) -> Plan {
        self.any_resolved_scan(q.query.unwrap())
    }

    fn any_resolved_scan(&mut self, q: AnyResolvedScanProto) -> Plan {
        match q.node.unwrap() {
            ResolvedSingleRowScanNode(q) => self.single_row(q),
            ResolvedTableScanNode(q) => self.table_scan(*q),
            ResolvedJoinScanNode(q) => self.join(*q),
            ResolvedFilterScanNode(q) => self.filter(*q),
            ResolvedSetOperationScanNode(q) => self.set_operation(q),
            ResolvedOrderByScanNode(q) => self.order_by(*q),
            ResolvedLimitOffsetScanNode(q) => self.limit_offset(*q),
            ResolvedWithRefScanNode(q) => self.with_ref(q),
            ResolvedProjectScanNode(q) => self.project(*q),
            ResolvedWithScanNode(q) => self.with(*q),
            ResolvedAggregateScanBaseNode(q) => match q.node.unwrap() {
                ResolvedAggregateScanNode(q) => self.aggregate(*q),
                other => panic!("{:?} not supported", other),
            },
            other => panic!("{:?} not supported", other),
        }
    }
    fn single_row(&mut self, q: ResolvedSingleRowScanProto) -> Plan {
        root(LogicalSingleGet)
    }

    fn table_scan(&mut self, q: ResolvedTableScanProto) -> Plan {
        let table = q.table.unwrap();
        let op = LogicalGet(Table::from(table));
        root(op)
    }

    fn join(&mut self, q: ResolvedJoinScanProto) -> Plan {
        let left = self.any_resolved_scan(*q.left_scan.unwrap());
        let right = self.any_resolved_scan(*q.right_scan.unwrap());
        let mut input = Root(Leaf); // TODO this is clearly wrong
        let predicates = match q.join_expr {
            Some(expr) => self.predicate(*expr, &mut input),
            None => vec![],
        };
        match q.join_type.unwrap() {
            // Inner
            0 => binary(LogicalInnerJoin(predicates), left, right),
            // Left
            1 => binary(LogicalRightJoin(predicates), right, left),
            // Right
            2 => binary(LogicalRightJoin(predicates), left, right),
            // Full
            3 => binary(LogicalOuterJoin(predicates), left, right),
            // Invalid
            other => panic!("{:?} not supported", other),
        }
    }

    fn filter(&mut self, q: ResolvedFilterScanProto) -> Plan {
        let mut input = self.any_resolved_scan(*q.input_scan.unwrap());
        let predicates = self.predicate(*q.filter_expr.unwrap(), &mut input);
        unary(LogicalFilter(predicates), input)
    }

    fn predicate(&mut self, x: AnyResolvedExprProto, outer: &mut Plan) -> Vec<Scalar> {
        if let ResolvedFunctionCallBaseNode(x) = x.clone().node.unwrap() {
            if let ResolvedFunctionCallNode(x) = x.node.unwrap() {
                let x = x.parent.unwrap();
                let f = x.function.unwrap();
                let name = f.name.unwrap();
                if name == "ZetaSQL:$and" {
                    return self.exprs(x.argument_list, outer);
                }
            }
        }
        vec![self.expr(x, outer)]
    }

    fn set_operation(&mut self, q: ResolvedSetOperationScanProto) -> Plan {
        // Note that this nests the operations backwards.
        // For example, `a U b U c` will be nested as (c (b a)).
        // This is important for `a EXCEPT b`, which needs to be nested as
        // (EXCEPT b a) so the build side is on the left.
        let input = q.input_item_list[0].scan.clone();
        let mut right = self.any_resolved_scan(input.unwrap());
        for i in 1..q.input_item_list.len() {
            let operation = self.set_operation_operation(q.op_type.unwrap());
            let input = q.input_item_list[i].scan.clone();
            let left = self.any_resolved_scan(input.unwrap());
            right = binary(operation, left, right);
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
            other => panic!("{:?} not supported", other),
        }
    }

    fn order_by(&mut self, q: ResolvedOrderByScanProto) -> Plan {
        let mut list = vec![];
        for x in q.order_by_item_list {
            let column = Column::from(x.column_ref.unwrap().column.unwrap());
            let desc = x.is_descending.unwrap_or(false);
            list.push(Sort { column, desc });
        }
        let input = self.any_resolved_scan(*q.input_scan.unwrap());
        unary(LogicalSort(list), input)
    }

    fn limit_offset(&mut self, q: ResolvedLimitOffsetScanProto) -> Plan {
        let input = self.any_resolved_scan(*q.input_scan.unwrap());
        let limit = self.int_literal(q.limit);
        let offset = self.int_literal(q.offset);
        unary(LogicalLimit { limit, offset }, input)
    }

    fn int_literal(&mut self, x: Option<Box<AnyResolvedExprProto>>) -> i64 {
        match x {
            Some(x) => match (*x).node.unwrap() {
                ResolvedLiteralNode(x) => match x.value.unwrap().value.unwrap().value.unwrap() {
                    Int64Value(x) => x,
                    other => panic!("{:?}", other),
                },
                other => panic!("{:?}", other),
            },
            None => 0,
        }
    }

    fn project(&mut self, q: ResolvedProjectScanProto) -> Plan {
        let mut input = self.any_resolved_scan(*q.input_scan.unwrap());
        if q.expr_list.len() == 0 {
            return input;
        }
        let mut list = vec![];
        for x in q.expr_list {
            let value = self.expr(x.expr.unwrap(), &mut input);
            let column = Column::from(x.column.unwrap());
            list.push((value, column))
        }
        unary(LogicalProject(list), input)
    }

    fn with(&mut self, q: ResolvedWithScanProto) -> Plan {
        let mut result = self.any_resolved_scan(*q.query.unwrap());
        for i in (0..q.with_entry_list.len()).rev() {
            let q = q.with_entry_list[i].clone();
            let name = q.with_query_name.unwrap();
            let next = self.any_resolved_scan(q.with_subquery.unwrap());
            result = binary(LogicalWith(name), next, result);
        }
        result
    }

    fn with_ref(&mut self, q: ResolvedWithRefScanProto) -> Plan {
        let name = q.with_query_name.unwrap();
        root(LogicalGetWith(name))
    }

    fn aggregate(&mut self, q: ResolvedAggregateScanProto) -> Plan {
        let q = *q.parent.unwrap();
        let mut input = self.any_resolved_scan(*q.clone().input_scan.unwrap());
        let mut project = vec![];
        let mut group_by = vec![];
        let mut aggregate = vec![];
        self.group_by(q.clone(), &mut project, &mut group_by, &mut input);
        for c in q.aggregate_list {
            let call = self.coerce_aggregate_call(c.clone());
            let function = self.convert_aggregate_call(call, &mut project, &mut input);
            let column = Column::from(c.column.unwrap());
            aggregate.push((function, column));
        }
        if project.len() == 0 {
            return unary(LogicalAggregate(group_by, aggregate), input);
        }
        return unary(
            LogicalAggregate(group_by, aggregate),
            unary(LogicalProject(project), input),
        );
    }

    fn group_by(
        &mut self,
        q: ResolvedAggregateScanBaseProto,
        project: &mut Vec<(Scalar, Column)>,
        group_by: &mut Vec<Column>,
        outer: &mut Plan,
    ) {
        for c in q.group_by_list {
            let value = self.expr(c.expr.unwrap(), outer);
            let column = Column::from(c.column.unwrap());
            project.push((value, column.clone()));
            group_by.push(column);
        }
    }

    fn coerce_aggregate_call(
        &mut self,
        c: ResolvedComputedColumnProto,
    ) -> ResolvedAggregateFunctionCallProto {
        match c.expr.unwrap().node.unwrap() {
            ResolvedFunctionCallBaseNode(f) => match (*f).node.unwrap() {
                ResolvedNonScalarFunctionCallBaseNode(f) => match (*f).node.unwrap() {
                    ResolvedAggregateFunctionCallNode(f) => *f,
                    other => panic!("expected aggregate but found {:?}", other),
                },
                other => panic!("expected aggregate but found {:?}", other),
            },
            other => panic!("expected aggregate but found {:?}", other),
        }
    }

    fn convert_aggregate_call(
        &mut self,
        call: ResolvedAggregateFunctionCallProto,
        project: &mut Vec<(Scalar, Column)>,
        outer: &mut Plan,
    ) -> Aggregate {
        let parent = call.parent.unwrap();
        let grandparent = parent.parent.unwrap();
        let distinct = parent.distinct.unwrap();
        let ignore_nulls = parent.null_handling_modifier.unwrap() == 1;
        let argument = self.aggregate_argument(grandparent.clone(), project, outer);
        let function = grandparent.function.unwrap().name.unwrap();
        Aggregate::from(function, distinct, ignore_nulls, argument)
    }

    fn aggregate_argument(
        &mut self,
        call: ResolvedFunctionCallBaseProto,
        project: &mut Vec<(Scalar, Column)>,
        outer: &mut Plan,
    ) -> Option<Column> {
        if call.argument_list.is_empty() {
            None
        } else if call.argument_list.len() == 1 {
            match self.expr(call.argument_list.first().unwrap().clone(), outer) {
                // If aggregate has a column references as its arg, add the column reference directly and continue.
                Scalar::Column(column) => Some(column),
                // Otherwise, generate a pseudo-column to hold the intermediate expression.
                argument => {
                    let function = call.function.unwrap().name.unwrap();
                    let name = function_name(function);
                    let column = self.create_column(String::from("$project"), name, argument.typ());
                    project.push((argument, column.clone()));
                    Some(column)
                }
            }
        } else {
            panic!(
                "expected 1 or 0 arguments but found {:?}",
                call.argument_list.len()
            );
        }
    }

    fn create_index(&mut self, q: ResolvedCreateIndexStmtProto) -> Plan {
        unimplemented!()
    }

    fn create_table_as(&mut self, q: ResolvedCreateTableAsSelectStmtProto) -> Plan {
        unimplemented!()
    }

    fn create_table(&mut self, q: ResolvedCreateTableStmtProto) -> Plan {
        unimplemented!()
    }

    fn drop(&mut self, q: ResolvedDropStmtProto) -> Plan {
        unimplemented!()
    }

    fn insert(&mut self, q: ResolvedInsertStmtProto) -> Plan {
        unimplemented!()
    }

    fn delete(&mut self, q: ResolvedDeleteStmtProto) -> Plan {
        unimplemented!()
    }

    fn update(&mut self, q: ResolvedUpdateStmtProto) -> Plan {
        unimplemented!()
    }

    fn rename(&mut self, q: ResolvedRenameStmtProto) -> Plan {
        unimplemented!()
    }

    fn create_database(&mut self, q: ResolvedCreateDatabaseStmtProto) -> Plan {
        unimplemented!()
    }

    fn alter_table(&mut self, q: ResolvedAlterTableStmtProto) -> Plan {
        unimplemented!()
    }

    fn exprs(&mut self, xs: Vec<AnyResolvedExprProto>, outer: &mut Plan) -> Vec<Scalar> {
        let mut list = vec![];
        for x in xs {
            list.push(self.expr(x, outer));
        }
        list
    }

    fn expr(&mut self, x: AnyResolvedExprProto, outer: &mut Plan) -> Scalar {
        match x.node.unwrap() {
            ResolvedLiteralNode(x) => {
                let x = x.value.unwrap();
                let value = x.value.unwrap();
                let typ = x.r#type.unwrap();
                Scalar::Literal(literal(value, typ))
            }
            ResolvedColumnRefNode(x) => self.column(x.column.unwrap()),
            ResolvedFunctionCallBaseNode(x) => self.function_call(*x, outer),
            ResolvedCastNode(x) => self.cast(*x),
            ResolvedSubqueryExprNode(x) => self.subquery_expr(*x, outer),
            other => panic!("{:?} not supported", other),
        }
    }

    fn column(&mut self, x: ResolvedColumnProto) -> Scalar {
        Scalar::Column(Column::from(x))
    }

    fn function_call(&mut self, x: AnyResolvedFunctionCallBaseProto, outer: &mut Plan) -> Scalar {
        match x.node.unwrap() {
            ResolvedFunctionCallNode(x) => self.scalar_function_call(x, outer),
            other => panic!("{:?} not supported", other),
        }
    }

    fn scalar_function_call(&mut self, x: ResolvedFunctionCallProto, outer: &mut Plan) -> Scalar {
        let x = x.parent.unwrap();
        let f = Function::from(x.function.unwrap().name.unwrap());
        let arguments = self.exprs(x.argument_list, outer);
        Scalar::Call(f, arguments)
    }

    fn cast(&mut self, x: ResolvedCastProto) -> Scalar {
        unimplemented!()
    }

    fn subquery_expr(&mut self, x: ResolvedSubqueryExprProto, outer: &mut Plan) -> Scalar {
        match x.subquery_type.unwrap() {
            // Scalar
            0 => {
                let subquery = *x.subquery.unwrap();
                let corr = self.any_resolved_scan(subquery.clone());
                let scalar = Scalar::Column(Column::from(single_column(subquery)));
                *outer = binary(
                    LogicalSingleJoin(vec![]),
                    corr,
                    mem::replace(outer, Root(Leaf)),
                );
                scalar
            }
            // Array
            1 => unimplemented!(),
            // Exists
            2 => unimplemented!(),
            // In
            3 => {
                let subquery = *x.subquery.unwrap();
                let mark = self.create_column("$mark".to_string(), "$in".to_string(), Type::Bool);
                let corr = self.any_resolved_scan(subquery.clone());
                let inx = self.expr(*x.in_expr.unwrap(), outer);
                let sel = self.column(single_column(subquery));
                let args = vec![inx, sel];
                let equals = Scalar::Call(Function::Equal, args);
                let predicates = vec![equals];
                *outer = binary(
                    LogicalMarkJoin(predicates, mark.clone()),
                    corr,
                    mem::replace(outer, Root(Leaf)),
                );
                Scalar::Column(mark)
            }
            n => panic!("{} is not a subquery type", n),
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

fn single_column(q: AnyResolvedScanProto) -> ResolvedColumnProto {
    match q.node.unwrap() {
        ResolvedSingleRowScanNode(q) => q.parent.unwrap().column_list[0].clone(),
        ResolvedTableScanNode(q) => q.parent.unwrap().column_list[0].clone(),
        ResolvedJoinScanNode(q) => q.parent.unwrap().column_list[0].clone(),
        ResolvedArrayScanNode(q) => q.element_column.unwrap(),
        ResolvedFilterScanNode(q) => q.parent.unwrap().column_list[0].clone(),
        ResolvedSetOperationScanNode(q) => q.parent.unwrap().column_list[0].clone(),
        ResolvedOrderByScanNode(q) => q.parent.unwrap().column_list[0].clone(),
        ResolvedLimitOffsetScanNode(q) => q.parent.unwrap().column_list[0].clone(),
        ResolvedWithRefScanNode(q) => q.parent.unwrap().column_list[0].clone(),
        ResolvedAnalyticScanNode(q) => q.parent.unwrap().column_list[0].clone(),
        ResolvedSampleScanNode(q) => q.parent.unwrap().column_list[0].clone(),
        ResolvedProjectScanNode(q) => q.parent.unwrap().column_list[0].clone(),
        ResolvedWithScanNode(q) => q.parent.unwrap().column_list[0].clone(),
        ResolvedTvfscanNode(q) => q.parent.unwrap().column_list[0].clone(),
        ResolvedRelationArgumentScanNode(q) => q.parent.unwrap().column_list[0].clone(),
        ResolvedAggregateScanBaseNode(q) => {
            let inner = match q.node.unwrap() {
                ResolvedAggregateScanNode(q) => *q,
            };
            inner.parent.unwrap().parent.unwrap().column_list[0].clone()
        }
    }
}

fn function_name(name: String) -> String {
    format!("${}", name.trim_start_matches("ZetaSQL:"))
}

fn literal(value: ValueProto, typ: TypeProto) -> Value {
    match value.value.unwrap() {
        Int64Value(x) => Value::Int64(x),
        BoolValue(x) => Value::Bool(x),
        DoubleValue(x) => Value::Double(x),
        StringValue(x) => Value::String(x),
        BytesValue(x) => Value::Bytes(x),
        DateValue(x) => Value::Date(date_value(x)),
        TimestampValue(x) => Value::Timestamp(timestamp_value(x)),
        ArrayValue(x) => {
            let typ = *typ.array_type.unwrap().element_type.unwrap();
            Value::Array(array_value(x.element, typ))
        }
        StructValue(x) => {
            let types = typ.struct_type.unwrap().field;
            Value::Struct(struct_value(x.field, types))
        }
        NumericValue(x) => Value::Numeric(int128::decode(x)),
        other => panic!("{:?} not supported", other),
    }
}

fn date_value(date: i32) -> chrono::Date<chrono::Utc> {
    unimplemented!()
}

fn timestamp_value(time: prost_types::Timestamp) -> chrono::DateTime<chrono::Utc> {
    unimplemented!()
}

fn array_value(values: Vec<ValueProto>, typ: TypeProto) -> Vec<Value> {
    let mut list = vec![];
    for v in values {
        list.push(literal(v.clone(), typ.clone()));
    }
    list
}

fn struct_value(values: Vec<ValueProto>, types: Vec<StructFieldProto>) -> Vec<(String, Value)> {
    let mut list = vec![];
    for i in 0..list.len() {
        list.push(struct_field(values[i].clone(), types[i].clone()));
    }
    list
}

fn struct_field(value: ValueProto, typ: StructFieldProto) -> (String, Value) {
    let name = typ.field_name.unwrap();
    let literal = literal(value, typ.field_type.unwrap());
    (name, literal)
}
