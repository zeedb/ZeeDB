use super::int128;
use encoding::*;
use node::*;
use zetasql::any_resolved_aggregate_scan_base_proto::Node::*;
use zetasql::any_resolved_alter_object_stmt_proto::Node::*;
use zetasql::any_resolved_create_statement_proto::Node::*;
use zetasql::any_resolved_create_table_stmt_base_proto::Node::*;
use zetasql::any_resolved_expr_proto::Node::*;
use zetasql::any_resolved_function_call_base_proto::Node::*;
use zetasql::any_resolved_scan_proto::Node::*;
use zetasql::any_resolved_statement_proto::Node::*;
use zetasql::value_proto::Value::*;
use zetasql::*;

pub fn convert(q: AnyResolvedStatementProto) -> Plan {
    Converter::new().any_stmt(q)
}

struct Converter {
    created_columns: Vec<(String, Type)>,
    max_column_id: i64,
}

impl Converter {
    fn new() -> Converter {
        let created_columns = vec![];
        let max_column_id = 0;
        Converter {
            created_columns,
            max_column_id,
        }
    }

    fn any_stmt(&self, q: AnyResolvedStatementProto) -> Plan {
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

    fn query(&self, q: ResolvedQueryStmtProto) -> Plan {
        self.any_resolved_scan(q.query.unwrap())
    }

    fn any_resolved_scan(&self, q: AnyResolvedScanProto) -> Plan {
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
    fn single_row(&self, q: ResolvedSingleRowScanProto) -> Plan {
        Plan(LogicalSingleGet, vec![])
    }

    fn table_scan(&self, q: ResolvedTableScanProto) -> Plan {
        let table = q.table.unwrap();
        let op = LogicalGet(Table::from(table));
        Plan(op, vec![])
    }

    fn join(&self, q: ResolvedJoinScanProto) -> Plan {
        let left = self.any_resolved_scan(*q.left_scan.unwrap());
        let right = self.any_resolved_scan(*q.right_scan.unwrap());
        let predicates = match q.join_expr {
            Some(expr) => self.predicate(*expr),
            None => vec![],
        };
        match q.join_type.unwrap() {
            // Inner
            0 => Plan(LogicalOuterJoin(predicates), vec![left, right]),
            // Left
            1 => Plan(LogicalRightJoin(predicates), vec![right, left]),
            // Right
            2 => Plan(LogicalRightJoin(predicates), vec![left, right]),
            // Full
            3 => Plan(LogicalOuterJoin(predicates), vec![left, right]),
            // Invalid
            other => panic!("{:?} not supported", other),
        }
    }

    fn filter(&self, q: ResolvedFilterScanProto) -> Plan {
        let input = self.any_resolved_scan(*q.input_scan.unwrap());
        let predicates = self.predicate(*q.filter_expr.unwrap());
        Plan(LogicalFilter(predicates), vec![input])
    }

    fn predicate(&self, x: AnyResolvedExprProto) -> Vec<Scalar> {
        if let ResolvedFunctionCallBaseNode(x) = x.clone().node.unwrap() {
            if let ResolvedFunctionCallNode(x) = x.node.unwrap() {
                let x = x.parent.unwrap();
                let f = x.function.unwrap();
                let name = f.name.unwrap();
                if name == "ZetaSQL:$and" {
                    return self.exprs(x.argument_list);
                }
            }
        }
        vec![self.expr(x)]
    }

    fn set_operation(&self, q: ResolvedSetOperationScanProto) -> Plan {
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
            right = Plan(operation, vec![left, right]);
        }
        right
    }

    fn set_operation_operation(&self, i: i32) -> Operator {
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

    fn order_by(&self, q: ResolvedOrderByScanProto) -> Plan {
        let mut list = vec![];
        for x in q.order_by_item_list {
            let column = Column::from(x.column_ref.unwrap().column.unwrap());
            let desc = x.is_descending.unwrap_or(false);
            list.push(Sort { column, desc });
        }
        let input = self.any_resolved_scan(*q.input_scan.unwrap());
        Plan(LogicalSort(list), vec![input])
    }

    fn limit_offset(&self, q: ResolvedLimitOffsetScanProto) -> Plan {
        let input = self.any_resolved_scan(*q.input_scan.unwrap());
        let limit = self.int_literal(q.limit);
        let offset = self.int_literal(q.offset);
        Plan(LogicalLimit(Limit { limit, offset }), vec![input])
    }

    fn int_literal(&self, x: Option<Box<AnyResolvedExprProto>>) -> i32 {
        match x {
            Some(x) => match (*x).node.unwrap() {
                ResolvedLiteralNode(x) => match x.value.unwrap().value.unwrap().value.unwrap() {
                    Int32Value(x) => x,
                    _ => 0,
                },
                _ => 0,
            },
            _ => 0,
        }
    }

    fn project(&self, q: ResolvedProjectScanProto) -> Plan {
        let input = self.any_resolved_scan(*q.input_scan.unwrap());
        if q.expr_list.len() == 0 {
            return input;
        }
        let mut list = vec![];
        for x in q.expr_list {
            let value = self.expr(x.expr.unwrap());
            let column = Column::from(x.column.unwrap());
            list.push((value, column))
        }
        Plan(LogicalProject(list), vec![input])
    }

    fn with(&self, q: ResolvedWithScanProto) -> Plan {
        let mut result = self.any_resolved_scan(*q.query.unwrap());
        for i in (1..q.with_entry_list.len()).rev() {
            let q = q.with_entry_list[i].clone();
            let name = q.with_query_name.unwrap();
            let next = self.any_resolved_scan(q.with_subquery.unwrap());
            result = Plan(LogicalWith(name), vec![next, result]);
        }
        result
    }

    fn with_ref(&self, q: ResolvedWithRefScanProto) -> Plan {
        let name = q.with_query_name.unwrap();
        Plan(LogicalGetWith(name), vec![])
    }

    fn aggregate(&self, q: ResolvedAggregateScanProto) -> Plan {
        unimplemented!()
    }

    fn create_index(&self, q: ResolvedCreateIndexStmtProto) -> Plan {
        unimplemented!()
    }

    fn create_table_as(&self, q: ResolvedCreateTableAsSelectStmtProto) -> Plan {
        unimplemented!()
    }

    fn create_table(&self, q: ResolvedCreateTableStmtProto) -> Plan {
        unimplemented!()
    }

    fn drop(&self, q: ResolvedDropStmtProto) -> Plan {
        unimplemented!()
    }

    fn insert(&self, q: ResolvedInsertStmtProto) -> Plan {
        unimplemented!()
    }

    fn delete(&self, q: ResolvedDeleteStmtProto) -> Plan {
        unimplemented!()
    }

    fn update(&self, q: ResolvedUpdateStmtProto) -> Plan {
        unimplemented!()
    }

    fn rename(&self, q: ResolvedRenameStmtProto) -> Plan {
        unimplemented!()
    }

    fn create_database(&self, q: ResolvedCreateDatabaseStmtProto) -> Plan {
        unimplemented!()
    }

    fn alter_table(&self, q: ResolvedAlterTableStmtProto) -> Plan {
        unimplemented!()
    }

    fn exprs(&self, xs: Vec<AnyResolvedExprProto>) -> Vec<Scalar> {
        let mut list = vec![];
        for x in xs {
            list.push(self.expr(x))
        }
        list
    }

    fn expr(&self, x: AnyResolvedExprProto) -> Scalar {
        match x.node.unwrap() {
            ResolvedLiteralNode(x) => {
                let x = x.value.unwrap();
                let value = x.value.unwrap();
                let typ = x.r#type.unwrap();
                Scalar::Literal(literal(value, typ))
            }
            ResolvedColumnRefNode(x) => self.column(x),
            ResolvedFunctionCallBaseNode(x) => self.function_call(*x),
            ResolvedCastNode(x) => self.cast(*x),
            ResolvedSubqueryExprNode(x) => self.subquery_expr(*x),
            other => panic!("{:?} not supported", other),
        }
    }

    fn column(&self, x: ResolvedColumnRefProto) -> Scalar {
        Scalar::Column(Column::from(x.column.unwrap()))
    }

    fn function_call(&self, x: AnyResolvedFunctionCallBaseProto) -> Scalar {
        match x.node.unwrap() {
            ResolvedFunctionCallNode(x) => self.scalar_function_call(x),
            other => panic!("{:?} not supported", other),
        }
    }

    fn scalar_function_call(&self, x: ResolvedFunctionCallProto) -> Scalar {
        let x = x.parent.unwrap();
        let f = x.function.unwrap().name.unwrap();
        Scalar::Call(Function::from(f), self.exprs(x.argument_list))
    }

    fn cast(&self, x: ResolvedCastProto) -> Scalar {
        unimplemented!()
    }

    fn subquery_expr(&self, x: ResolvedSubqueryExprProto) -> Scalar {
        unimplemented!()
    }
}

fn literal(value: ValueProto, typ: TypeProto) -> Literal {
    match value.value.unwrap() {
        Int64Value(x) => Literal::Int(x),
        BoolValue(x) => Literal::Bool(x),
        DoubleValue(x) => Literal::Double(x),
        StringValue(x) => Literal::String(x),
        BytesValue(x) => Literal::Bytes(x),
        DateValue(x) => Literal::Date(date_value(x)),
        TimestampValue(x) => Literal::Timestamp(timestamp_value(x)),
        ArrayValue(x) => {
            let typ = *typ.array_type.unwrap().element_type.unwrap();
            Literal::Array(array_value(x.element, typ))
        }
        StructValue(x) => {
            let types = typ.struct_type.unwrap().field;
            Literal::Struct(struct_value(x.field, types))
        }
        NumericValue(x) => Literal::Numeric(int128::decode(x)),
        other => panic!("{:?} not supported", other),
    }
}

fn date_value(date: i32) -> chrono::Date<chrono::Utc> {
    unimplemented!()
}

fn timestamp_value(time: prost_types::Timestamp) -> chrono::DateTime<chrono::Utc> {
    unimplemented!()
}

fn array_value(values: Vec<ValueProto>, typ: TypeProto) -> Vec<Literal> {
    let mut list = vec![];
    for v in values {
        list.push(literal(v.clone(), typ.clone()));
    }
    list
}

fn struct_value(values: Vec<ValueProto>, types: Vec<StructFieldProto>) -> Vec<(String, Literal)> {
    let mut list = vec![];
    for i in 0..list.len() {
        list.push(struct_field(values[i].clone(), types[i].clone()));
    }
    list
}

fn struct_field(value: ValueProto, typ: StructFieldProto) -> (String, Literal) {
    let name = typ.field_name.unwrap();
    let literal = literal(value, typ.field_type.unwrap());
    (name, literal)
}
