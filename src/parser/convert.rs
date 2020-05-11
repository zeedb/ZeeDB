use encoding::*;
use node::*;
use std::borrow::Borrow;
use std::mem;
use zetasql::any_resolved_aggregate_scan_base_proto::Node::*;
use zetasql::any_resolved_alter_action_proto::Node::*;
use zetasql::any_resolved_alter_object_stmt_proto::Node::*;
use zetasql::any_resolved_create_statement_proto::Node::*;
use zetasql::any_resolved_create_table_stmt_base_proto::Node::*;
use zetasql::any_resolved_expr_proto::Node::*;
use zetasql::any_resolved_function_call_base_proto::Node::*;
use zetasql::any_resolved_non_scalar_function_call_base_proto::Node::*;
use zetasql::any_resolved_scan_proto::Node::*;
use zetasql::any_resolved_statement_proto::Node::*;
use zetasql::resolved_create_statement_enums::*;
use zetasql::resolved_insert_stmt_enums::*;
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
            let mut project = vec![];
            for x in &q.expr_list {
                project.push(self.computed_column(x, &mut input));
            }
            unary(LogicalProject(project), input)
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
        let name = Name {
            path: q.parent.get().name_path.clone(),
        };
        let table = Name {
            path: q.table_name_path.clone(),
        };
        if q.is_unique == Some(true) {
            panic!("unique index is not supported")
        }
        let mut columns = vec![];
        for item in &q.index_item_list {
            if item.descending == Some(true) {
                panic!("descending index is not supported")
            }
            columns.push(item.column_ref.get().column.get().name.get().clone())
        }
        root(LogicalCreateIndex {
            name,
            table,
            columns,
        })
    }

    fn create_table_as(&mut self, q: &ResolvedCreateTableAsSelectStmtProto) -> Plan {
        let input = self.any_resolved_scan(q.query.get());
        let mut project = vec![];
        for i in 0..q.output_column_list.len() {
            let value = Scalar::Column(Column::from(q.output_column_list[i].column.get()));
            let column = Column::from(q.parent.get().column_definition_list[i].column.get());
            project.push((value, column))
        }
        unary(LogicalProject(project), input)
    }

    fn create_table(&mut self, q: &ResolvedCreateTableStmtProto) -> Plan {
        root(self.create_table_base(q.parent.get()))
    }

    fn create_table_base(&mut self, q: &ResolvedCreateTableStmtBaseProto) -> Operator {
        // TODO fail on unsupported options
        let name = Name {
            path: q.parent.get().name_path.clone(),
        };
        let columns = self.column_definitions(&q.column_definition_list);
        let primary_key = match &q.primary_key {
            Some(key) => key.column_offset_list.clone(),
            None => vec![],
        };
        let index_of = |expr: &AnyResolvedExprProto| -> i64 {
            let target = match expr.node.get() {
                ResolvedColumnRefNode(ResolvedColumnRefProto {
                    column:
                        Some(ResolvedColumnProto {
                            column_id: Some(id),
                            ..
                        }),
                    ..
                }) => id,
                other => panic!("{:?}", other),
            };
            for i in 0..q.column_definition_list.len() {
                if q.column_definition_list[i].column.get().column_id.get() == target {
                    return i as i64;
                }
            }
            panic!("{:?}", target)
        };
        let mut partition_by = vec![];
        for expr in &q.partition_by_list {
            partition_by.push(index_of(expr));
        }
        let mut cluster_by = vec![];
        for expr in &q.cluster_by_list {
            cluster_by.push(index_of(expr));
        }
        LogicalCreateTable {
            name,
            columns,
            partition_by,
            cluster_by,
            primary_key,
        }
    }

    fn column_definitions(
        &mut self,
        cs: &Vec<ResolvedColumnDefinitionProto>,
    ) -> Vec<(String, Type)> {
        let mut columns = Vec::with_capacity(cs.len());
        for c in cs {
            columns.push(self.column_definition(c))
        }
        columns
    }

    fn column_definition(&mut self, c: &ResolvedColumnDefinitionProto) -> (String, Type) {
        (c.name.get().clone(), Type::from(c.r#type.get()))
    }

    fn drop(&mut self, q: &ResolvedDropStmtProto) -> Plan {
        let object = ObjectType::from(q.object_type.get());
        let name = Name {
            path: q.name_path.clone(),
        };
        root(LogicalDrop { object, name })
    }

    fn insert(&mut self, q: &ResolvedInsertStmtProto) -> Plan {
        if q.insert_mode != Some(InsertMode::OrError as i32) {
            todo!("alternative insert modes")
        }
        if q.assert_rows_modified.is_some() {
            todo!("assert_rows_modified")
        }
        if q.insert_column_list.is_empty() {
            todo!("nested insert")
        }
        let input = match &q.query {
            Some(q) => self.any_resolved_scan(q),
            None => self.rows(q),
        };
        let table = Table::from(q.table_scan.get().table.get());
        let operator = Operator::LogicalInsert(table, self.columns(&q.insert_column_list));
        unary(operator, input)
    }

    fn rows(&mut self, q: &ResolvedInsertStmtProto) -> Plan {
        let mut input = root(LogicalSingleGet);
        let mut rows = Vec::with_capacity(q.row_list.len());
        for row in &q.row_list {
            rows.push(self.row(row, &mut input));
        }
        let operator = LogicalValues(rows, self.columns(&q.insert_column_list));
        unary(operator, input)
    }

    fn row(&mut self, row: &ResolvedInsertRowProto, input: &mut Plan) -> Vec<Scalar> {
        let mut values = Vec::with_capacity(row.value_list.len());
        for value in &row.value_list {
            values.push(self.expr(value.value.get(), input));
        }
        values
    }

    fn columns(&mut self, xs: &Vec<ResolvedColumnProto>) -> Vec<Column> {
        let mut cs = vec![];
        for x in xs {
            cs.push(Column::from(x));
        }
        cs
    }

    fn delete(&mut self, q: &ResolvedDeleteStmtProto) -> Plan {
        let mut input = self.table_scan(q.table_scan.get());
        let predicates = self.predicate(q.where_expr.get(), &mut input);
        let table = Table::from(q.table_scan.get().table.get());
        unary(
            LogicalDelete(table),
            unary(LogicalFilter(predicates), input),
        )
    }

    fn update(&mut self, q: &ResolvedUpdateStmtProto) -> Plan {
        if q.table_scan.is_none() {
            todo!("nested updates")
        }
        let mut input = self.table_scan(q.table_scan.get());
        if let Some(from) = &q.from_scan {
            let from = self.any_resolved_scan(from);
            input = binary(LogicalInnerJoin(vec![]), input, from);
        }
        if let Some(pred) = &q.where_expr {
            let pred = self.predicate(pred, &mut input);
            input = unary(LogicalFilter(pred), input);
        }
        let mut project = vec![];
        let mut update = vec![];
        for item in &q.update_item_list {
            let target = match item.target.get().node.get() {
                ResolvedColumnRefNode(ResolvedColumnRefProto {
                    column: Some(target),
                    ..
                }) => Column::from(target),
                other => panic!("{:?}", other),
            };
            let value = match item.set_value.get().value.get().node.get() {
                ResolvedColumnRefNode(ResolvedColumnRefProto {
                    column: Some(value),
                    ..
                }) => Some(Column::from(value)),
                ResolvedDmldefaultNode(_) => None,
                _ => {
                    let scalar = self.expr(item.set_value.get().value.get(), &mut input);
                    let fake = self.create_column(
                        "$update".to_string(),
                        "$expr".to_string(),
                        scalar.typ(),
                    );
                    project.push((scalar, fake.clone()));
                    Some(fake)
                }
            };
            update.push((target, value))
        }
        if project.is_empty() {
            return unary(LogicalUpdate(update), input);
        }
        unary(LogicalUpdate(update), unary(LogicalProject(project), input))
    }

    fn rename(&mut self, q: &ResolvedRenameStmtProto) -> Plan {
        let object = ObjectType::from(q.object_type.get());
        let from = Name {
            path: q.old_name_path.clone(),
        };
        let to = Name {
            path: q.new_name_path.clone(),
        };
        root(LogicalRename { object, from, to })
    }

    fn create_database(&mut self, q: &ResolvedCreateDatabaseStmtProto) -> Plan {
        // TODO fail on unspported options
        root(LogicalCreateDatabase(Name {
            path: q.name_path.clone(),
        }))
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
        let name = Name {
            path: q.parent.get().name_path.clone(),
        };
        let mut actions = vec![];
        for action in &q.parent.get().alter_action_list {
            actions.push(self.alter_action(action))
        }
        root(LogicalAlterTable { name, actions })
    }

    fn alter_action(&mut self, action: &AnyResolvedAlterActionProto) -> Alter {
        match action.node.get() {
            ResolvedSetOptionsActionNode(_) => panic!("{:?}", action),
            ResolvedAddColumnActionNode(add) => {
                let (name, typ) = self.column_definition(add.column_definition.get());
                Alter::AddColumn { name, typ }
            }
            ResolvedDropColumnActionNode(drop) => {
                let name = drop.column_reference.get().column.get().name.get().clone();
                Alter::DropColumn { name }
            }
            ResolvedGrantToActionNode(_) => panic!("{:?}", action),
            ResolvedFilterUsingActionNode(_) => panic!("{:?}", action),
            ResolvedRevokeFromActionNode(_) => panic!("{:?}", action),
            ResolvedRenameToActionNode(_) => panic!("{:?}", action),
        }
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
            ResolvedCastNode(x) => self.cast(x, outer),
            ResolvedSubqueryExprNode(x) => self.subquery_expr(x, outer),
            other => panic!("{:?}", other),
        }
    }

    fn column(&mut self, x: &ResolvedColumnProto) -> Scalar {
        Scalar::Column(Column::from(x))
    }

    fn function_call(&mut self, x: &AnyResolvedFunctionCallBaseProto, outer: &mut Plan) -> Scalar {
        let (function, arguments, returns) = match x {
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
                                signature:
                                    Some(FunctionSignatureProto {
                                        return_type:
                                            Some(FunctionArgumentTypeProto {
                                                r#type: Some(returns),
                                                ..
                                            }),
                                        ..
                                    }),
                                ..
                            }),
                        ..
                    })),
            } => (function, argument_list, returns),
            other => panic!("{:?}", other),
        };
        let function = Function::from(function.clone());
        let arguments = self.exprs(arguments, outer);
        let returns = encoding::Type::from(returns);
        Scalar::Call(function, arguments, returns)
    }

    fn cast(&mut self, x: &ResolvedCastProto, outer: &mut Plan) -> Scalar {
        let (expr, ty) = match x {
            ResolvedCastProto {
                parent:
                    Some(ResolvedExprProto {
                        r#type: Some(ty), ..
                    }),
                expr: Some(expr),
                return_null_on_error: Some(false),
            } => (expr, ty),
            other => panic!("{:?}", other),
        };
        Scalar::Cast(Box::new(self.expr(expr, outer)), Type::from(ty))
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
            2 => {
                let mark =
                    self.create_column("$mark".to_string(), "$exists".to_string(), Type::Bool);
                *outer = binary(
                    LogicalMarkJoin(vec![], mark.clone()),
                    corr,
                    mem::replace(outer, Root(Leaf)),
                );
                Scalar::Column(mark.clone())
            }
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
                let equals = Scalar::Call(Function::Equal, args, Type::Bool);
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
            table: Some(table),
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
    };
    &q.column_list[0]
}

fn single_column_aggregate(q: &AnyResolvedAggregateScanBaseProto) -> &ResolvedScanProto {
    match q.node.get() {
        ResolvedAggregateScanNode(q) => q.parent.get().parent.get(),
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
        NumericValue(buf) => {
            let mut x = 0i128;
            varint128::write(&mut x, buf);
            Value::Numeric(x)
        }
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

fn date_value(date: i32) -> chrono::NaiveDate {
    chrono::NaiveDate::from_ymd(1970, 1, 1) + time::Duration::days(date as i64)
}

fn timestamp_value(time: &prost_types::Timestamp) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::from_utc(
        chrono::NaiveDateTime::from_timestamp(time.seconds, time.nanos as u32),
        chrono::Utc,
    )
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
