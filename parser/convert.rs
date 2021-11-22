use std::{borrow::Borrow, collections::HashMap};

use ast::*;
use kernel::*;
use zetasql::{
    any_resolved_aggregate_scan_base_proto::Node::*, any_resolved_create_statement_proto::Node::*,
    any_resolved_create_table_stmt_base_proto::Node::*, any_resolved_expr_proto::Node::*,
    any_resolved_function_call_base_proto::Node::*,
    any_resolved_non_scalar_function_call_base_proto::Node::*, any_resolved_scan_proto::Node::*,
    any_resolved_statement_proto::Node::*, resolved_create_statement_enums::CreateScope,
    value_proto::Value::*, *,
};

#[log::trace]
pub fn convert<'a>(
    q: &AnyResolvedStatementProto,
    variables: &'a HashMap<String, Value>,
    catalog_id: i64,
) -> Expr {
    Converter {
        catalog_id,
        variables,
        known_columns: HashMap::new(),
    }
    .any_stmt(q)
}

struct Converter<'a> {
    catalog_id: i64,
    variables: &'a HashMap<String, Value>,
    known_columns: HashMap<i64, Column>,
}

impl<'a> Converter<'a> {
    fn any_stmt(&mut self, q: &AnyResolvedStatementProto) -> Expr {
        match q.node.get() {
            ResolvedQueryStmtNode(q) => self.query(q),
            ResolvedCreateStatementNode(q) => self.create(q),
            ResolvedDropStmtNode(q) => self.drop(q),
            ResolvedInsertStmtNode(q) => self.insert(q),
            ResolvedDeleteStmtNode(q) => self.delete(q),
            ResolvedUpdateStmtNode(q) => self.update(q),
            ResolvedCreateDatabaseStmtNode(q) => self.create_database(q),
            ResolvedCallStmtNode(q) => self.call(q),
            ResolvedExplainStmtNode(q) => self.explain(q),
            ResolvedAssertStmtNode(q) => self.assert(q),
            other => panic!("{:?}", other),
        }
    }

    fn query(&mut self, q: &ResolvedQueryStmtProto) -> Expr {
        let input = self.any_resolved_scan(q.query.get());
        LogicalOut {
            projects: q
                .output_column_list
                .iter()
                .map(|c| self.column(c.column.get()))
                .collect(),
            input: Box::new(input),
        }
    }

    fn any_resolved_scan(&mut self, q: &AnyResolvedScanProto) -> Expr {
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

    fn single_row(&mut self, _: &ResolvedSingleRowScanProto) -> Expr {
        LogicalSingleGet
    }

    fn table_scan(&mut self, q: &ResolvedTableScanProto) -> Expr {
        self.register_columns(q);

        let mut projects: Vec<Column> = q
            .parent
            .get()
            .column_list
            .iter()
            .map(|c| self.column(c))
            .collect();
        let table = Table::from(q);
        let xmin = Column::computed("$xmin", &None, DataType::I64);
        let xmax = Column::computed("$xmax", &None, DataType::I64);
        let predicates = vec![
            Scalar::Call(Box::new(F::LessOrEqual(
                Scalar::Column(xmin.clone()),
                Scalar::Call(Box::new(F::Xid)),
            ))),
            Scalar::Call(Box::new(F::Less(
                Scalar::Call(Box::new(F::Xid)),
                Scalar::Column(xmax.clone()),
            ))),
        ];
        projects.push(xmin);
        projects.push(xmax);
        LogicalGet {
            projects,
            predicates,
            table,
        }
    }

    fn table_scan_for_update(&mut self, q: &ResolvedTableScanProto) -> (Expr, Column) {
        self.register_columns(q);

        let mut projects: Vec<Column> = q
            .parent
            .get()
            .column_list
            .iter()
            .map(|c| self.column(c))
            .collect();
        let table = Table::from(q);
        let xmin = Column::computed("$xmin", &None, DataType::I64);
        let xmax = Column::computed("$xmax", &None, DataType::I64);
        let tid = Column::computed("$tid", &None, DataType::I64);
        let predicates = vec![
            Scalar::Call(Box::new(F::LessOrEqual(
                Scalar::Column(xmin.clone()),
                Scalar::Call(Box::new(F::Xid)),
            ))),
            Scalar::Call(Box::new(F::Less(
                Scalar::Call(Box::new(F::Xid)),
                Scalar::Column(xmax.clone()),
            ))),
        ];
        projects.push(xmin);
        projects.push(xmax);
        projects.push(tid.clone());
        let expr = LogicalGet {
            projects,
            predicates,
            table,
        };
        (expr, tid)
    }

    fn join(&mut self, q: &ResolvedJoinScanProto) -> Expr {
        let left = self.any_resolved_scan(q.left_scan.get());
        let right = self.any_resolved_scan(q.right_scan.get());
        // Convert inner join to join-then-filter.
        if *q.join_type.get().borrow() == 0 {
            let mut input = LogicalJoin {
                join: Join::Inner(vec![]),
                left: Box::new(left),
                right: Box::new(right),
            };
            let predicates = match &q.join_expr {
                Some(expr) => self.predicate(expr.borrow(), &mut input),
                None => vec![],
            };
            if predicates.is_empty() {
                return input;
            }
            return LogicalFilter {
                predicates,
                input: Box::new(input),
            };
        }
        // Convert outer join using join condition.
        let dummy = LogicalSingleGet;
        let mut input = dummy.clone();
        let predicates = match &q.join_expr {
            Some(expr) => self.predicate(expr.borrow(), &mut input),
            None => vec![],
        };
        if input != dummy {
            panic!("Nested expressions are not allowed on the ON expressions of outer joins")
        }
        match q.join_type.get().borrow() {
            // Left
            1 => LogicalJoin {
                join: Join::Right(predicates),
                left: Box::new(right),
                right: Box::new(left),
            },
            // Right
            2 => LogicalJoin {
                join: Join::Right(predicates),
                left: Box::new(left),
                right: Box::new(right),
            },
            // Full
            3 => LogicalJoin {
                join: Join::Outer(predicates),
                left: Box::new(left),
                right: Box::new(right),
            },
            // Invalid
            other => panic!("{:?}", other),
        }
    }

    fn filter(&mut self, q: &ResolvedFilterScanProto) -> Expr {
        let mut input = self.any_resolved_scan(q.input_scan.get());
        let predicates = self.predicate(q.filter_expr.get(), &mut input);
        LogicalFilter {
            predicates,
            input: Box::new(input),
        }
    }

    fn predicate(&mut self, x: &AnyResolvedExprProto, outer: &mut Expr) -> Vec<Scalar> {
        match self.predicate_and(x, outer) {
            Some(ps) => ps,
            None => vec![self.expr(x, outer)],
        }
    }

    fn predicate_and(&mut self, x: &AnyResolvedExprProto, outer: &mut Expr) -> Option<Vec<Scalar>> {
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

    fn set_operation(&mut self, q: &ResolvedSetOperationScanProto) -> Expr {
        match q.op_type.unwrap() {
            // UnionAll
            0 => self.union(&q.parent.get().column_list, &q.input_item_list),
            // UnionDistinct
            1 => panic!("UNION DISTINCT is not supported"),
            // IntersectAll
            2 => panic!("UNION DISTINCT is not supported"),
            // IntersectDistinct
            3 => panic!("INTERSECT DISTINCT is not supported"),
            // ExceptAll
            4 => panic!("EXCEPT ALL is not supported"),
            // ExceptDistinct
            5 => panic!("EXCEPT DISTINCT is not supported"),
            // Other
            other => panic!("{:?}", other),
        }
    }

    fn union(
        &mut self,
        outputs: &Vec<ResolvedColumnProto>,
        items: &[ResolvedSetOperationItemProto],
    ) -> Expr {
        match items.len() {
            0 => panic!(),
            1 => {
                let input = self.any_resolved_scan(items[0].scan.get());
                let columns = self.columns(&items[0].output_column_list);
                let outputs = self.columns(outputs);
                self.rename_columns(outputs, columns, input)
            }
            _ => {
                let left = self.union(outputs, &items[0..1]);
                let right = self.union(outputs, &items[1..]);
                LogicalUnion {
                    left: Box::new(left),
                    right: Box::new(right),
                }
            }
        }
    }

    fn rename_columns(&mut self, outputs: Vec<Column>, inputs: Vec<Column>, input: Expr) -> Expr {
        let mut projects = vec![];
        for i in 0..outputs.len() {
            let input = Scalar::Column(inputs[i].clone());
            let output = outputs[i].clone();
            projects.push((input, output))
        }
        assert!(!projects.is_empty());
        LogicalMap {
            include_existing: false,
            projects,
            input: Box::new(input),
        }
    }

    fn order_by(&mut self, q: &ResolvedOrderByScanProto) -> Expr {
        let input = self.any_resolved_scan(q.input_scan.get().borrow());
        let mut order_by = vec![];
        for x in &q.order_by_item_list {
            let descending = x.is_descending.unwrap_or(false);
            // let nulls_first = match x.null_order.unwrap_or(0) {
            //     0 => !descending, // NullOrderMode::OrderUnspecified
            //     1 => true,        // NullOrderMode::NullsFirst
            //     2 => false,       // NullOrderMode::NullsLast
            //     _ => panic!(),
            // };
            order_by.push(OrderBy {
                column: self.column_ref(x.column_ref.get()),
                descending,
            });
        }
        LogicalSort {
            order_by,
            input: Box::new(input),
        }
    }

    fn limit_offset(&mut self, q: &ResolvedLimitOffsetScanProto) -> Expr {
        let input = self.any_resolved_scan(q.input_scan.get().borrow());
        let limit = self.int_literal(q.limit.get().borrow()) as usize;
        let offset = match &q.offset {
            Some(offset) => self.int_literal(offset) as usize,
            None => 0,
        };
        LogicalLimit {
            limit,
            offset,
            input: Box::new(input),
        }
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

    fn project(&mut self, q: &ResolvedProjectScanProto) -> Expr {
        let mut input = self.any_resolved_scan(q.input_scan.get());
        let mut projects = vec![];
        for x in &q.expr_list {
            projects.push(self.computed_column(x, &mut input));
        }
        for c in &q.parent.get().column_list {
            if q.expr_list
                .iter()
                .any(|x| x.column.get().column_id.unwrap() == c.column_id.unwrap())
            {
                continue;
            }
            let column = self.column(&c);
            projects.push((Scalar::Column(column.clone()), column))
        }
        assert!(!projects.is_empty());
        LogicalMap {
            include_existing: false,
            projects,
            input: Box::new(input),
        }
    }

    fn computed_column(
        &mut self,
        x: &ResolvedComputedColumnProto,
        input: &mut Expr,
    ) -> (Scalar, Column) {
        let value = self.expr(x.expr.get(), input);
        let column = self.column(x.column.get());
        (value, column)
    }

    fn with(&mut self, q: &ResolvedWithScanProto) -> Expr {
        let mut right = self.any_resolved_scan(q.query.get().borrow());
        for i in (0..q.with_entry_list.len()).rev() {
            match &q.with_entry_list[i] {
                ResolvedWithEntryProto {
                    with_query_name: Some(name),
                    with_subquery: Some(query),
                    ..
                } => {
                    let left = self.any_resolved_scan(&query);
                    let columns = self.columns(column_list(&query));
                    right = LogicalWith {
                        name: name.clone(),
                        columns,
                        left: Box::new(left),
                        right: Box::new(right),
                    };
                }
                other => panic!("{:?}", other),
            }
        }
        right
    }

    fn with_ref(&mut self, q: &ResolvedWithRefScanProto) -> Expr {
        let name = q.with_query_name.get().clone();
        let columns = self.columns(&q.parent.get().column_list);
        LogicalGetWith { name, columns }
    }

    fn aggregate(&mut self, q: &ResolvedAggregateScanProto) -> Expr {
        let q = q.parent.get();
        let mut input = self.any_resolved_scan(q.input_scan.get());
        // Project each of the group-by columns under its own name.
        let mut input_projects: Vec<(Scalar, Column)> = vec![];
        let mut group_by_columns: Vec<Column> = vec![];
        for compute in &q.group_by_list {
            let scalar = self.expr(compute.expr.get(), &mut input);
            let column = self.column(compute.column.get());
            input_projects.push((scalar, column.clone()));
            group_by_columns.push(column);
        }
        // Organize each of the aggregation operations into stages.
        let mut aggregate_operators: Vec<AggregateExpr> = vec![];
        let mut output_projects: Vec<(Scalar, Column)> = vec![];
        for aggregate in &q.aggregate_list {
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
            if ignore_nulls {
                panic!("IGNORE NULLS is not supported");
            }
            let function = function.parent.get();
            let arguments = &function.argument_list;
            let function = function.function.get().name.get().clone();
            if &function == "ZetaSQL:avg" {
                assert!(arguments.len() == 1);

                let input_expr = self.expr(&arguments[0], &mut input);
                let input_column = Column::computed("$avg", &None, input_expr.data_type());
                input_projects.push((input_expr.clone(), input_column.clone()));
                let sum_column = Column::computed("$avg$sum", &None, input_expr.data_type());
                let count_column = Column::computed("$avg$count", &None, input_expr.data_type());
                aggregate_operators.push(AggregateExpr {
                    function: AggregateFunction::Sum,
                    distinct,
                    input: input_column.clone(),
                    output: sum_column.clone(),
                });
                aggregate_operators.push(AggregateExpr {
                    function: AggregateFunction::Count,
                    distinct,
                    input: input_column.clone(),
                    output: count_column.clone(),
                });
                let avg_expr = Scalar::Call(Box::new(F::DivideDouble(
                    Scalar::Cast(Box::new(Scalar::Column(sum_column)), DataType::F64),
                    Scalar::Cast(Box::new(Scalar::Column(count_column)), DataType::F64),
                )));
                let avg_column = self.column(aggregate.column.get());
                output_projects.push((avg_expr, avg_column));
            } else if &function == "ZetaSQL:$count_star" {
                assert!(arguments.len() == 0);

                let input_expr = Scalar::Literal(Value::I64(Some(1)));
                let input_column = Column::computed("$star", &None, DataType::I64);
                input_projects.push((input_expr, input_column.clone()));
                let count_column = self.column(aggregate.column.get());
                aggregate_operators.push(AggregateExpr {
                    function: AggregateFunction::Count,
                    distinct,
                    input: input_column,
                    output: count_column,
                });
            } else {
                assert!(arguments.len() == 1);

                let input_expr = self.expr(&arguments[0], &mut input);
                let input_column =
                    Column::computed(&function_name(&function), &None, input_expr.data_type());
                input_projects.push((input_expr, input_column.clone()));
                let aggregate_column = self.column(aggregate.column.get());
                aggregate_operators.push(AggregateExpr {
                    function: AggregateFunction::from(&function),
                    distinct,
                    input: input_column,
                    output: aggregate_column,
                });
            }
        }
        // Form the result, using as many stages as are necessary.
        let mut result = input;
        if input_projects.len() > 0 {
            assert!(!input_projects.is_empty());
            result = LogicalMap {
                include_existing: false,
                projects: input_projects,
                input: Box::new(result),
            };
        }
        result = LogicalAggregate {
            group_by: group_by_columns,
            aggregate: aggregate_operators,
            input: Box::new(result),
        };
        if output_projects.len() > 0 {
            assert!(!output_projects.is_empty());
            result = LogicalMap {
                include_existing: true,
                projects: output_projects,
                input: Box::new(result),
            };
        }
        result
    }

    fn create(&mut self, q: &AnyResolvedCreateStatementProto) -> Expr {
        match q.node.get() {
            ResolvedCreateIndexStmtNode(q) => self.create_index(q),
            ResolvedCreateTableStmtBaseNode(AnyResolvedCreateTableStmtBaseProto {
                node: Some(ResolvedCreateTableStmtNode(q)),
            }) => self.create_table(q),
            other => panic!("{:?}", other),
        }
    }

    fn create_index(&mut self, q: &ResolvedCreateIndexStmtProto) -> Expr {
        for option in &q.option_list {
            panic!("CREATE INDEX does not support option {}", option.name());
        }
        let name = Name {
            catalog_id: self.catalog_id,
            path: q.parent.get().name_path.clone(),
        };
        let table = Table::from(q.table_scan.get());
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
        LogicalCreateIndex {
            name,
            table,
            columns,
            reserved_id: sequences::next_index_id(),
        }
    }

    fn create_table(&mut self, q: &ResolvedCreateTableStmtProto) -> Expr {
        self.create_table_base(q.parent.get())
    }

    fn create_table_base(&mut self, q: &ResolvedCreateTableStmtBaseProto) -> Expr {
        if q.parent.get().create_scope == Some(CreateScope::CreateTemp as i32) {
            panic!("CREATE TEMP TABLE is not supported");
        }
        for option in &q.option_list {
            panic!("CREATE TABLE does not support option {}", option.name());
        }
        let name = Name {
            catalog_id: self.catalog_id,
            path: q.parent.get().name_path.clone(),
        };
        let columns = self.column_definitions(&q.column_definition_list);
        let reserved_id = sequences::next_table_id();
        LogicalCreateTable {
            name,
            columns,
            reserved_id,
        }
    }

    fn column_definitions(
        &mut self,
        cs: &Vec<ResolvedColumnDefinitionProto>,
    ) -> Vec<(String, DataType)> {
        let mut columns = Vec::with_capacity(cs.len());
        for c in cs {
            columns.push(self.column_definition(c))
        }
        columns
    }

    fn column_definition(&mut self, c: &ResolvedColumnDefinitionProto) -> (String, DataType) {
        (c.name.get().clone(), DataType::from(c.r#type.get()))
    }

    fn drop(&mut self, q: &ResolvedDropStmtProto) -> Expr {
        let object = ObjectType::from(q.object_type.get());
        let name = Name {
            catalog_id: self.catalog_id,
            path: q.name_path.clone(),
        };
        LogicalDrop { object, name }
    }

    fn insert(&mut self, q: &ResolvedInsertStmtProto) -> Expr {
        let table = self.table_scan_for_insert(q.table_scan.get());

        if let Some(scan) = &q.query {
            LogicalInsert {
                table,
                input: Box::new(self.any_resolved_scan(scan)),
                columns: (0..q.query_output_column_list.len())
                    .map(|i| {
                        (
                            self.column(&q.query_output_column_list[i]),
                            q.insert_column_list[i].name.get().clone(),
                        )
                    })
                    .collect(),
            }
        } else {
            LogicalInsert {
                table,
                input: Box::new(self.rows(q)),
                columns: q
                    .insert_column_list
                    .iter()
                    .map(|c| (self.column(c), c.name.get().clone()))
                    .collect(),
            }
        }
    }

    fn table_scan_for_insert(&mut self, q: &ResolvedTableScanProto) -> Table {
        self.register_columns(q);

        Table::from(q)
    }

    fn rows(&mut self, q: &ResolvedInsertStmtProto) -> Expr {
        let num_columns = q.insert_column_list.len();
        let mut input = LogicalSingleGet;
        let mut values: Vec<Vec<Scalar>> = Vec::with_capacity(num_columns);
        values.resize_with(num_columns, Vec::new);
        for i in 0..q.row_list.len() {
            for j in 0..num_columns {
                values[j].push(self.expr(
                    q.row_list[i].value_list[j].value.as_ref().unwrap(),
                    &mut input,
                ));
            }
        }
        LogicalValues {
            columns: self.columns(&q.insert_column_list),
            values,
            input: Box::new(input),
        }
    }

    fn columns(&mut self, xs: &Vec<ResolvedColumnProto>) -> Vec<Column> {
        let mut cs = vec![];
        for x in xs {
            cs.push(self.column(x));
        }
        cs
    }

    fn column_ref(&mut self, x: &ResolvedColumnRefProto) -> Column {
        self.column(x.column.get())
    }

    fn delete(&mut self, q: &ResolvedDeleteStmtProto) -> Expr {
        let (mut input, tid) = self.table_scan_for_update(q.table_scan.get());
        let predicates = self.predicate(q.where_expr.get(), &mut input);
        LogicalDelete {
            table: Table::from(q.table_scan.get()),
            tid,
            input: Box::new(LogicalFilter {
                predicates,
                input: Box::new(input),
            }),
        }
    }

    fn update(&mut self, q: &ResolvedUpdateStmtProto) -> Expr {
        let table = Table::from(q.table_scan.get());
        let (mut input, tid) = self.table_scan_for_update(q.table_scan.get());
        if let Some(from) = &q.from_scan {
            let from = self.any_resolved_scan(from);
            let predicates = vec![];
            input = LogicalJoin {
                join: Join::Inner(predicates),
                left: Box::new(input),
                right: Box::new(from),
            };
        }
        if let Some(pred) = &q.where_expr {
            let predicates = self.predicate(pred, &mut input);
            input = LogicalFilter {
                predicates,
                input: Box::new(input),
            };
        }
        let column_list = &q.table_scan.get().parent.get().column_list;
        let mut projects = vec![];
        for column in column_list {
            let as_column = self.column(column);
            let value = self
                .updated_column(q, column, &mut input)
                .unwrap_or(Scalar::Column(as_column.clone()));
            projects.push((value, as_column))
        }
        projects.push((Scalar::Column(tid.clone()), tid.clone()));
        LogicalUpdate {
            table,
            tid,
            input: Box::new(LogicalMap {
                include_existing: false,
                projects,
                input: Box::new(input),
            }),
            columns: column_list
                .iter()
                .map(|c| (self.column(c), c.name.get().clone()))
                .collect(),
        }
    }

    fn updated_column(
        &mut self,
        q: &ResolvedUpdateStmtProto,
        column: &ResolvedColumnProto,
        outer: &mut Expr,
    ) -> Option<Scalar> {
        for item in &q.update_item_list {
            if let ResolvedColumnRefNode(target) = item.target.get().node.get() {
                if target.column.get().name == column.name {
                    let value = match item.set_value.get().value.get().node.get() {
                        ResolvedDmldefaultNode(_) => {
                            panic!("DEFAULT is not supported");
                        }
                        other => self.expr_node(other, outer),
                    };
                    return Some(value);
                }
            }
        }
        None
    }

    fn create_database(&mut self, q: &ResolvedCreateDatabaseStmtProto) -> Expr {
        for option in &q.option_list {
            panic!("CREATE DATABASE does not support option {}", option.name());
        }
        LogicalCreateDatabase {
            name: Name {
                catalog_id: self.catalog_id,
                path: q.name_path.clone(),
            },
            reserved_id: sequences::next_catalog_id(),
        }
    }

    fn call(&mut self, q: &ResolvedCallStmtProto) -> Expr {
        let mut input = LogicalSingleGet;
        let procedure = match q.procedure.get().name.get().as_str() {
            "create_table" => Procedure::CreateTable(self.expr(&q.argument_list[0], &mut input)),
            "drop_table" => Procedure::DropTable(self.expr(&q.argument_list[0], &mut input)),
            "create_index" => Procedure::CreateIndex(self.expr(&q.argument_list[0], &mut input)),
            "drop_index" => Procedure::DropIndex(self.expr(&q.argument_list[0], &mut input)),
            other => panic!("{}", other),
        };
        LogicalCall {
            procedure,
            input: Box::new(input),
        }
    }

    fn explain(&mut self, q: &ResolvedExplainStmtProto) -> Expr {
        LogicalExplain {
            input: Box::new(self.any_stmt(q.statement.get())),
        }
    }

    fn assert(&mut self, q: &ResolvedAssertStmtProto) -> Expr {
        let mut input = Expr::LogicalSingleGet;
        LogicalCall {
            procedure: Procedure::Assert(
                self.expr(q.expression.get(), &mut input),
                q.description
                    .clone()
                    .unwrap_or("Assert failed.".to_string()),
            ),
            input: Box::new(input),
        }
    }

    fn exprs(&mut self, xs: &Vec<AnyResolvedExprProto>, outer: &mut Expr) -> Vec<Scalar> {
        let mut list = vec![];
        for x in xs {
            list.push(self.expr(x, outer));
        }
        list
    }

    fn expr(&mut self, x: &AnyResolvedExprProto, outer: &mut Expr) -> Scalar {
        self.expr_node(x.node.get(), outer)
    }

    fn expr_node(&mut self, x: &any_resolved_expr_proto::Node, outer: &mut Expr) -> Scalar {
        match x {
            ResolvedLiteralNode(x) => {
                let value = x.value.get().value.get();
                let data_type = x.value.get().r#type.get();
                Scalar::Literal(literal(value, data_type))
            }
            ResolvedColumnRefNode(x) => Scalar::Column(self.column_ref(x)),
            ResolvedFunctionCallBaseNode(x) => self.function_call(x, outer),
            ResolvedCastNode(x) => self.cast(x, outer),
            ResolvedParameterNode(x) => self.parameter(x),
            ResolvedSubqueryExprNode(x) => self.subquery_expr(x, outer),
            other => panic!("{:?}", other),
        }
    }

    fn function_call(&mut self, x: &AnyResolvedFunctionCallBaseProto, outer: &mut Expr) -> Scalar {
        match x {
            AnyResolvedFunctionCallBaseProto {
                node:
                    Some(ResolvedFunctionCallNode(ResolvedFunctionCallProto {
                        parent:
                            Some(ResolvedFunctionCallBaseProto {
                                function: Some(function),
                                argument_list,
                                signature: Some(signature),
                                ..
                            }),
                        ..
                    })),
            } => {
                let arguments = self.exprs(argument_list, outer);
                Scalar::Call(Box::new(F::from(function, signature, arguments)))
            }
            other => panic!("{:?}", other),
        }
    }

    fn cast(&mut self, x: &ResolvedCastProto, outer: &mut Expr) -> Scalar {
        let (expr, ty) = match x {
            ResolvedCastProto {
                parent:
                    Some(ResolvedExprProto {
                        r#type: Some(ty), ..
                    }),
                expr: Some(expr),
                ..
            } => (expr, ty),
            other => panic!("{:?}", other),
        };
        Scalar::Cast(Box::new(self.expr(expr, outer)), DataType::from(ty))
    }

    fn parameter(&mut self, x: &ResolvedParameterProto) -> Scalar {
        Scalar::Literal(self.variables[x.name.get()].clone())
    }

    fn subquery_expr(&mut self, x: &ResolvedSubqueryExprProto, outer: &mut Expr) -> Scalar {
        let parameters: Vec<Column> = x
            .parameter_list
            .iter()
            .map(|c| self.column(c.column.get()))
            .collect();
        let subquery = self.any_resolved_scan(x.subquery.get());
        let (join, scalar) = match x.subquery_type.get() {
            // Scalar
            0 => {
                let join = Join::Single(vec![]);
                let scalar = self.single_column(x.subquery.get());
                (join, scalar)
            }
            // AnyArray
            1 => unimplemented!(),
            // Exists
            2 => {
                let mark = Column::computed("$exists", &None, DataType::Bool);
                let join = Join::Mark(mark.clone(), vec![]);
                let scalar = Scalar::Column(mark);
                (join, scalar)
            }
            // In
            3 => {
                let mark = Column::computed("$in", &None, DataType::Bool);
                let find = match x {
                    ResolvedSubqueryExprProto {
                        in_expr: Some(x), ..
                    } => self.expr(x, outer),
                    other => panic!("{:?}", other),
                };
                let check = self.single_column(x.subquery.get());
                let join_filter = vec![Scalar::Call(Box::new(F::Equal(find, check)))];
                let join = Join::Mark(mark.clone(), join_filter);
                let scalar = Scalar::Column(mark);
                (join, scalar)
            }
            other => panic!("{:?}", other),
        };
        // Push join onto outer.
        *outer = self.create_dependent_join(parameters, join, subquery, outer.clone());
        // Return scalar that represents the entire query.
        scalar
    }

    fn create_dependent_join(
        &mut self,
        subquery_parameters: Vec<Column>,
        join: Join,
        mut subquery: Expr,
        outer: Expr,
    ) -> Expr {
        if subquery_parameters.is_empty() {
            return LogicalJoin {
                join,
                left: Box::new(subquery),
                right: Box::new(outer),
            };
        }
        // A correlated subquery can be interpreted as a dependent join
        // that executes the subquery once for every tuple in outer:
        //
        //        DependentJoin
        //         +         +
        //         +         +
        //    subquery      outer
        //
        // As a first step in eliminating the dependent join, we rewrite it as dependent join
        // that executes the subquery once for every *distinct combination of parameters* in outer,
        // and then an equi-join that looks up the appropriate row for every tuple in outer:
        //
        //             LogicalJoin
        //              +       +
        //              +       +
        //   DependentJoin     outer
        //    +         +
        //    +         +
        // subquery  Project
        //              +
        //              +
        //            outer
        //
        // This is a slight improvement because the number of distinct combinations of parameters in outer
        // is likely much less than the number of tuples in outer,
        // and it makes the dependent join much easier to manipulate because Project is a set rather than a multiset.
        // During the rewrite phase, we will take advantage of this to push the dependent join down
        // until it can be eliminated or converted to an inner join.
        //
        //   DependentJoin
        //    +         +
        //    +         +
        // subquery  Project
        //              +
        //              +
        //            outer
        //
        let rename_subquery_parameters: Vec<Column> = subquery_parameters
            .iter()
            .map(|p| Column::computed(&p.name, &None, p.data_type))
            .collect();
        let map_subquery_parameters: HashMap<Column, Column> = (0..subquery_parameters.len())
            .map(|i| {
                (
                    subquery_parameters[i].clone(),
                    rename_subquery_parameters[i].clone(),
                )
            })
            .collect();
        let mut domain = outer.clone();
        subquery.subst(&map_subquery_parameters);
        domain.subst(&map_subquery_parameters);
        let dependent_join = LogicalDependentJoin {
            parameters: rename_subquery_parameters.clone(),
            predicates: vec![],
            subquery: Box::new(subquery),
            domain: Box::new(domain),
        };
        //             LogicalJoin
        //              +       +
        //              +       +
        //   DependentJoin     outer
        //    +         +
        let mut join_predicates: Vec<Scalar> = (0..subquery_parameters.len())
            .map(|i| {
                Scalar::Call(Box::new(F::Is(
                    Scalar::Column(subquery_parameters[i].clone()),
                    Scalar::Column(rename_subquery_parameters[i].clone()),
                )))
            })
            .collect();
        let join = match join {
            Join::Single(additional_predicates) => {
                for p in additional_predicates {
                    join_predicates.push(p.clone());
                }
                Join::Single(join_predicates)
            }
            Join::Mark(mark, additional_predicates) => {
                for p in additional_predicates {
                    join_predicates.push(p.clone());
                }
                Join::Mark(mark.clone(), join_predicates)
            }
            _ => panic!("{}", join),
        };
        LogicalJoin {
            join,
            left: Box::new(dependent_join),
            right: Box::new(outer),
        }
    }

    fn single_column(&mut self, q: &AnyResolvedScanProto) -> Scalar {
        Scalar::Column(self.column(&column_list(q)[0]))
    }

    /// Register a column as belonging to a table.
    fn register_columns(&mut self, q: &ResolvedTableScanProto) {
        for c in &q.parent.get().column_list {
            if !self.known_columns.contains_key(&c.column_id.unwrap()) {
                self.known_columns.insert(
                    c.column_id.unwrap(),
                    Column::table(
                        c.name.get(),
                        q.table.get().serialization_id.unwrap(),
                        q.table.get().name.get(),
                        DataType::from(c.r#type.as_ref().unwrap()),
                    ),
                );
            }
        }
    }

    fn column(&mut self, c: &ResolvedColumnProto) -> Column {
        if !self.known_columns.contains_key(&c.column_id.unwrap()) {
            self.known_columns.insert(
                c.column_id.unwrap(),
                Column::computed(
                    c.name.get(),
                    &c.table_name,
                    DataType::from(c.r#type.as_ref().unwrap()),
                ),
            );
        }
        self.known_columns[&c.column_id.unwrap()].clone()
    }
}

fn column_list(q: &AnyResolvedScanProto) -> &Vec<ResolvedColumnProto> {
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
        ResolvedRecursiveRefScanNode(_) | ResolvedRecursiveScanNode(_) => {
            panic!("recursive queries not supported")
        }
        ResolvedPivotScanNode(_) | ResolvedUnpivotScanNode(_) => panic!("PIVOT not supported"),
        ResolvedGroupRowsScanNode(_) => panic!("GROUP_ROWS not supported"),
    };
    &q.column_list
}

fn single_column_aggregate(q: &AnyResolvedAggregateScanBaseProto) -> &ResolvedScanProto {
    match q.node.get() {
        ResolvedAggregateScanNode(q) => q.parent.get().parent.get(),
        ResolvedAnonymizedAggregateScanNode(_) => panic!("differential privacy not supported"),
    }
}

fn function_name(name: &String) -> String {
    format!("${}", name.trim_start_matches("ZetaSQL:"))
}

fn literal(value: &ValueProto, data_type: &TypeProto) -> Value {
    let value = match value {
        ValueProto { value: Some(value) } => value,
        _ => return Value::null(DataType::from(data_type)),
    };
    match value {
        Int64Value(x) => Value::I64(Some(*x)),
        BoolValue(x) => Value::Bool(Some(*x)),
        DoubleValue(x) => Value::F64(Some(*x)),
        DateValue(x) => Value::Date(Some(*x)),
        TimestampValue(x) => Value::Timestamp(Some(microseconds_since_epoch(x))),
        StringValue(x) => Value::String(Some(x.clone())),
        EnumValue(i) => Value::EnumValue(*i),
        other => panic!("{:?}", other),
    }
}

fn microseconds_since_epoch(time: &prost_types::Timestamp) -> i64 {
    (time.seconds * 1_000_000) + (time.nanos / 1000) as i64
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
