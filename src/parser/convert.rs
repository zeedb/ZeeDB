use arrow::datatypes::*;
use ast::*;
use catalog::Index;
use encoding::varint128;
use std::borrow::Borrow;
use std::collections::HashMap;
use zetasql::any_resolved_aggregate_scan_base_proto::Node::*;
use zetasql::any_resolved_create_statement_proto::Node::*;
use zetasql::any_resolved_create_table_stmt_base_proto::Node::*;
use zetasql::any_resolved_expr_proto::Node::*;
use zetasql::any_resolved_function_call_base_proto::Node::*;
use zetasql::any_resolved_non_scalar_function_call_base_proto::Node::*;
use zetasql::any_resolved_scan_proto::Node::*;
use zetasql::any_resolved_statement_proto::Node::*;
use zetasql::resolved_insert_stmt_enums::*;
use zetasql::value_proto::Value::*;
use zetasql::*;

pub fn convert(catalog_id: i64, q: &AnyResolvedStatementProto) -> Expr {
    Converter {
        catalog_id,
        next_column_id: 0,
    }
    .any_stmt(q)
}

struct Converter {
    catalog_id: i64,
    next_column_id: i64,
}

impl Converter {
    fn any_stmt(&mut self, q: &AnyResolvedStatementProto) -> Expr {
        match q.node.get() {
            ResolvedQueryStmtNode(q) => self.query(q),
            ResolvedCreateStatementNode(q) => self.create(q),
            ResolvedDropStmtNode(q) => self.drop(q),
            ResolvedInsertStmtNode(q) => self.insert(q),
            ResolvedDeleteStmtNode(q) => self.delete(q),
            ResolvedUpdateStmtNode(q) => self.update(q),
            ResolvedCreateDatabaseStmtNode(q) => self.create_database(q),
            ResolvedSingleAssignmentStmtNode(q) => self.assign(q),
            ResolvedCallStmtNode(q) => self.call(q),
            other => panic!("{:?}", other),
        }
    }

    fn query(&mut self, q: &ResolvedQueryStmtProto) -> Expr {
        LogicalOut {
            projects: q
                .output_column_list
                .iter()
                .map(|c| Column::from(c.column.get()))
                .collect(),
            input: Box::new(self.any_resolved_scan(q.query.get())),
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
            },
            other => panic!("{:?}", other),
        }
    }

    fn single_row(&mut self, _: &ResolvedSingleRowScanProto) -> Expr {
        LogicalSingleGet
    }

    fn table_scan(&mut self, q: &ResolvedTableScanProto) -> Expr {
        let mut projects: Vec<Column> = q
            .parent
            .get()
            .column_list
            .iter()
            .map(|c| Column::from(c))
            .collect();
        let table = Table::from(q);
        let xmin = self.create_column("$xmin".to_string(), DataType::Int64, Phase::Convert);
        let xmax = self.create_column("$xmax".to_string(), DataType::Int64, Phase::Convert);
        let predicates = vec![
            Scalar::Call(Box::new(Function::LessOrEqual(
                Scalar::Column(xmin.clone()),
                Scalar::Call(Box::new(Function::Xid)),
            ))),
            Scalar::Call(Box::new(Function::Less(
                Scalar::Call(Box::new(Function::Xid)),
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
        let mut projects: Vec<Column> = q
            .parent
            .get()
            .column_list
            .iter()
            .map(|c| Column::from(c))
            .collect();
        let table = Table::from(q);
        let xmin = self.create_column("$xmin".to_string(), DataType::Int64, Phase::Convert);
        let xmax = self.create_column("$xmax".to_string(), DataType::Int64, Phase::Convert);
        let tid = self.create_column("$tid".to_string(), DataType::Int64, Phase::Convert);
        let predicates = vec![
            Scalar::Call(Box::new(Function::LessOrEqual(
                Scalar::Column(xmin.clone()),
                Scalar::Call(Box::new(Function::Xid)),
            ))),
            Scalar::Call(Box::new(Function::Less(
                Scalar::Call(Box::new(Function::Xid)),
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
        // TODO if we introduce the concept of a comparison join, some nested expressions can be handled.
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
            1 => panic!("UNION DISTINCT is not supported"), // TODO
            // IntersectAll
            2 => panic!("UNION DISTINCT is not supported"), // TODO
            // IntersectDistinct
            3 => panic!("INTERSECT DISTINCT is not supported"), // TODO
            // ExceptAll
            4 => panic!("EXCEPT ALL is not supported"), // TODO
            // ExceptDistinct
            5 => panic!("EXCEPT DISTINCT is not supported"), // TODO
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
            let nulls_first = match x.null_order.unwrap_or(0) {
                0 => !descending, // NullOrderMode::OrderUnspecified
                1 => true,        // NullOrderMode::NullsFirst
                2 => false,       // NullOrderMode::NullsLast
                _ => panic!(),
            };
            order_by.push(OrderBy {
                column: self.column_ref(x.column_ref.get()),
                descending,
                nulls_first,
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
            let column = Column::from(&c);
            projects.push((Scalar::Column(column.clone()), column))
        }
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
        let column = Column::from(x.column.get());
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
        let mut projects = vec![];
        let group_by = q
            .group_by_list
            .iter()
            .map(|c| self.compute(c, &mut projects, &mut input))
            .collect();
        let aggregate = q
            .aggregate_list
            .iter()
            .map(|c| {
                let (aggregate, parameter) = self.reduce(c, &mut projects, &mut input);
                let result = Column::from(&c.column.get());
                (aggregate, parameter, result)
            })
            .collect();
        if projects.len() > 0 {
            input = LogicalMap {
                include_existing: false,
                projects,
                input: Box::new(input),
            };
        }
        LogicalAggregate {
            group_by,
            aggregate,
            input: Box::new(input),
        }
    }

    fn compute(
        &mut self,
        compute: &ResolvedComputedColumnProto,
        project: &mut Vec<(Scalar, Column)>,
        input: &mut Expr,
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
        input: &mut Expr,
    ) -> (AggregateFn, Column) {
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
        let distinct = function.distinct.unwrap_or(false); // TODO
        let ignore_nulls = function.null_handling_modifier.unwrap_or(0) == 1; // TODO
        let function = function.parent.get();
        let arguments = &function.argument_list;
        let function = function.function.get().name.get().clone();
        let argument = if arguments.len() == 0 {
            let argument = Scalar::Literal(Value::Int64(1));
            let column = self.create_column("$star".to_string(), DataType::Int64, Phase::Convert);
            project.push((argument, column.clone()));
            column.clone()
        } else if arguments.len() == 1 {
            let argument = self.expr(arguments.first().get(), input);
            let column = self.create_column(
                function_name(&function),
                argument.data_type(),
                Phase::Convert,
            );
            project.push((argument, column.clone()));
            column.clone()
        } else {
            panic!("expected 1 or 0 arguments but found {:?}", arguments.len());
        };
        if &function == "ZetaSQL:avg" {
            todo!()
        }
        (AggregateFn::from(function), argument)
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
        let name = Name {
            catalog_id: self.catalog_id,
            path: q.parent.get().name_path.clone(),
        };
        let table = Name {
            catalog_id: self.catalog_id,
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
        LogicalCreateIndex {
            name,
            table,
            columns,
        }
    }

    fn create_table(&mut self, q: &ResolvedCreateTableStmtProto) -> Expr {
        self.create_table_base(q.parent.get())
    }

    fn create_table_base(&mut self, q: &ResolvedCreateTableStmtBaseProto) -> Expr {
        let name = Name {
            catalog_id: self.catalog_id,
            path: q.parent.get().name_path.clone(),
        };
        let columns = self.column_definitions(&q.column_definition_list);
        LogicalCreateTable { name, columns }
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
        (c.name.get().clone(), data_type::from_proto(c.r#type.get()))
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
            Some(scan) => self.insert_scan(q, scan),
            None => self.rows(q),
        };
        LogicalInsert {
            table: Table::from(q.table_scan.get()),
            input: Box::new(input),
        }
    }

    fn insert_scan(&mut self, q: &ResolvedInsertStmtProto, scan: &AnyResolvedScanProto) -> Expr {
        let input = self.any_resolved_scan(scan);
        let projects = (0..q.query_output_column_list.len())
            .map(|i| {
                (
                    Scalar::Column(Column::from(&q.query_output_column_list[i])),
                    Column::from(&q.insert_column_list[i]),
                )
            })
            .collect();
        LogicalMap {
            include_existing: false,
            projects,
            input: Box::new(input),
        }
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
            cs.push(Column::from(x));
        }
        cs
    }

    fn column_ref(&mut self, x: &ResolvedColumnRefProto) -> Column {
        Column::from(x.column.get())
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
        if q.table_scan.is_none() {
            todo!("nested updates")
        }
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
        let mut projects = vec![];
        let mut updated_column = |column: &ResolvedColumnProto| -> Option<Scalar> {
            for item in &q.update_item_list {
                if let ResolvedColumnRefNode(target) = item.target.get().node.get() {
                    if target.column.get().name == column.name {
                        let value = match item.set_value.get().value.get().node.get() {
                            ResolvedDmldefaultNode(default) => {
                                self.default(column, default.parent.get().r#type.get())
                            }
                            other => self.expr_node(other, &mut input),
                        };
                        return Some(value);
                    }
                }
            }
            None
        };
        for column in &q.table_scan.get().parent.get().column_list {
            let as_column = Column::from(column);
            let value = updated_column(column).unwrap_or(Scalar::Column(as_column.clone()));
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
        }
    }

    fn default(&mut self, column: &ResolvedColumnProto, data_type: &TypeProto) -> Scalar {
        Scalar::Call(Box::new(Function::Default(
            Column::from(column),
            data_type::from_proto(data_type),
        )))
    }

    fn create_database(&mut self, q: &ResolvedCreateDatabaseStmtProto) -> Expr {
        // TODO fail on unsupported options
        LogicalCreateDatabase {
            name: Name {
                catalog_id: self.catalog_id,
                path: q.name_path.clone(),
            },
        }
    }

    fn assign(&mut self, q: &ResolvedSingleAssignmentStmtProto) -> Expr {
        let mut input = LogicalSingleGet;
        LogicalAssign {
            variable: q.variable.get().clone(),
            value: self.expr(q.expr.get(), &mut input),
            input: Box::new(input),
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
                Scalar::Call(Box::new(Function::from(function, signature, arguments)))
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
                return_null_on_error: Some(false),
                extended_cast: None,
            } => (expr, ty),
            other => panic!("{:?}", other),
        };
        Scalar::Cast(Box::new(self.expr(expr, outer)), data_type::from_proto(ty))
    }

    fn parameter(&mut self, x: &ResolvedParameterProto) -> Scalar {
        Scalar::Parameter(
            x.name.get().clone(),
            data_type::from_proto(x.parent.get().r#type.get()),
        )
    }

    fn subquery_expr(&mut self, x: &ResolvedSubqueryExprProto, outer: &mut Expr) -> Scalar {
        let parameters: Vec<Column> = x
            .parameter_list
            .iter()
            .map(|c| Column::from(c.column.get()))
            .collect();
        let subquery = self.any_resolved_scan(x.subquery.get());
        let (join, scalar) = match x.subquery_type.get() {
            // Scalar
            0 => {
                let join = Join::Single(vec![]);
                let scalar = single_column(x.subquery.get());
                (join, scalar)
            }
            // Array
            1 => unimplemented!(),
            // Exists
            2 => {
                let mark =
                    self.create_column("$exists".to_string(), DataType::Boolean, Phase::Convert);
                let join = Join::Mark(mark.clone(), vec![]);
                let scalar = Scalar::Column(mark);
                (join, scalar)
            }
            // In
            3 => {
                let mark = self.create_column("$in".to_string(), DataType::Boolean, Phase::Convert);
                let find = match x {
                    ResolvedSubqueryExprProto {
                        in_expr: Some(x), ..
                    } => self.expr(x, outer),
                    other => panic!("{:?}", other),
                };
                let check = single_column(x.subquery.get());
                let join_filter = vec![Scalar::Call(Box::new(Function::Equal(find, check)))];
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
        subquery: Expr,
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
        // TODO this doesn't work if we're only looking at part of the expression
        let rename_subquery_parameters: Vec<Column> = subquery_parameters
            .iter()
            .map(|p| self.create_column(p.name.clone(), p.data_type.clone(), Phase::Plan))
            .collect();
        let map_subquery_parameters: HashMap<Column, Column> = (0..subquery_parameters.len())
            .map(|i| {
                (
                    subquery_parameters[i].clone(),
                    rename_subquery_parameters[i].clone(),
                )
            })
            .collect();
        let dependent_join = LogicalDependentJoin {
            parameters: rename_subquery_parameters.clone(),
            predicates: vec![],
            subquery: Box::new(subquery.clone().subst(&map_subquery_parameters)),
            domain: Box::new(outer.clone().subst(&map_subquery_parameters)),
        };
        //             LogicalJoin
        //              +       +
        //              +       +
        //   DependentJoin     outer
        //    +         +
        let mut join_predicates: Vec<Scalar> = (0..subquery_parameters.len())
            .map(|i| {
                Scalar::Call(Box::new(Function::Equal(
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

    fn create_column(&mut self, name: String, data_type: DataType, created: Phase) -> Column {
        let column = Column {
            created,
            id: self.next_column_id,
            name,
            table: None,
            data_type,
        };
        self.next_column_id += 1;
        column
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
    };
    &q.column_list
}

fn single_column(q: &AnyResolvedScanProto) -> Scalar {
    Scalar::Column(Column::from(&column_list(q)[0]))
}

fn single_column_aggregate(q: &AnyResolvedAggregateScanBaseProto) -> &ResolvedScanProto {
    match q.node.get() {
        ResolvedAggregateScanNode(q) => q.parent.get().parent.get(),
    }
}

fn function_name(name: &String) -> String {
    format!("${}", name.trim_start_matches("ZetaSQL:"))
}

fn literal(value: &ValueProto, data_type: &TypeProto) -> Value {
    let value = match value {
        ValueProto { value: Some(value) } => value,
        _ => return Value::Null(data_type::from_proto(data_type)),
    };
    match value {
        Int64Value(x) => Value::Int64(*x),
        BoolValue(x) => Value::Boolean(*x),
        DoubleValue(x) => Value::Float64(*x),
        StringValue(x) => Value::Utf8(x.to_string()),
        DateValue(x) => Value::Date(*x),
        TimestampValue(x) => Value::Timestamp(microseconds_since_epoch(x)),
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
