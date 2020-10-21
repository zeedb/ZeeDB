use crate::indent_print::*;
use crate::operator::*;
use std::collections::HashSet;
use std::fmt;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Expr(pub Box<Operator<Expr>>);

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.indent_print(f, 0)
    }
}

impl IndentPrint for Expr {
    fn indent_print(&self, f: &mut fmt::Formatter<'_>, indent: usize) -> fmt::Result {
        self.0.indent_print(f, indent)
    }
}

impl Expr {
    pub fn new(op: Operator<Expr>) -> Self {
        Expr(Box::new(op))
    }

    pub fn as_ref(&self) -> &Operator<Expr> {
        self.0.as_ref()
    }

    pub fn bottom_up_rewrite(self, visitor: &impl Fn(Expr) -> Expr) -> Expr {
        let operator = self.0.map(|child| child.bottom_up_rewrite(visitor));
        visitor(Expr::new(operator))
    }

    pub fn top_down_rewrite(self, visitor: &impl Fn(Expr) -> Expr) -> Expr {
        let expr = visitor(self);
        let operator = expr.0.map(|child| child.top_down_rewrite(visitor));
        Expr::new(operator)
    }

    pub fn iter(&self) -> ExprIterator {
        ExprIterator { stack: vec![self] }
    }
}

impl Scope for Expr {
    fn attributes(&self) -> HashSet<Column> {
        self.0.attributes()
    }
    fn free(&self, parameters: &Vec<Column>) -> HashSet<Column> {
        self.0.free(parameters)
    }
}

pub struct ExprIterator<'it> {
    stack: Vec<&'it Expr>,
}

impl<'it> Iterator for ExprIterator<'it> {
    type Item = &'it Expr;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(next) = self.stack.pop() {
            match next.as_ref() {
                Operator::LogicalUnion(left, right)
                | Operator::LogicalIntersect(left, right)
                | Operator::LogicalExcept(left, right)
                | Operator::Union(left, right)
                | Operator::Intersect(left, right)
                | Operator::Except(left, right)
                | Operator::LogicalJoin(_, left, right)
                | Operator::LogicalWith(_, _, left, right)
                | Operator::NestedLoop(_, left, right)
                | Operator::HashJoin(_, _, left, right)
                | Operator::CreateTempTable(_, _, left, right) => {
                    self.stack.push(left);
                    self.stack.push(right);
                }
                Operator::LogicalFilter(_, input)
                | Operator::LogicalMap(_, input)
                | Operator::LogicalProject(_, input)
                | Operator::LogicalAggregate { input, .. }
                | Operator::LogicalLimit { input, .. }
                | Operator::LogicalSort(_, input)
                | Operator::LogicalInsert(_, _, input)
                | Operator::LogicalValues(_, _, input)
                | Operator::LogicalUpdate(_, input)
                | Operator::LogicalDelete(_, input)
                | Operator::LogicalCreateTable {
                    input: Some(input), ..
                }
                | Operator::Filter(_, input)
                | Operator::Map(_, input)
                | Operator::Aggregate { input, .. }
                | Operator::Limit { input, .. }
                | Operator::Sort(_, input)
                | Operator::Insert(_, _, input)
                | Operator::Update(_, input)
                | Operator::Delete(_, input) => {
                    self.stack.push(input);
                }
                Operator::LogicalSingleGet { .. }
                | Operator::LogicalGet { .. }
                | Operator::LogicalDependentJoin { .. }
                | Operator::LogicalGetWith { .. }
                | Operator::LogicalCreateDatabase { .. }
                | Operator::LogicalCreateTable { .. }
                | Operator::LogicalCreateIndex { .. }
                | Operator::LogicalAlterTable { .. }
                | Operator::LogicalDrop { .. }
                | Operator::LogicalRename { .. }
                | Operator::TableFreeScan { .. }
                | Operator::SeqScan { .. }
                | Operator::IndexScan { .. }
                | Operator::GetTempTable { .. }
                | Operator::Values { .. }
                | Operator::CreateDatabase { .. }
                | Operator::CreateTable { .. }
                | Operator::CreateIndex { .. }
                | Operator::AlterTable { .. }
                | Operator::Drop { .. }
                | Operator::Rename { .. } => {}
            }
            Some(next)
        } else {
            None
        }
    }
}
