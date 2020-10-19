use crate::indent_print::*;
use crate::operator::*;
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

    pub fn correlated(&self, column: &Column) -> bool {
        for expr in self.iter() {
            if expr.0.introduces(column) {
                return false;
            }
        }
        true
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
                _ => {}
            }
            Some(next)
        } else {
            None
        }
    }
}
