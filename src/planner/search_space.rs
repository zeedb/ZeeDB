use crate::cost::*;
use crate::rule::*;
use ast::*;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::ops;

// TODO make fields private, moving some functions to the file.

// SearchSpace is a data structure that compactly describes a combinatorial set of query plans.
pub struct SearchSpace {
    pub groups: Vec<Option<Group>>,
    pub mexprs: Vec<MultiExpr>,
    pub memo_first: HashMap<Expr, MultiExprID>,
    pub memo_all: HashMap<(GroupID, Expr), MultiExprID>,
}

#[derive(Copy, Clone, Hash, Eq, Ord, PartialOrd, PartialEq)]
pub struct GroupID(pub usize);

#[derive(Copy, Clone, Hash, Eq, Ord, PartialOrd, PartialEq)]
pub struct MultiExprID(pub usize);

// Group represents a single logical query, which can be realized by many
// specific logical and physical query plans.
pub struct Group {
    // logical holds a set of equivalent logical query plans.
    pub logical: Vec<MultiExprID>,
    // physical holds a set of physical implementations of the query plans in logical.
    pub physical: Vec<MultiExprID>,
    // props holds the logical characteristics of the output of this part of the query plan.
    // No matter how we implement this group using physical operators,
    // these logical characteristics will not change.
    pub props: LogicalProps,
    // lower_bound is a crude estimate of the lowest-cost plan we could possibly discover.
    // We calculated it by looking at the logical schema of the current group
    // and considering the minimal cost of joins and operator overhead to create it.
    // We use lower_bound to stop early when we know the cost of the current group
    // will exceed the upper_bound.
    pub lower_bound: Cost,
    // upper_bound is calculated by taking a winning plan and propagating a goal downwards.
    // We need to find a plan that is better than upper_bound, or it will be ignored
    // because it's worse than a plan we already know about.
    pub upper_bound: Cost,
    // winner holds the best physical plan discovered so far.
    pub winner: Option<Winner>,
    // explored is marked true on the first invocation of optimizeGroup,
    // whose job is to make sure optimizeExpr is called on every group at least once.
    pub explored: bool,
}

// MultiExpr represents a part of a Group.
// Unlike Group, which represents *all* equivalent query plans,
// MultiExpr specifies operator at the top of a the query.
pub struct MultiExpr {
    // Parent group of this expression.
    pub parent: GroupID,
    // The top operator in this query.
    // Inputs are represented using Group,
    // so they represent a class of equivalent plans rather than a single plan.
    pub expr: Expr,
    // As we try different *logical* transformation rules,
    // we will record the fact that we've already tried this rule on this multi-expression
    // so we can avoid checking it agin. It's safe to mark transformations as complete,
    // because we explore the inputs to each MultiExpr before we start
    // applying transformation rules to the group.
    pub fired: HashSet<Rule>,
}

#[derive(Copy, Clone)]
pub struct Winner {
    pub plan: MultiExprID,
    pub cost: Cost,
}

pub struct LogicalProps {
    // cardinality contains the estimated number of rows in the query.
    pub cardinality: usize,
    // column_unique_cardinality contains the number of distinct values in each column.
    pub column_unique_cardinality: HashMap<Column, usize>,
}

impl SearchSpace {
    pub fn new() -> Self {
        Self {
            groups: vec![],
            mexprs: vec![],
            memo_first: HashMap::new(),
            memo_all: HashMap::new(),
        }
    }

    pub fn reserve(&mut self) -> GroupID {
        self.groups.push(None);
        GroupID(self.groups.len() - 1)
    }

    pub fn add_group(&mut self, gid: GroupID, group: Group) {
        self.groups[gid.0] = Some(group);
    }

    pub fn add_mexpr(&mut self, mexpr: MultiExpr) -> Option<MultiExprID> {
        let mid = MultiExprID(self.mexprs.len());
        // Record the first instance of each logical expression.
        if self.find_dup(&mexpr.expr).is_none() {
            self.memo_first.insert(mexpr.expr.clone(), mid);
        }
        // Only add each logical expression to each group once.
        if self.find_dup_in(&mexpr.expr, mexpr.parent).is_some() {
            return None;
        }
        self.memo_all
            .insert((mexpr.parent, mexpr.expr.clone()), mid);
        self.mexprs.push(mexpr);
        Some(mid)
    }

    pub fn find_dup(&mut self, expr: &Expr) -> Option<MultiExprID> {
        if expr.is_logical() {
            self.memo_first.get(expr).map(|id| *id)
        } else {
            None
        }
    }

    pub fn find_dup_in(&mut self, expr: &Expr, parent: GroupID) -> Option<MultiExprID> {
        if expr.is_logical() {
            self.memo_all.get(&(parent, expr.clone())).map(|id| *id)
        } else {
            None
        }
    }
}

impl fmt::Display for GroupID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl IndentPrint for GroupID {
    fn indent_print(&self, f: &mut fmt::Formatter<'_>, _indent: usize) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl ops::Index<GroupID> for SearchSpace {
    type Output = Group;

    fn index(&self, index: GroupID) -> &Self::Output {
        self.groups[index.0].as_ref().unwrap()
    }
}

impl ops::Index<MultiExprID> for SearchSpace {
    type Output = MultiExpr;

    fn index(&self, index: MultiExprID) -> &Self::Output {
        &self.mexprs[index.0]
    }
}

impl ops::IndexMut<GroupID> for SearchSpace {
    fn index_mut(&mut self, index: GroupID) -> &mut Self::Output {
        self.groups[index.0].as_mut().unwrap()
    }
}

impl ops::IndexMut<MultiExprID> for SearchSpace {
    fn index_mut(&mut self, index: MultiExprID) -> &mut Self::Output {
        &mut self.mexprs[index.0]
    }
}

impl fmt::Debug for SearchSpace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for i in 0..self.groups.len() {
            let card = self[GroupID(i)].props.cardinality;
            let cost = self[GroupID(i)].winner.map(|w| w.cost).unwrap_or(f64::NAN);
            writeln!(f, "{} #{} ${}", i, card, cost)?;
            for j in 0..self[GroupID(i)].logical.len() {
                writeln!(f, "\t{:?}", self[self[GroupID(i)].logical[j]])?;
            }
            for j in 0..self[GroupID(i)].physical.len() {
                write!(f, "\t{:?}", self[self[GroupID(i)].physical[j]])?;
                if self[GroupID(i)].winner.map(|w| w.plan) == Some(self[GroupID(i)].physical[j]) {
                    write!(f, " *")?;
                }
                writeln!(f, "")?;
            }
        }
        Ok(())
    }
}

impl Group {}

impl MultiExpr {
    pub fn new(parent: GroupID, expr: Expr) -> Self {
        Self {
            parent,
            expr,
            fired: HashSet::new(),
        }
    }
}

impl fmt::Debug for MultiExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.expr.name())?;
        for i in 0..self.expr.len() {
            write!(f, " {}", self.expr[i])?;
        }
        Ok(())
    }
}

pub(crate) fn leaf(expr: &Expr) -> GroupID {
    if let Leaf(gid) = expr {
        GroupID(*gid)
    } else {
        panic!("{} is not Leaf", expr.name())
    }
}
