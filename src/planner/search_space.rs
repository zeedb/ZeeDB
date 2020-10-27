use crate::cost::*;
use crate::rule::*;
use node::*;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::ops;

// SearchSpace is a data structure that compactly describes a combinatorial set of query plans.
pub struct SearchSpace {
    pub groups: Vec<Group>,
    pub mexprs: Vec<MultiExpr>,
    pub memo: HashSet<(GroupID, Operator<GroupID>)>,
}

#[derive(Copy, Clone, Hash, Eq, PartialEq)]
pub struct GroupID(pub usize);

pub const UNLINKED: GroupID = GroupID(usize::MAX);

#[derive(Copy, Clone, Hash, Eq, PartialEq)]
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
    pub op: Operator<GroupID>,
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
            memo: HashSet::new(),
        }
    }

    pub fn add(&mut self, group: Group) -> GroupID {
        self.groups.push(group);
        GroupID(self.groups.len() - 1)
    }

    pub fn intern(&mut self, mexpr: MultiExpr) -> Option<MultiExprID> {
        let fingerprint = (mexpr.parent.clone(), mexpr.op.clone());
        if self.memo.contains(&fingerprint) {
            None
        } else {
            self.mexprs.push(mexpr);
            self.memo.insert(fingerprint);
            Some(MultiExprID(self.mexprs.len() - 1))
        }
    }
}

impl ops::Index<GroupID> for SearchSpace {
    type Output = Group;

    fn index(&self, index: GroupID) -> &Self::Output {
        &self.groups[index.0]
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
        &mut self.groups[index.0]
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
            writeln!(
                f,
                "{} #{} ${}",
                i,
                self.groups[i].props.cardinality,
                self.groups[i].winner.map(|w| w.cost).unwrap_or(f64::MAX)
            )?;
            for j in 0..self.groups[i].logical.len() {
                writeln!(f, "\t{:?}", self[self.groups[i].logical[j]])?;
            }
            for j in 0..self.groups[i].physical.len() {
                write!(f, "\t{:?}", self[self.groups[i].physical[j]])?;
                if self.groups[i].winner.map(|w| w.plan) == Some(self.groups[i].physical[j]) {
                    write!(f, " *")?;
                }
                writeln!(f, "")?;
            }
        }
        Ok(())
    }
}

impl fmt::Debug for MultiExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.op.name())?;
        for i in 0..self.op.len() {
            write!(f, " {}", self.op[i].0)?;
        }
        Ok(())
    }
}
