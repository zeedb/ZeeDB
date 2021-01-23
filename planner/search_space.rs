use crate::{cardinality_estimation::*, cost::*, rule::*};
use ast::*;
use std::{
    collections::{HashMap, HashSet},
    fmt, ops,
    ops::{Index, IndexMut},
};

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
    pub upper_bound: Context<Cost>,
    // winner holds the best physical plan discovered so far for each possible physical property.
    pub winners: Context<Winner>,
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

pub struct Context<T> {
    by_required_prop: [Option<T>; 3],
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum PhysicalProp {
    None = 0,
    BroadcastDist = 1,
    ExchangeDist = 2,
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
            writeln!(f, "{} #{}", i, card)?;
            for j in 0..self[GroupID(i)].logical.len() {
                writeln!(f, "\t{:?}", self[self[GroupID(i)].logical[j]])?;
            }
            for j in 0..self[GroupID(i)].physical.len() {
                write!(f, "\t{:?}", self[self[GroupID(i)].physical[j]])?;
                for require in PhysicalProp::all() {
                    if let Some(winner) = self[GroupID(i)].winners[require] {
                        if winner.plan == self[GroupID(i)].physical[j] {
                            if require == PhysicalProp::None {
                                write!(f, " *")?;
                            } else {
                                write!(f, " *{}", require.name())?;
                            }
                            let cost = self[GroupID(i)].winners[require]
                                .map(|w| w.cost)
                                .unwrap_or(f64::NAN);
                            write!(f, " ${}", cost)?;
                        }
                    }
                }
                writeln!(f, "")?;
            }
        }
        Ok(())
    }
}

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
        match &self.expr {
            LogicalGet { table, .. } => write!(f, "{} {}", self.expr.name(), &table.name)?,
            HashJoin {
                broadcast: true, ..
            } => write!(f, "{} (broadcast)", self.expr.name())?,
            HashJoin {
                broadcast: false, ..
            } => write!(f, "{} (partitioned)", self.expr.name())?,
            _ => write!(f, "{}", self.expr.name())?,
        }
        for i in 0..self.expr.len() {
            write!(f, " {}", self.expr[i])?;
        }
        Ok(())
    }
}

pub fn leaf(expr: &Expr) -> GroupID {
    if let Leaf { gid } = expr {
        GroupID(*gid)
    } else {
        panic!("{} is not Leaf", expr.name())
    }
}

impl<T> Context<T> {
    pub fn empty() -> Self {
        Self {
            by_required_prop: Default::default(),
        }
    }
}

impl<T> Index<PhysicalProp> for Context<T> {
    type Output = Option<T>;

    fn index(&self, index: PhysicalProp) -> &Self::Output {
        &self.by_required_prop[index as usize]
    }
}

impl<T> IndexMut<PhysicalProp> for Context<T> {
    fn index_mut(&mut self, index: PhysicalProp) -> &mut Self::Output {
        &mut self.by_required_prop[index as usize]
    }
}

impl PhysicalProp {
    pub fn all() -> Vec<Self> {
        vec![
            PhysicalProp::None,
            PhysicalProp::BroadcastDist,
            PhysicalProp::ExchangeDist,
        ]
    }

    pub fn required(expr: &Expr, input: usize) -> Self {
        match (expr, input) {
            (
                HashJoin {
                    broadcast: true, ..
                },
                0,
            ) => PhysicalProp::BroadcastDist,
            (
                HashJoin {
                    broadcast: false, ..
                },
                0,
            ) => PhysicalProp::ExchangeDist,
            (
                HashJoin {
                    broadcast: false, ..
                },
                1,
            ) => PhysicalProp::ExchangeDist,
            (IndexScan { .. }, 0) => PhysicalProp::BroadcastDist,
            (NestedLoop { .. }, 0) => PhysicalProp::BroadcastDist,
            (Aggregate { .. }, 0) => PhysicalProp::ExchangeDist,
            (_, _) => PhysicalProp::None,
        }
    }

    pub fn met(&self, expr: &Expr) -> bool {
        match self {
            PhysicalProp::None => match expr {
                Expr::Broadcast { .. } | Expr::Exchange { .. } => false,
                _ => true,
            },
            PhysicalProp::BroadcastDist => match expr {
                Expr::Broadcast { .. } => true,
                _ => false,
            },
            PhysicalProp::ExchangeDist => match expr {
                Expr::Exchange { .. } => true,
                _ => false,
            },
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            PhysicalProp::None => "Any",
            PhysicalProp::BroadcastDist => "Broadcast",
            PhysicalProp::ExchangeDist => "Exchange",
        }
    }
}