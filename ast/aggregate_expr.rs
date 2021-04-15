use kernel::DataType;
use serde::{Deserialize, Serialize};

use crate::Column;

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct AggregateExpr {
    pub function: AggregateFunction,
    pub distinct: bool,
    pub input: Column,
    pub output: Column,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum AggregateFunction {
    AnyValue,
    Count,
    LogicalAnd,
    LogicalOr,
    Max,
    Min,
    Sum,
}

impl AggregateFunction {
    pub fn from(name: &str) -> Self {
        match name {
            "ZetaSQL:any_value" => AggregateFunction::AnyValue,
            "ZetaSQL:avg" => panic!("avg should be converted into sum / count"),
            "ZetaSQL:count" => AggregateFunction::Count,
            "ZetaSQL:logical_and" => AggregateFunction::LogicalAnd,
            "ZetaSQL:logical_or" => AggregateFunction::LogicalOr,
            "ZetaSQL:max" => AggregateFunction::Max,
            "ZetaSQL:min" => AggregateFunction::Min,
            "ZetaSQL:sum" => AggregateFunction::Sum,
            _ => panic!("{} is not supported", name),
        }
    }

    pub fn data_type(&self, column_type: DataType) -> DataType {
        match self {
            AggregateFunction::AnyValue
            | AggregateFunction::Max
            | AggregateFunction::Min
            | AggregateFunction::Sum => column_type.clone(),
            AggregateFunction::Count => DataType::I64,
            AggregateFunction::LogicalAnd | AggregateFunction::LogicalOr => DataType::Bool,
        }
    }
}
