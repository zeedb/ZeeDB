use std::fmt;

#[derive(Debug)]
pub enum Plan {
    Root(Operator),
    Unary(Operator, Box<Plan>),
    Binary(Operator, Box<Plan>, Box<Plan>),
}

impl fmt::Display for Plan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Plan::Root(op) => write!(f, "{}", op),
            Plan::Unary(op, input) => write!(f, "({} {})", op, *input),
            Plan::Binary(op, left, right) => write!(f, "({} {} {})", op, *left, *right),
        }
    }
}

// Operator plan nodes combine inputs in a Plan tree.
#[derive(Debug)]
pub enum Operator {
    // LogicalSingleGet acts as the input to a SELECT with no FROM clause.
    LogicalSingleGet,
    // LogicalGet(table) implements the FROM clause.
    LogicalGet(Table),
    // LogicalFilter(predicates) implements the WHERE/HAVING clauses.
    LogicalFilter(Vec<Scalar>),
    // LogicalProject(columns) implements the SELECT clause.
    LogicalProject(Vec<(Scalar, Column)>),
    // LogicalInnerJoin implements the default SQL join.
    LogicalInnerJoin(Vec<Scalar>),
    // LogicalRightJoin(predicates) is used for both left and right joins.
    // When parsing a left join, we convert it to a right join by reversing the order of left and right.
    // The left side is the build side of the join.
    LogicalRightJoin(Vec<Scalar>),
    // LogicalOuterJoin is a cartesian product join.
    LogicalOuterJoin(Vec<Scalar>),
    // LogicalSemiJoin(predicates) indicates a semi-join like `select 1 from customer where store_id in (select store_id from store)`.
    // Note that the left side is the build side of the join, which is the reverse of the usual convention.
    LogicalSemiJoin(Vec<Scalar>),
    // LogicalAntiJoin(predicates) indicates an anti-join like `select 1 from customer where store_id not in (select store_id from store)`.
    // Note that like Semi, the left side is the build side of the join.
    LogicalAntiJoin(Vec<Scalar>),
    // LogicalSingleJoin(predicates) implements "Single Join" from http://btw2017.informatik.uni-stuttgart.de/slidesandpapers/F1-10-37/paper_web.pdf
    // The correlated subquery is always on the left.
    LogicalSingleJoin(Vec<Scalar>),
    // LogicalMarkJoin(predicates, mark) implements "Mark Join" from http://btw2017.informatik.uni-stuttgart.de/slidesandpapers/F1-10-37/paper_web.pdf
    // The correlated subquery is always on the left.
    LogicalMarkJoin(Vec<Scalar>, Column),
    // LogicalWith(table) implements with subquery as  _.
    // The with-subquery is always on the left.
    LogicalWith(String),
    // LogicalGetWith(table) reads the subquery that was created by With.
    LogicalGetWith(String),
    // LogicalAggregate(group_by, aggregate) implements the GROUP BY clause.
    LogicalAggregate(Vec<Column>, Vec<(Aggregate, Column)>),
    // LogicalLimit(n) implements the LIMIT / OFFSET / TOP clause.
    LogicalLimit(Limit),
    // LogicalSort(columns) implements the ORDER BY clause.
    LogicalSort(Vec<Sort>),
    // LogicalUnion implements SELECT _ UNION ALL SELECT _.
    LogicalUnion,
    // LogicalIntersect implements SELECT _ INTERSECT SELECT _.
    LogicalIntersect,
    // LogicalExcept implements SELECT _ EXCEPT SELECT _.
    LogicalExcept,
    // LogicalInsert(table, columns) implements the INSERT operation.
    LogicalInsert(Table, Vec<Column>),
    // LogicalValues(rows, columns) implements VALUES expressions.
    LogicalValues(Vec<Vec<Scalar>>, Vec<Column>),
    // LogicalUpdate(sets) implements the UPDATE operation.
    LogicalUpdate(Vec<(Column, Column)>),
    // LogicalDelete(table) implements the DELETE operation.
    LogicalDelete(Table),
    // LogicalCreateDatabase(database) implements the CREATE DATABASE operation.
    LogicalCreateDatabase(Name),
    // LogicalCreateTable implements the CREATE TABLE operation.
    LogicalCreateTable(CreateTable),
    // LogicalCreateIndex implements the CREATE INDEX operation.
    LogicalCreateIndex(CreateIndex),
    // LogicalAlterTable implements the ALTER TABLE operation.
    LogicalAlterTable(AlterTable),
    // LogicalDrop implements the DROP DATABASE/TABLE/INDEX operation.
    LogicalDrop(Drop),
    // LogicalRename implements the RENAME operation.
    LogicalRename(Rename),
}

impl fmt::Display for Operator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Operator::LogicalSingleGet => write!(f, "LogicalSingleGet"),
            Operator::LogicalGet(table) => write!(f, "LogicalGet {}", table.name),
            Operator::LogicalFilter(predicates) => {
                let mut strings = vec![];
                for p in predicates {
                    strings.push(format!("{}", p));
                }
                write!(f, "LogicalFilter {}", strings.join(" "))
            }
            Operator::LogicalProject(projects) => {
                let mut strings = vec![];
                for (x, c) in projects {
                    strings.push(format!("[{} {}]", x, c.name));
                }
                write!(f, "LogicalProject {}", strings.join(" "))
            }
            Operator::LogicalInnerJoin(predicates) => write!(f, "TODO"),
            Operator::LogicalRightJoin(predicates) => write!(f, "TODO"),
            Operator::LogicalOuterJoin(predicates) => write!(f, "TODO"),
            Operator::LogicalSemiJoin(predicates) => write!(f, "TODO"),
            Operator::LogicalAntiJoin(predicates) => write!(f, "TODO"),
            Operator::LogicalSingleJoin(predicates) => write!(f, "TODO"),
            Operator::LogicalMarkJoin(predicates, Column) => write!(f, "TODO"),
            Operator::LogicalWith(name) => write!(f, "TODO"),
            Operator::LogicalGetWith(name) => write!(f, "TODO"),
            Operator::LogicalAggregate(group_by, aggregate) => write!(f, "TODO"),
            Operator::LogicalLimit(limit) => write!(f, "TODO"),
            Operator::LogicalSort(order_by) => write!(f, "TODO"),
            Operator::LogicalUnion => write!(f, "TODO"),
            Operator::LogicalIntersect => write!(f, "TODO"),
            Operator::LogicalExcept => write!(f, "TODO"),
            Operator::LogicalInsert(table, columns) => write!(f, "TODO"),
            Operator::LogicalValues(rows, columns) => write!(f, "TODO"),
            Operator::LogicalUpdate(updates) => write!(f, "TODO"),
            Operator::LogicalDelete(table) => write!(f, "TODO"),
            Operator::LogicalCreateDatabase(name) => write!(f, "TODO"),
            Operator::LogicalCreateTable(create) => write!(f, "TODO"),
            Operator::LogicalCreateIndex(create) => write!(f, "TODO"),
            Operator::LogicalAlterTable(alter) => write!(f, "TODO"),
            Operator::LogicalDrop(drop) => write!(f, "TODO"),
            Operator::LogicalRename(rename) => write!(f, "TODO"),
        }
    }
}

#[derive(Debug)]
pub struct Table {
    pub id: i64,
    pub name: String,
}

impl Table {
    pub fn from(table: zetasql::TableRefProto) -> Self {
        let id = table.serialization_id.expect("no serialization id");
        let name = table.name.expect("no table name");
        Table { id, name }
    }
}

#[derive(Debug, Clone)]
pub struct Column {
    pub id: i64,
    pub name: String,
    pub typ: encoding::Type,
}

impl Column {
    pub fn from(column: zetasql::ResolvedColumnProto) -> Self {
        let id = column.column_id.unwrap();
        let name = column.name.unwrap();
        let typ = encoding::Type::from(column.r#type.unwrap());
        Column { id, name, typ }
    }
}

#[derive(Debug)]
pub struct Name(Vec<String>);

#[derive(Debug)]
pub enum ObjectType {
    Database,
    Table,
    Index,
    Column,
}

#[derive(Debug)]
pub struct Limit {
    pub limit: i32,
    pub offset: i32,
}

#[derive(Debug)]
pub struct Sort {
    pub column: Column,
    pub desc: bool,
}

#[derive(Debug)]
pub struct CreateTable {
    pub name: Name,
    pub columns: Vec<(String, encoding::Type)>,
    pub partition_by: Vec<i32>,
    pub cluster_by: Vec<i32>,
    pub primary_key: Vec<i32>,
    pub replace_if_not_exists: bool,
}

#[derive(Debug)]
pub struct CreateIndex {
    pub name: Name,
    pub table: Name,
    pub columns: Vec<String>,
}

#[derive(Debug)]
pub struct AlterTable {
    pub name: Name,
    pub actions: Vec<Alter>,
}

#[derive(Debug)]
pub enum Alter {
    AddColumn(AddColumn),
    DropColumn(DropColumn),
}

#[derive(Debug)]
pub struct AddColumn {
    pub name: String,
    pub typ: encoding::Type,
    pub ignore_if_exists: bool,
}

#[derive(Debug)]
pub struct DropColumn {
    pub name: String,
    pub ignore_if_not_exists: bool,
}

#[derive(Debug)]
pub struct Drop {
    pub object: ObjectType,
    pub ignore_if_not_exists: bool,
}

#[derive(Debug)]
pub struct Rename {
    pub object: ObjectType,
    pub from: String,
    pub to: String,
}

#[derive(Debug)]
pub enum Scalar {
    Literal(Value),
    Column(Column),
    Call(Function, Vec<Scalar>),
    Cast(Box<Scalar>, encoding::Type),
}

impl fmt::Display for Scalar {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Scalar::Literal(value) => write!(f, "{}", value),
            Scalar::Column(column) => write!(f, "TODO"),
            Scalar::Call(function, arguments) => write!(f, "TODO"),
            Scalar::Cast(value, typ) => write!(f, "TODO"),
        }
    }
}

#[derive(Debug)]
pub enum Value {
    Int(i64),
    Bool(bool),
    Double(f64),
    String(String),
    Bytes(Vec<u8>),
    Date(chrono::Date<chrono::Utc>),
    Timestamp(chrono::DateTime<chrono::Utc>),
    Numeric(decimal::d128),
    Array(Vec<Value>),
    Struct(Vec<(String, Value)>),
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Int(x) => write!(f, "{}", x),
            Value::Bool(x) => write!(f, "{}", x),
            Value::Double(x) => write!(f, "{}", x),
            Value::String(x) => write!(f, "{:?}", x),
            Value::Bytes(x) => write!(f, "TODO"),
            Value::Date(x) => write!(f, "{}", x),
            Value::Timestamp(x) => write!(f, "{}", x),
            Value::Numeric(x) => write!(f, "TODO"),
            Value::Array(x) => write!(f, "TODO"),
            Value::Struct(x) => write!(f, "TODO"),
        }
    }
}

// Functions appear in scalar expressions.
#[derive(Debug)]
pub enum Function {
    Abs,
    Acos,
    Acosh,
    Add,
    And,
    Asin,
    Asinh,
    Atan,
    Atan2,
    Atanh,
    Between,
    BitCount,
    BitwiseAnd,
    BitwiseLeftShift,
    BitwiseNot,
    BitwiseOr,
    BitwiseRightShift,
    BitwiseXor,
    ByteLength,
    CaseNoValue,
    CaseWithValue,
    Ceil,
    CharLength,
    Coalesce,
    Concat,
    Cos,
    Cosh,
    CurrentDate,
    CurrentTimestamp,
    Date,
    DateAdd,
    DateDiff,
    DateFromUnixDate,
    DateSub,
    DateTrunc,
    Div,
    Divide,
    EndsWith,
    Equal,
    Exp,
    Extract,
    ExtractDate,
    Floor,
    FromBase64,
    FromHex,
    Greater,
    GreaterOrEqual,
    Greatest,
    If,
    Ifnull,
    In,
    IsInf,
    IsNan,
    IsNull,
    Least,
    Length,
    Less,
    LessOrEqual,
    Like,
    Ln,
    Log,
    Log10,
    Lower,
    Lpad,
    Ltrim,
    Mod,
    Multiply,
    Not,
    NotEqual,
    Or,
    Pow,
    Rand,
    RegexpContains,
    RegexpExtract,
    RegexpExtractAll,
    RegexpReplace,
    Repeat,
    Replace,
    Reverse,
    Round,
    Rpad,
    Rtrim,
    Sign,
    Sin,
    Sinh,
    Split,
    Sqrt,
    StartsWith,
    Strpos,
    Substr,
    Subtract,
    Tan,
    Tanh,
    Timestamp,
    TimestampAdd,
    TimestampDiff,
    TimestampFromUnixMicros,
    TimestampFromUnixMillis,
    TimestampFromUnixSeconds,
    TimestampMicros,
    TimestampMillis,
    TimestampSeconds,
    TimestampSub,
    TimestampTrunc,
    ToBase64,
    ToHex,
    Trim,
    Trunc,
    UnaryMinus,
    UnixDate,
    UnixMicrosFromTimestamp,
    UnixMillisFromTimestamp,
    UnixSecondsFromTimestamp,
    Upper,
}

impl Function {
    pub fn from(name: &str) -> Self {
        match name {
            "ZetaSQL:abs" => Function::Abs,
            "ZetaSQL:acos" => Function::Acos,
            "ZetaSQL:acosh" => Function::Acosh,
            "ZetaSQL:$add" => Function::Add,
            "ZetaSQL:$and" => Function::And,
            "ZetaSQL:asin" => Function::Asin,
            "ZetaSQL:asinh" => Function::Asinh,
            "ZetaSQL:atan" => Function::Atan,
            "ZetaSQL:atan2" => Function::Atan2,
            "ZetaSQL:atanh" => Function::Atanh,
            "ZetaSQL:$between" => Function::Between,
            "ZetaSQL:$bitwise_and" => Function::BitwiseAnd,
            "ZetaSQL:$bitwise_left_shift" => Function::BitwiseLeftShift,
            "ZetaSQL:$bitwise_not" => Function::BitwiseNot,
            "ZetaSQL:$bitwise_or" => Function::BitwiseOr,
            "ZetaSQL:$bitwise_right_shift" => Function::BitwiseRightShift,
            "ZetaSQL:$bitwise_xor" => Function::BitwiseXor,
            "ZetaSQL:byte_length" => Function::ByteLength,
            "ZetaSQL:$case_no_value" => Function::CaseNoValue,
            "ZetaSQL:$case_with_value" => Function::CaseWithValue,
            "ZetaSQL:ceil" => Function::Ceil,
            "ZetaSQL:char_length" => Function::CharLength,
            "ZetaSQL:coalesce" => Function::Coalesce,
            "ZetaSQL:concat" => Function::Concat,
            "ZetaSQL:cos" => Function::Cos,
            "ZetaSQL:cosh" => Function::Cosh,
            "ZetaSQL:current_date" => Function::CurrentDate,
            "ZetaSQL:current_timestamp" => Function::CurrentTimestamp,
            "ZetaSQL:date" => Function::Date,
            "ZetaSQL:date_add" => Function::DateAdd,
            "ZetaSQL:date_diff" => Function::DateDiff,
            "ZetaSQL:date_from_unix_date" => Function::DateFromUnixDate,
            "ZetaSQL:date_sub" => Function::DateSub,
            "ZetaSQL:date_trunc" => Function::DateTrunc,
            "ZetaSQL:div" => Function::Div,
            "ZetaSQL:$divide" => Function::Divide,
            "ZetaSQL:ends_with" => Function::EndsWith,
            "ZetaSQL:$equal" => Function::Equal,
            "ZetaSQL:exp" => Function::Exp,
            "ZetaSQL:$extract" => Function::Extract,
            "ZetaSQL:$extract_date" => Function::ExtractDate,
            "ZetaSQL:floor" => Function::Floor,
            "ZetaSQL:from_base64" => Function::FromBase64,
            "ZetaSQL:from_hex" => Function::FromHex,
            "ZetaSQL:$greater" => Function::Greater,
            "ZetaSQL:$greater_or_equal" => Function::GreaterOrEqual,
            "ZetaSQL:greatest" => Function::Greatest,
            "ZetaSQL:if" => Function::If,
            "ZetaSQL:ifnull" => Function::Ifnull,
            "ZetaSQL:$in" => Function::In,
            "ZetaSQL:is_inf" => Function::IsInf,
            "ZetaSQL:is_nan" => Function::IsNan,
            "ZetaSQL:$is_null" => Function::IsNull,
            "ZetaSQL:least" => Function::Least,
            "ZetaSQL:length" => Function::Length,
            "ZetaSQL:$less" => Function::Less,
            "ZetaSQL:$less_or_equal" => Function::LessOrEqual,
            "ZetaSQL:$like" => Function::Like,
            "ZetaSQL:ln" => Function::Ln,
            "ZetaSQL:log" => Function::Log,
            "ZetaSQL:log10" => Function::Log10,
            "ZetaSQL:lower" => Function::Lower,
            "ZetaSQL:lpad" => Function::Lpad,
            "ZetaSQL:ltrim" => Function::Ltrim,
            "ZetaSQL:mod" => Function::Mod,
            "ZetaSQL:$multiply" => Function::Multiply,
            "ZetaSQL:$not" => Function::Not,
            "ZetaSQL:$not_equal" => Function::NotEqual,
            "ZetaSQL:$or" => Function::Or,
            "ZetaSQL:pow" => Function::Pow,
            "ZetaSQL:rand" => Function::Rand,
            "ZetaSQL:regexp_contains" => Function::RegexpContains,
            "ZetaSQL:regexp_extract" => Function::RegexpExtract,
            "ZetaSQL:regexp_extract_all" => Function::RegexpExtractAll,
            "ZetaSQL:regexp_replace" => Function::RegexpReplace,
            "ZetaSQL:repeat" => Function::Repeat,
            "ZetaSQL:replace" => Function::Replace,
            "ZetaSQL:reverse" => Function::Reverse,
            "ZetaSQL:round" => Function::Round,
            "ZetaSQL:rpad" => Function::Rpad,
            "ZetaSQL:rtrim" => Function::Rtrim,
            "ZetaSQL:sign" => Function::Sign,
            "ZetaSQL:sin" => Function::Sin,
            "ZetaSQL:sinh" => Function::Sinh,
            "ZetaSQL:split" => Function::Split,
            "ZetaSQL:sqrt" => Function::Sqrt,
            "ZetaSQL:starts_with" => Function::StartsWith,
            "ZetaSQL:strpos" => Function::Strpos,
            "ZetaSQL:substr" => Function::Substr,
            "ZetaSQL:$subtract" => Function::Subtract,
            "ZetaSQL:tan" => Function::Tan,
            "ZetaSQL:tanh" => Function::Tanh,
            "ZetaSQL:timestamp" => Function::Timestamp,
            "ZetaSQL:timestamp_add" => Function::TimestampAdd,
            "ZetaSQL:timestamp_diff" => Function::TimestampDiff,
            "ZetaSQL:timestamp_from_unix_micros" => Function::TimestampFromUnixMicros,
            "ZetaSQL:timestamp_from_unix_millis" => Function::TimestampFromUnixMillis,
            "ZetaSQL:timestamp_from_unix_seconds" => Function::TimestampFromUnixSeconds,
            "ZetaSQL:timestamp_micros" => Function::TimestampMicros,
            "ZetaSQL:timestamp_millis" => Function::TimestampMillis,
            "ZetaSQL:timestamp_seconds" => Function::TimestampSeconds,
            "ZetaSQL:timestamp_sub" => Function::TimestampSub,
            "ZetaSQL:timestamp_trunc" => Function::TimestampTrunc,
            "ZetaSQL:to_base64" => Function::ToBase64,
            "ZetaSQL:to_hex" => Function::ToHex,
            "ZetaSQL:trim" => Function::Trim,
            "ZetaSQL:trunc" => Function::Trunc,
            "ZetaSQL:$unary_minus" => Function::UnaryMinus,
            "ZetaSQL:unix_date" => Function::UnixDate,
            "ZetaSQL:unix_seconds" => Function::UnixSecondsFromTimestamp,
            "ZetaSQL:unix_millis" => Function::UnixMillisFromTimestamp,
            "ZetaSQL:unix_micros" => Function::UnixMicrosFromTimestamp,
            "ZetaSQL:upper" => Function::Upper,
            other => panic!("{} is not supported", other),
        }
    }
}

// Aggregate functions appear in GROUP BY expressions.
#[derive(Debug)]
pub enum Aggregate {
    AnyValue(Column),
    ArrayAgg(Distinct, IgnoreNulls, Column),
    ArrayConcatAgg(Column),
    Avg(Distinct, Column),
    BitAnd(Distinct, Column),
    BitOr(Distinct, Column),
    BitXor(Distinct, Column),
    Count(Distinct, Column),
    CountStar,
    LogicalAnd(Column),
    LogicalOr(Column),
    Max(Column),
    Min(Column),
    StringAgg(Distinct, Column),
    // TODO what happens to string_agg(_, ', ')
    Sum(Distinct, Column),
}

impl Aggregate {
    pub fn from(
        name: &str,
        distinct: bool,
        ignore_nulls: bool,
        argument: Option<Column>,
    ) -> Self {
        let distinct = Distinct(distinct);
        let ignore_nulls = IgnoreNulls(ignore_nulls);
        match name {
            "ZetaSQL:any_value" => Aggregate::AnyValue(argument.unwrap()),
            "ZetaSQL:array_agg" => Aggregate::ArrayAgg(distinct, ignore_nulls, argument.unwrap()),
            "ZetaSQL:array_concat_agg" => Aggregate::ArrayConcatAgg(argument.unwrap()),
            "ZetaSQL:avg" => Aggregate::Avg(distinct, argument.unwrap()),
            "ZetaSQL:bit_and" => Aggregate::BitAnd(distinct, argument.unwrap()),
            "ZetaSQL:bit_or" => Aggregate::BitOr(distinct, argument.unwrap()),
            "ZetaSQL:bit_xor" => Aggregate::BitXor(distinct, argument.unwrap()),
            "ZetaSQL:count" => Aggregate::Count(distinct, argument.unwrap()),
            "ZetaSQL:$count_star" => Aggregate::CountStar,
            "ZetaSQL:logical_and" => Aggregate::LogicalAnd(argument.unwrap()),
            "ZetaSQL:logical_or" => Aggregate::LogicalOr(argument.unwrap()),
            "ZetaSQL:max" => Aggregate::Max(argument.unwrap()),
            "ZetaSQL:min" => Aggregate::Min(argument.unwrap()),
            "ZetaSQL:string_agg" => Aggregate::StringAgg(distinct, argument.unwrap()),
            "ZetaSQL:sum" => Aggregate::Sum(distinct, argument.unwrap()),
            other => panic!("{} is not supported", name),
        }
    }
}

#[derive(Debug)]
pub struct Distinct(bool);

#[derive(Debug)]
pub struct IgnoreNulls(bool);
