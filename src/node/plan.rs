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
    Literal(Literal),
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
pub enum Literal {
    Int(i64),
    Bool(bool),
    Double(f64),
    String(String),
    Bytes(Vec<u8>),
    Date(chrono::Date<chrono::Utc>),
    Timestamp(chrono::DateTime<chrono::Utc>),
    Array(Vec<Literal>),
    Struct(Vec<(String, Literal)>),
    Numeric(decimal::d128),
}

impl fmt::Display for Literal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Literal::Int(i) => write!(f, "{}", i),
            Literal::Bool(b) => write!(f, "TODO"),
            Literal::Double(d) => write!(f, "TODO"),
            Literal::String(s) => write!(f, "TODO"),
            Literal::Bytes(bs) => write!(f, "TODO"),
            Literal::Date(date) => write!(f, "TODO"),
            Literal::Timestamp(time) => write!(f, "TODO"),
            Literal::Array(xs) => write!(f, "TODO"),
            Literal::Struct(xs) => write!(f, "TODO"),
            Literal::Numeric(n) => write!(f, "TODO"),
        }
    }
}

// Functions appear in scalar expressions.
#[derive(Debug)]
pub enum Function {
    AddDouble,
    AddInt64,
    AddNumeric,
    And,
    CaseNoValue,
    CaseWithValue,
    DivideDouble,
    DivideNumeric,
    Greater,
    GreaterOrEqual,
    Less,
    LessOrEqual,
    Equal,
    StringLike,
    In,
    Between,
    IsNull,
    MultiplyDouble,
    MultiplyInt64,
    MultiplyNumeric,
    Not,
    NotEqual,
    Or,
    SubtractDouble,
    SubtractInt64,
    SubtractNumeric,
    UnaryMinusInt64,
    UnaryMinusDouble,
    UnaryMinusNumeric,
    // Bitwise unary operators.
    BitwiseNotInt64,
    // Bitwise binary operators.
    BitwiseOrInt64,
    BitwiseXorInt64,
    BitwiseAndInt64,
    // For all bitwise shift operators, the second argument has int64 type as i32.
    // Expected behavior of bitwise shift operations:
    // * Shifting by a negative offset is an error.
    // * Shifting right on signed values does not do sign extension.
    BitwiseLeftShiftInt64,
    BitwiseRightShiftInt64,
    // BITCOUNT functions.
    BitCountInt64,
    // String functions
    ConcatString,
    StrposString,
    LowerString,
    UpperString,
    LengthString,
    StartsWithString,
    EndsWithString,
    SubstrString,
    TrimString,
    LtrimString,
    RtrimString,
    ReplaceString,
    RegexpExtractString,
    RegexpReplaceString,
    RegexpExtractAllString,
    ByteLengthString,
    CharLengthString,
    SplitString,
    RegexpContainsString,
    ToBase64,
    FromBase64,
    ToHex,
    FromHex,
    LpadString,
    RpadString,
    RepeatString,
    ReverseString,
    // Control flow functions
    If,
    // Coalesce is used to express the output join column in FULL JOIN.
    Coalesce,
    Ifnull,
    // Time functions
    DateAddDate,
    TimestampAdd,
    DateDiffDate,
    TimestampDiff,
    DateSubDate,
    TimestampSub,
    DateTruncDate,
    TimestampTrunc,
    DateFromUnixDate,
    TimestampFromInt64Seconds,
    TimestampFromInt64Millis,
    TimestampFromInt64Micros,
    TimestampFromUnixSecondsInt64,
    TimestampFromUnixSecondsTimestamp,
    TimestampFromUnixMillisInt64,
    TimestampFromUnixMillisTimestamp,
    TimestampFromUnixMicrosInt64,
    TimestampFromUnixMicrosTimestamp,
    UnixDate,
    UnixSecondsFromTimestamp,
    UnixMillisFromTimestamp,
    UnixMicrosFromTimestamp,
    DateFromYearMonthDay,
    TimestampFromString,
    // Signatures for extracting date parts, taking a date/timestam as i32p
    // and the target date part as arguments.
    ExtractFromDate,
    ExtractFromTimestamp,
    // TIMESTAMP.
    ExtractDateFromTimestamp,
    // Math functions
    AbsInt64,
    AbsDouble,
    AbsNumeric,
    SignInt64,
    SignDouble,
    SignNumeric,
    RoundDouble,
    RoundNumeric,
    RoundWithDigitsDouble,
    RoundWithDigitsNumeric,
    TruncDouble,
    TruncNumeric,
    TruncWithDigitsDouble,
    TruncWithDigitsNumeric,
    CeilDouble,
    CeilNumeric,
    FloorDouble,
    FloorNumeric,
    ModInt64,
    ModNumeric,
    DivInt64,
    DivNumeric,
    IsInf,
    IsNan,
    Greatest,
    Least,
    SqrtDouble,
    PowDouble,
    PowNumeric,
    ExpDouble,
    NaturalLogarithmDouble,
    DecimalLogarithmDouble,
    LogarithmDouble,
    CosDouble,
    CoshDouble,
    AcosDouble,
    AcoshDouble,
    SinDouble,
    SinhDouble,
    AsinDouble,
    AsinhDouble,
    TanDouble,
    TanhDouble,
    AtanDouble,
    AtanhDouble,
    Atan2Double,
    // Non-deterministic functions
    CurrentDate,
    CurrentTimestamp,
    Rand,
}

impl Function {
    pub fn from(name: String) -> Self {
        unimplemented!()
    }
}

// Aggregate functions appear in GROUP BY expressions.
#[derive(Debug)]
pub enum Aggregate {
    CountStar,
    AnyValue(Column),
    ArrayAgg(Distinct, IgnoreNulls, Column),
    ArrayConcatAgg(Column),
    AvgInt64(Distinct, Column),
    AvgDouble(Distinct, Column),
    AvgNumeric(Distinct, Column),
    BitAndInt64(Distinct, Column),
    BitOrInt64(Distinct, Column),
    BitXorInt64(Distinct, Column),
    Count(Distinct, Column),
    LogicalAnd(Column),
    LogicalOr(Column),
    Max(Column),
    Min(Column),
    StringAggString(Distinct, Column),
    StringAggDelimString(Distinct, Column),
    StringAggBytes(Distinct, Column),
    StringAggDelimBytes(Distinct, Column),
    SumInt64(Distinct, Column),
    SumDouble(Distinct, Column),
    SumNumeric(Distinct, Column),
}

impl Aggregate {
    pub fn from(
        name: String,
        distinct: bool,
        ignore_nulls: bool,
        argument: Option<Column>,
    ) -> Self {
        unimplemented!()
    }
}

#[derive(Debug)]
pub struct Distinct(bool);

#[derive(Debug)]
pub struct IgnoreNulls(bool);
