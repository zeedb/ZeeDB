#[derive(Debug)]
pub enum Plan {
    Logical(Logical, Vec<Plan>),
}

// Logical plan nodes represent the query between parsing by ZetaSQL and optimization by the query planner.
#[derive(Debug)]
pub enum Logical {
    // GetNone implements SELECT with no FROM clause.
    GetNone,
    /// Get(table) implements the FROM clause.
    Get(Table),
    // Filter(predicates) implements the WHERE/HAVING clauses.
    Filter(Vec<Scalar>),
    // Project(columns) implements the SELECT clause.
    Project(Vec<(Scalar, Column)>),
    // InnerJoin implements the default SQL join.
    InnerJoin(Vec<Scalar>),
    // RightJoin(predicates) is used for both left and right joins.
    // When parsing a left join, we convert it to a right join by reversing the order of left and right.
    // The left side is the build side of the join.
    RightJoin(Vec<Scalar>),
    OuterJoin(Vec<Scalar>),
    // SemiJoin(predicates) indicates a semi-join like `select 1 from customer where store_id in (select store_id from store)`.
    // Note that the left side is the build side of the join, which is the reverse of the usual convention.
    SemiJoin(Vec<Scalar>),
    // AntiJoin(predicates) indicates an anti-join like `select 1 from customer where store_id not in (select store_id from store)`.
    // Note that like Semi, the left side is the build side of the join.
    AntiJoin(Vec<Scalar>),
    // SingleJoin(predicates) implements "Single Join" from http://btw2017.informatik.uni-stuttgart.de/slidesandpapers/F1-10-37/paper_web.pdf
    // The correlated subquery is always on the left.
    SingleJoin(Vec<Scalar>),
    // MarkJoin(predicates, mark) implements "Mark Join" from http://btw2017.informatik.uni-stuttgart.de/slidesandpapers/F1-10-37/paper_web.pdf
    // The correlated subquery is always on the left.
    MarkJoin(Vec<Scalar>, Column),
    // With(table) implements with subquery as  _.
    // The with-subquery is always on the left.
    With(String),
    // GetWith(table) reads the subquery that was created by With.
    GetWith(String),
    // Aggregate(group_by, aggregate) implements the GROUP BY clause.
    Aggregate(Vec<Column>, Vec<(Aggregate, Column)>),
    // Limit(n) implements the LIMIT / OFFSET / TOP clause.
    Limit(Limit),
    // Sort(columns) implements the ORDER BY clause.
    Sort(Vec<Sort>),
    // Union implements SELECT _ UNION ALL SELECT _.
    Union,
    // Intersect implements SELECT _ INTERSECT SELECT _.
    Intersect,
    // Except implements SELECT _ EXCEPT SELECT _.
    Except,
    // Insert(table, columns) implements the INSERT operation.
    Insert(Table, Vec<Column>),
    // Values(rows, columns) implements VALUES expressions.
    Values(Vec<Vec<Scalar>>, Vec<Column>),
    // Update(sets) implements the UPDATE operation.
    Update(Vec<(Column, Column)>),
    // Delete(table) implements the DELETE operation.
    Delete(Table),
    // CreateDatabase(database) implements the CREATE DATABASE operation.
    CreateDatabase(Name),
    // CreateTable implements the CREATE TABLE operation.
    CreateTable(CreateTable),
    // CreateIndex implements the CREATE INDEX operation.
    CreateIndex(CreateIndex),
    // AlterTable implements the ALTER TABLE operation.
    AlterTable(AlterTable),
    // Drop implements the DROP DATABASE/TABLE/INDEX operation.
    Drop(Drop),
    // Rename implements the RENAME operation.
    Rename(Rename),
}

#[derive(Debug)]
pub struct Table {
    pub id: i64,
    pub name: String,
}

impl Table {
    pub fn from(table: zetasql::TableRefProto) -> Table {
        let id = table.serialization_id.expect("no serialization id");
        let name = table.name.expect("no table name");
        Table { id, name }
    }
}

#[derive(Debug)]
pub struct Column {
    pub id: i64,
    pub name: String,
    pub typ: encoding::Type,
}

impl Column {
    pub fn from(column: zetasql::ResolvedColumnProto) -> Column {
        let id = column.column_id.unwrap();
        let name = column.name.unwrap();
        let typ = encoding::Type::from(column.r#type.unwrap());
        Column { id, name, typ }
    }
}

#[derive(Debug)]
pub struct Name(Vec<String>);

#[derive(Debug)]
pub enum Object {
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
    pub object: Object,
    pub ignore_if_not_exists: bool,
}

#[derive(Debug)]
pub struct Rename {
    pub object: Object,
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
    pub fn from(name: String) -> Function {
        unimplemented!()
    }
}

// Aggregate functions appear in GROUP BY expressions.
#[derive(Debug)]
pub enum Aggregate {
    CountStar,
    AnyValue,
    ArrayAgg(Distinct, IgnoreNulls),
    ArrayConcatAgg,
    AvgInt64(Distinct),
    AvgDouble(Distinct),
    AvgNumeric(Distinct),
    BitAndInt64(Distinct),
    BitOrInt64(Distinct),
    BitXorInt64(Distinct),
    Count(Distinct),
    LogicalAnd,
    LogicalOr,
    Max,
    Min,
    StringAggString(Distinct),
    StringAggDelimString(Distinct),
    StringAggBytes(Distinct),
    StringAggDelimBytes(Distinct),
    SumInt64(Distinct),
    SumDouble(Distinct),
    SumNumeric(Distinct),
}

#[derive(Debug)]
pub struct Distinct(bool);

#[derive(Debug)]
pub struct IgnoreNulls(bool);
