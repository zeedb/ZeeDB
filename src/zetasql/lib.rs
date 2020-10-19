pub mod local_service;

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FieldFormat {}
pub mod field_format {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Format {
        /// No format annotation.
        /// This is meant as a no-annotation marker in code and should not actually
        /// be written as an annotation in .proto files.
        DefaultFormat = 0,
        /// A ZetaSQL DATE, encoded as days since 1970-01-01 UTC.
        /// Can be applied to a field with ctype int32 or an int64.
        ///
        /// The function DecodeFormattedDate in public/functions/date_time_util.h
        /// can be used to decode all supported date formats.
        Date = 1,
        /// A ZetaSQL timestamp.  Timestamps are encoded as elapsed
        /// seconds, millis, micros, or nanos since 1970-01-01 00:00:00 UTC.
        /// TIMESTAMP_SECONDS is the standard unix time_t type.
        /// Should be applied to fields with ctype int64.
        ///
        TimestampSeconds = 2,
        TimestampMillis = 3,
        TimestampMicros = 4,
        TimestampNanos = 5,
        /// A ZetaSQL DATE encoded in the MySQL date decimal format,
        /// which looks like YYYYMMDD written as an integer.
        /// Can be applied to a field with type int32 or an int64.
        ///
        /// NOTE: The value 0 decodes to NULL in this format.  (Otherwise it would
        /// be an invalid date.)
        ///
        /// The function DecodeFormattedDate in
        /// zetasql/public/functions/date_time_util.h
        /// can be used to decode all supported date formats.
        ///
        DateDecimal = 6,
        /// A ZetaSQL TIME encoded in an 8-byte bit field.
        /// See zetasql/public/civil_time.h for encoding methods.
        /// Can be applied to a field with ctype int64.
        TimeMicros = 7,
        /// A ZetaSQL DATETIME encoded in an 8-byte bit field.
        /// See zetasql/public/civil_time.h for encoding methods.
        /// Can be applied to a field with ctype int64.
        ///
        /// NOTE: The value 0 decodes to NULL in this format.  (Otherwise it would
        /// be an invalid datetime.)
        /// To have the default DATETIME value at 1970-01-01 00:00:00, use
        /// 138630961515462656 as the default integer value.
        DatetimeMicros = 8,
        /// A ZetaSQL GEOGRAPHY encoded using either C++'s
        /// STGeographyEncoder::Encode (util/geometry/st_lib/stgeography_coder.h) or
        /// Java's STGeographyCoder.encode
        /// (java/com/google/common/geometry/stlib/STGeographyCoder.java).
        ///
        /// Can be applied to bytes fields.
        StGeographyEncoded = 9,
        /// A ZetaSQL NUMERIC value. These values are encoded as scaled integers in
        /// two's complement form with the most significant bit storing the sign. See
        /// NumericValue::SerializeAsProtoBytes() for serialization format details.
        ///
        /// Can be applied to bytes fields.
        Numeric = 10,
        /// A ZetaSQL BIGNUMERIC value. These values are encoded as scaled integers
        /// in two's complement form with the most significant bit storing the sign.
        /// See BigNumericValue::SerializeAsProtoBytes() for serialization format
        /// details.
        ///
        /// Can be applied to bytes fields.
        Bignumeric = 11,
        /// User code that switches on this enum must have a default case so
        /// builds won't break if new enums get added.
        FieldFormatTypeSwitchMustHaveADefault = -1,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeprecatedEncoding {}
pub mod deprecated_encoding {
    /// DEPRECATED - Encoding has been folded into the Type enum above.
    /// This should not be used directly.
    /// TODO Remove this once there are no more references.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Encoding {
        DefaultEncoding = 0,
        /// Like in FieldFormat, PACKED32 is the wrong name, and people should be
        /// using DATE_DECIMAL instead.  This whole enum definition is deprecated
        /// though, so this definition probably won't be fixed.
        ///
        /// This is the wrong name. The format is DATE_DECIMAL.
        DatePacked32 = 1,
        FieldFormatEncodingSwitchMustHaveADefault = -1,
    }
}
/// This represents the serialized form of the zetasql::Type.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TypeProto {
    #[prost(enumeration = "TypeKind", optional, tag = "1")]
    pub type_kind: ::std::option::Option<i32>,
    /// If the type is not a simple type, then one (and only one) of these
    /// will be populated.
    #[prost(message, optional, boxed, tag = "2")]
    pub array_type: ::std::option::Option<::std::boxed::Box<ArrayTypeProto>>,
    #[prost(message, optional, tag = "3")]
    pub struct_type: ::std::option::Option<StructTypeProto>,
    #[prost(message, optional, tag = "4")]
    pub proto_type: ::std::option::Option<ProtoTypeProto>,
    #[prost(message, optional, tag = "5")]
    pub enum_type: ::std::option::Option<EnumTypeProto>,
    /// These <file_descriptor_set>s may (optionally) be populated only for
    /// the 'outermost' TypeProto when serializing a ZetaSQL Type,
    /// in particular when the TypeProto is created using
    /// zetasql::Type::SerializeToSelfContainedProto().  They will not be
    /// populated for nested TypeProtos.  If populated, they must capture all file
    /// dependencies related to the type and all of its descendants, in order
    /// be used for deserializing back to the ZetaSQL Type.  If they are not
    /// populated, then deserialization can still be done if the relevant
    /// FileDescriptorSets are provided to deserialization independent of this
    /// proto.  One FileDescriptorSet is created for every distinct DescriptorPool
    /// from which an enum or proto type contained within this type originates.
    /// For example, serializing a struct with two proto fields from different
    /// DescriptorPools would result in two FileDescriptorSets.
    #[prost(message, repeated, tag = "6")]
    pub file_descriptor_set: ::std::vec::Vec<::prost_types::FileDescriptorSet>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ArrayTypeProto {
    #[prost(message, optional, boxed, tag = "1")]
    pub element_type: ::std::option::Option<::std::boxed::Box<TypeProto>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StructFieldProto {
    #[prost(string, optional, tag = "1")]
    pub field_name: ::std::option::Option<std::string::String>,
    #[prost(message, optional, tag = "2")]
    pub field_type: ::std::option::Option<TypeProto>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StructTypeProto {
    #[prost(message, repeated, tag = "1")]
    pub field: ::std::vec::Vec<StructFieldProto>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ProtoTypeProto {
    /// The _full_ name of the proto.
    #[prost(string, optional, tag = "1")]
    pub proto_name: ::std::option::Option<std::string::String>,
    #[prost(string, optional, tag = "2")]
    pub proto_file_name: ::std::option::Option<std::string::String>,
    /// The index of the FileDescriptorSet in the top-level TypeProto that can be
    /// used to deserialize this particular ProtoType.
    #[prost(int32, optional, tag = "3", default = "0")]
    pub file_descriptor_set_index: ::std::option::Option<i32>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EnumTypeProto {
    /// The _full_ name of the enum.
    #[prost(string, optional, tag = "1")]
    pub enum_name: ::std::option::Option<std::string::String>,
    #[prost(string, optional, tag = "2")]
    pub enum_file_name: ::std::option::Option<std::string::String>,
    /// The index of the FileDescriptorSet in the top-level TypeProto that can be
    /// used to deserialize this particular EnumType.
    #[prost(int32, optional, tag = "3", default = "0")]
    pub file_descriptor_set_index: ::std::option::Option<i32>,
}
/// NEXT_ID: 25
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum TypeKind {
    /// User code that switches on this enum must have a default case so
    /// builds won't break if new enums get added.
    SwitchMustHaveADefault = -1,
    /// This can be used by consumers to record an unknown type.
    /// This is not used internally by ZetaSQL.
    /// Most functions that take TypeKind will fail on TYPE_UNKNOWN.
    TypeUnknown = 0,
    TypeInt32 = 1,
    TypeInt64 = 2,
    TypeUint32 = 3,
    TypeUint64 = 4,
    TypeBool = 5,
    TypeFloat = 6,
    TypeDouble = 7,
    TypeString = 8,
    TypeBytes = 9,
    TypeDate = 10,
    TypeTimestamp = 19,
    TypeEnum = 15,
    TypeArray = 16,
    TypeStruct = 17,
    TypeProto = 18,
    /// TIME and DATETIME is controlled by FEATURE_V_1_2_CIVIL_TIME
    TypeTime = 20,
    TypeDatetime = 21,
    /// GEOGRAPHY is controlled by FEATURE_GEOGRAPHY
    TypeGeography = 22,
    /// NUMERIC is controlled by FEATURE_NUMERIC_TYPE
    TypeNumeric = 23,
    /// BIGNUMERIC is controlled by FEATURE_BIGNUMERIC_TYPE
    TypeBignumeric = 24,
}
/// This proto is used as a absl::Status error payload to give the location
/// for SQL parsing and analysis errors.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ErrorLocation {
    /// 1-based <line> and <column> offset in the input SQL string.
    /// <column> may point to one character off the end of a line when the error
    /// occurs at end-of-line.
    ///
    /// NOTE: <line> is computed assuming lines can be split
    /// with \n, \r or \r\n, and <column> is computed assuming tabs
    /// expand to eight characters.
    #[prost(int32, optional, tag = "1", default = "1")]
    pub line: ::std::option::Option<i32>,
    #[prost(int32, optional, tag = "2", default = "1")]
    pub column: ::std::option::Option<i32>,
    /// An optional filename related to the error, if applicable.  May be used
    /// to express more general error source information than a filename, for
    /// instance if the error comes from a module imported from datascape.
    /// ErrorLocation typically gets formatted as 'filename:line:column', so
    /// this field content should make sense in this format (i.e., it should
    /// probably avoid whitespace).
    #[prost(string, optional, tag = "3")]
    pub filename: ::std::option::Option<std::string::String>,
    /// An optional list of error source information for the related Status.
    /// The last element in this list is the immediate error cause, with
    /// the previous element being its cause, etc.  These error sources should
    /// normally be displayed in reverse order.
    #[prost(message, repeated, tag = "4")]
    pub error_source: ::std::vec::Vec<ErrorSource>,
}
/// This proto indicates an error that is the source of another error.
/// It is expected that all of <error_message>, <error_message_caret_string>,
/// and <error_location> are populated in normal use cases.
///
/// An example of usage is for deferred, nested resolution of SQL expressions
/// related to SQL UDFs inside Modules.  Resolving a SQL UDF (func1) that
/// references another SQL UDF (func2) can cause nested resolution, and if the
/// resolution of func2 fails then that error information is captured in an
/// ErrorSource that is attached to the error Status returned by func1's
/// resolution.  The returned Status may also have its own ErrorLocation
/// related to func1, while the ErrorSource will be specific to func2's
/// resolution.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ErrorSource {
    /// The error message for this ErrorSource.
    #[prost(string, optional, tag = "1")]
    pub error_message: ::std::option::Option<std::string::String>,
    /// An additional error string added to <error_message> when in
    /// ErrorMessageMode ERROR_MESSAGE_MODE_MULTI_LINE_WITH_CARET.  This
    /// is constructed as one line of input text, a newline, and then a
    /// second line with the caret in the position pointing at the error
    /// in the first line.
    #[prost(string, optional, tag = "2")]
    pub error_message_caret_string: ::std::option::Option<std::string::String>,
    /// The error location indicating a position in the original input file
    /// containing the statement with the error.
    /// This <error_location> should not itself have <error_source> filled in.
    #[prost(message, optional, tag = "3")]
    pub error_location: ::std::option::Option<ErrorLocation>,
}
/// Contains information about a deprecation warning emitted by the
/// analyzer. Currently attached to any absl::Status returned by
/// AnalyzerOutput::deprecation_warnings().
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeprecationWarning {
    #[prost(enumeration = "deprecation_warning::Kind", optional, tag = "1")]
    pub kind: ::std::option::Option<i32>,
}
pub mod deprecation_warning {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Kind {
        /// User code that switches on this enum must have a default case so
        /// builds won't break if new enums get added.
        SwitchMustHaveADefault = -1,
        Unknown = 0,
        DeprecatedFunction = 1,
        DeprecatedFunctionSignature = 2,
        /// proto.has_<field>() is not well-defined if 'proto' comes from a file with
        /// proto3 syntax, but it is currently supported by the analyzer.
        Proto3FieldPresence = 3,
    }
}
/// A non-absl::Status-based representation of a deprecation warning that can be
/// stored in objects that can be stored in the resolved AST (e.g.,
/// FunctionSignatures).
///
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FreestandingDeprecationWarning {
    #[prost(string, optional, tag = "1")]
    pub message: ::std::option::Option<std::string::String>,
    #[prost(string, optional, tag = "2")]
    pub caret_string: ::std::option::Option<std::string::String>,
    #[prost(message, optional, tag = "3")]
    pub error_location: ::std::option::Option<ErrorLocation>,
    #[prost(message, optional, tag = "4")]
    pub deprecation_warning: ::std::option::Option<DeprecationWarning>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FunctionEnums {}
pub mod function_enums {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum ArgumentCardinality {
        Required = 0,
        /// occurs 0 or more times
        Repeated = 1,
        Optional = 2,
    }
    /// Function argument always has mode NOT_SET.
    /// Procedure argument is in one of the 3 modes:
    /// IN: argument is used only for input to the procedure. It is also the
    ///     default mode for procedure argument if no mode is specified.
    /// OUT: argument is used as output of the procedure.
    /// INOUT: argument is used both for input to and output from the procedure.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum ProcedureArgumentMode {
        NotSet = 0,
        In = 1,
        Out = 2,
        Inout = 3,
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum WindowOrderSupport {
        OrderUnsupported = 0,
        OrderOptional = 1,
        OrderRequired = 2,
    }
    /// A Function must have exactly one of the three modes: SCALAR, AGGREGATE,
    /// and ANALYTIC. It is not possible to select a mode based on overload
    /// resolution.
    /// 1) A SCALAR function cannot specify support for the OVER clause in
    ///    <function_options>.
    /// 2) An AGGREGATE function can specify support for the OVER clause in
    ///    <function_options>. For an AGGREGATE function with the support,
    ///    it acts as an analytic function if an OVER clause follows the function
    ///    call. Otherwise, it is treated as a regular aggregate function.
    /// 3) An ANALYTIC function must specify support for the OVER clause in
    ///    <function_options>. It cannot be used without OVER.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Mode {
        Scalar = 1,
        Aggregate = 2,
        Analytic = 3,
    }
    /// The volatility of a function determines how multiple executions of
    /// a function are related, and whether we always get the same answer when
    /// calling the function with the same input values.  Optimizers may use
    /// this property when considering transformations like common subexpression
    /// elimination.  Functions marked VOLATILE must be evaluated independently
    /// each time time they occur.
    /// This is based on postgres:
    /// http://www.postgresql.org/docs/9.4/static/xfunc-volatility.html
    ///
    /// Note that volatility is a property of a Function, not an expression.
    /// The function `+` is immutable, but in the expression "a + b", the
    /// column references do not have a volatility property, and neither does the
    /// expression.
    ///
    /// Functions like ANY_VALUE do not fit cleanly into this classification.
    /// ANY_VALUE is not required to be deterministic, but is allowed to be.
    /// Unlike RAND(), two calls to ANY_VALUE(x) are allowed to be combined by an
    /// optimizer so the result is shared.  Such functions are marked IMMUTABLE.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Volatility {
        /// Same answer always (for the same inputs).  e.g. 1+2
        Immutable = 0,
        // Optimizers can always reuse results for computing
        // this function on the same input values.
        /// Same answer within same statement (for the same inputs).
        Stable = 1,
        // e.g. CURRENT_TIMESTAMP()
        // Optimizers can always reuse results for computing
        // this function on the same input values
        // within the same statement.
        /// Each invocation is independent and may return a
        Volatile = 2,
    }
    /// This is an enumeration of all types of table-valued functions that
    /// ZetaSQL supports serializing and deserializing. It exists for use with
    /// the TableValuedFunction::RegisterDeserializer method to associate each TVF
    /// type with a callback to generate a new instance. Please see the comments
    /// for that method for more information.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum TableValuedFunctionType {
        Invalid = 0,
        FixedOutputSchemaTvf = 1,
        ForwardInputSchemaToOutputSchemaTvf = 2,
        TemplatedSqlTvf = 3,
        ForwardInputSchemaToOutputSchemaWithAppendedColumns = 7,
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SignatureArgumentKind {
    /// A specific concrete Type.  Each argument with ARG_TYPE_FIXED should include
    /// an instance of the Type object to indicate the exact type to use.
    ArgTypeFixed = 0,
    /// Templated type.  All arguments with this type must be the same type.
    /// For example,
    ///   IF <bool> THEN <T1> ELSE <T1> END -> <T1>
    ArgTypeAny1 = 1,
    /// Templated type.  All arguments with this type must be the same type.
    /// For example,
    ///   CASE <T1> WHEN <T1> THEN <T2>
    ///             WHEN <T1> THEN <T2> ELSE <T2> END -> <T2>
    ArgTypeAny2 = 2,
    /// Templated array type.  All arguments with this type must be the same
    /// type.  Additionally, all arguments with this type must be an array
    /// whose element type matches arguments with ARG_TYPE_ANY_1 type.
    /// For example,
    ///   FIRST(<array<T1>>) -> <T1>
    ArgArrayTypeAny1 = 3,
    /// Templated array type.  All arguments with this type must be the same
    /// type.  Additionally, all arguments with this type must be an array
    /// whose element type matches arguments with ARG_TYPE_ANY_2 type.
    /// For example,
    ///   LAST(<array<T2>>) -> <T2>
    ArgArrayTypeAny2 = 4,
    /// Templated proto type. All arguments with this type must be the same type.
    /// e.g.:
    ///   DEBUGSTRING(<proto>) -> <string>
    ArgProtoAny = 5,
    /// Templated struct type. All arguments with this type must be the same type.
    /// e.g.:
    ///   DEBUGSTRING(<struct>) -> <string>
    ArgStructAny = 6,
    /// Templated enum type. All arguments with this type must be the same type.
    /// e.g.:
    ///   ENUM_NAME(<enum>, 5) -> <string>
    ArgEnumAny = 7,
    /// Arbitrary Type. Multiple arguments with this type do not need to be the
    /// same type. This does not include relation arguments.
    ArgTypeArbitrary = 8,
    /// Relation type. This is only valid for table-valued functions (TVFs). This
    /// specifies a relation of any number and types of columns. Multiple arguments
    /// with this type do not necessarily represent the same relation.
    ///
    /// Background: each TVF may accept value or relation arguments. The signature
    /// specifies whether each argument should be a value or a relation. For a
    /// value argument, the signature may use one of the other
    /// SignatureArgumentKinds in this list.
    ///
    /// For more information, please see table_valued_function.h.
    ArgTypeRelation = 9,
    /// This is used for a non-existent return type for signatures that do not
    /// return a value.  This can only be used as a return type, and only in
    /// contexts where there is no return (e.g. Procedures, or signatures in
    /// ResolvedDropFunctionStmt).
    ArgTypeVoid = 10,
    /// Model type. This is only valid for table-valued functions (TVFs). This
    /// specifies a model for ML-related TVFs.
    /// For more information, please see TVFModelArgument in
    /// table_valued_function.h.
    ArgTypeModel = 11,
    /// Connection type. This is only valid for table-valued functions (TVFs). This
    /// specifies a connection for EXTERNAL_QUERY TVF.
    /// For more information, please see TVFConnectionArgument in
    /// table_valued_function.h.
    ArgTypeConnection = 12,
    /// Descriptor type. This is only valid for table-valued functions (TVFs). This
    /// specifies a descriptor with a list of column names.
    /// For more information, please see TVFDescriptorArgument in
    /// table_valued_function.h.
    ArgTypeDescriptor = 13,
    SwitchMustHaveADefault = -1,
}
/// Annotations for LanguageFeature enum values. Only for consumption by
/// ZetaSQL code.
///
/// LanguageOptions::EnableMaximumLanguageFeatures() enables all features with
/// 'ideally_enabled == true' and 'in_development == false'.
///
/// LanguageOptions::EnableMaximumLanguageFeaturesForDevelopment() enables all
/// features with 'ideally_enabled == true'.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LanguageFeatureOptions {
    /// Indicates whether a feature is enabled in the idealized ZetaSQL. (One
    /// reason to disable a feature is if it exists only to support backwards
    /// compatibility with older ZetaSQL behavior.)
    #[prost(bool, optional, tag = "1", default = "true")]
    pub ideally_enabled: ::std::option::Option<bool>,
    /// Indicates whether a feature is still undergoing development. Users should
    /// not enable features that are still in development, but internal ZetaSQL
    /// tests may do so.
    #[prost(bool, optional, tag = "2", default = "false")]
    pub in_development: ::std::option::Option<bool>,
}
/// ZetaSQL language versions.
/// See (broken link) for more detail.
///
/// A language version defines a stable set of features and required semantics.
/// LanguageVersion VERSION_x_y implicitly includes the LanguageFeatures below
/// named FEATURE_V_x_y_*.
///
/// The features and behavior supported by an engine can be expressed as a
/// LanguageVersion plus a set of LanguageFeatures added on top of that version.
///
/// New version numbers will be introduced periodically, and will normally
/// include the new features that have been specified up to that point.
/// Engines should move their version number forwards over time rather than
/// accumulating large sets of LanguageFeatures.
///
/// LINT: LEGACY_NAMES
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum LanguageVersion {
    /// All current features, including features that are not part of a frozen
    /// version.  For example, when v1.0 is the maximum released version number,
    /// this includes features that have been developed for v1.1.
    /// This does not include cross-version or experimental features.
    /// WARNING: Using this version means query behavior will change under you.
    VersionCurrent = 1,
    /// Version 1.0, frozen January 2015.
    Version10 = 10000,
    /// Version 1.1, frozen February 2017.
    Version11 = 11000,
    /// Version 1.2, frozen January 2018.
    Version12 = 12000,
    /// Version 1.3.  New features are being added here.
    Version13 = 13000,
    /// User code that switches on this enum must have a default case so
    /// builds won't break if new enums get added.
    SwitchMustHaveADefault = -1,
}
/// The list of optional features that engines may or may not support.
/// Features can be opted into in AnalyzerOptions.
///
/// There are three types of LanguageFeatures.
/// * Cross-version - Optional features that can be enabled orthogonally to
///                   versioning.  Some engines will never implement these
///                   features, and zetasql code will always support this
///                   switch.
/// * Versioned - Features that describe behavior changes adopted as of some
///               language version.  Eventually, all engines should support these
///               features, and switches in the zetasql code (and tests)
///               should eventually be removed.
///               All of these, and only these, show up in VERSION_CURRENT.
/// * Experimental - Features not currently part of any language version.
///
/// All optional features are off by default.  Some features have a negative
/// meaning, so turning them on will remove a feature or enable an error.
///
/// CROSS-VERSION FEATURES
///
/// These are features that can be opted into independently from versioning.
/// Some features may disable operations that are normally allowed by default.
///
/// These features should not change semantics, other than whether a feature
/// is allowed or not.  Versioned options may further the behavior of these
/// features, if they are enabled.  For example, an engine may choose to
/// support DML, orthogonally to versioning.  If it supports DML, and
/// specified semantics for DML may change over time, and the engine may use
/// versioned options to choose DML behavior as of v1.0 or v1.1.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum LanguageFeature {
    /// Enable analytic functions.
    /// See (broken link).
    FeatureAnalyticFunctions = 1,
    /// Enable the TABLESAMPLE clause on scans.
    /// See (broken link).
    FeatureTablesample = 2,
    /// If enabled, give an error on GROUP BY, DISTINCT or set operations (other
    /// than UNION ALL) on floating point types. This feature is disabled in the
    /// idealized ZetaSQL (i.e. LanguageOptions::EnableMaximumLanguageFeatures)
    /// because enabling it turns off support for a feature that is normally on by
    /// default.
    FeatureDisallowGroupByFloat = 3,
    /// If enabled, treats TIMESTAMP literal as 9 digits (nanos) precision.
    /// Otherwise TIMESTAMP has 6 digits (micros) precision.
    /// In general, a TIMESTAMP value has only 6 digits precision. This feature
    /// will only affect how a timestamp literal string is interpreted into a
    /// TIMESTAMP value. If enabled, a timestamp literal string can have up to 9
    /// digits of subseconds(nanos). Otherwise, it can only have up to 6 digits of
    /// subseconds (micros). 9 digits subsecond literal is not a valid timestamp
    /// string in the later case.
    FeatureTimestampNanos = 5,
    /// Enable support for JOINs in UPDATE statements, see
    /// (broken link).
    FeatureDmlUpdateWithJoin = 6,
    /// Enable table-valued functions. For more information, see
    /// table_valued_functions.h.
    FeatureTableValuedFunctions = 8,
    /// This enables support for CREATE AGGREGATE FUNCTION.
    FeatureCreateAggregateFunction = 9,
    /// This enables support for CREATE TABLE FUNCTION.
    /// For more information, see (broken link).
    FeatureCreateTableFunction = 10,
    /// This enables support for GROUP BY ROLLUP.
    FeatureGroupByRollup = 12,
    /// This enables support for creating and calling functions with templated
    /// argument types, using CREATE FUNCTION, CREATE AGGREGATE FUNCTION, or CREATE
    /// TABLE FUNCTION statements. For example, a function argument may be written
    /// as "argument ANY TYPE" to match against any scalar value. For more
    /// information, see (broken link).
    FeatureTemplateFunctions = 13,
    /// Enables support for PARTITION BY with CREATE TABLE and CREATE TABLE AS.
    /// See (broken link).
    FeatureCreateTablePartitionBy = 14,
    /// Enables support for CLUSTER BY with CREATE TABLE and CREATE TABLE AS.
    /// See (broken link).
    FeatureCreateTableClusterBy = 15,
    /// NUMERIC type support, see (broken link).
    FeatureNumericType = 16,
    /// Enables support for NOT NULL annotation in CREATE TABLE.
    /// See comment on FEATURE_CREATE_TABLE_FIELD_ANNOTATIONS and
    /// (broken link) for details.
    FeatureCreateTableNotNull = 17,
    /// Enables support for annotations (e.g., NOT NULL and OPTIONS()) for struct
    /// fields and array elements in CREATE TABLE.
    /// Does not affect table options or table column annotations.
    ///
    /// Example: Among the following queries
    /// Q1: CREATE TABLE t (c STRUCT<a INT64> NOT NULL)
    /// Q2: CREATE TABLE t (c STRUCT<a INT64 NOT NULL>)
    /// Q3: CREATE TABLE t (c STRUCT<a INT64> OPTIONS(foo=1))
    /// Q4: CREATE TABLE t (c STRUCT<a INT64 OPTIONS(foo=1)>)
    /// Q5: CREATE TABLE t (c STRUCT<a INT64 NOT NULL OPTIONS(foo=1)>)
    ///
    /// Allowed queries                  FEATURE_CREATE_TABLE_FIELD_ANNOTATIONS
    ///                                         =0               =1
    /// FEATURE_CREATE_TABLE_NOT_NULL=0        {Q3}           {Q3, Q4}
    /// FEATURE_CREATE_TABLE_NOT_NULL=1      {Q1, Q3}    {Q1, Q2, Q3, Q4, Q5}
    ///
    /// See (broken link).
    FeatureCreateTableFieldAnnotations = 18,
    /// Enables support for column definition list in CREATE TABLE AS SELECT.
    /// Example: CREATE TABLE t (x FLOAT64) AS SELECT 1 x
    /// The features in the column definition list are controlled by
    /// FEATURE_CREATE_TABLE_NOT_NULL and FEATURE_CREATE_TABLE_FIELD_ANNOTATIONS.
    FeatureCreateTableAsSelectColumnList = 19,
    /// Indicates that an engine that supports primary keys does not allow any
    /// primary key column to be NULL. Similarly, non-NULL primary key columns
    /// cannot have any NULL array elements or struct/proto fields anywhere inside
    /// them.
    ///
    /// Only interpreted by the compliance tests and the reference implementation
    /// (not the analyzer). It exists so that engines can disable tests for this
    /// atypical behavior without impacting their compliance ratios. It can never
    /// be totally enforced in the analyzer because the analyzer cannot evaluate
    /// expressions.
    ///
    /// TODO: When this feature is enabled, the reference implementation
    /// forbids NULL primary key columns, but it allows NULL array elements and
    /// NULL struct/proto fields. Change this behavior if we ever want to write
    /// compliance tests for these cases.
    FeatureDisallowNullPrimaryKeys = 20,
    /// Indicates that an engine that supports primary keys does not allow any
    /// primary key column to be modified with UPDATE.
    ///
    /// Only interpreted by the compliance tests and the reference implementation
    /// (not the analyzer) for now. It exists so that engines can disable tests for
    /// this atypical behavior without impacting their compliance ratios.
    ///
    /// TODO: Consider exposing information about primary keys to the
    /// analyzer and enforcing this feature there.
    FeatureDisallowPrimaryKeyUpdates = 21,
    /// Enables support for the TABLESAMPLE clause applied to table-valued function
    /// calls. For more information about table-valued functions, please see
    /// table_valued_functions.h and (broken link).
    FeatureTablesampleFromTableValuedFunctions = 22,
    /// Enable encryption- and decryption-related functions.
    /// See (broken link).
    FeatureEncryption = 23,
    /// Geography type support per (broken link)
    FeatureGeography = 25,
    /// Enables support for stratified TABLESAMPLE. For more information about
    /// stratified sampling, please see: (broken link).
    FeatureStratifiedReservoirTablesample = 26,
    /// Enables foreign keys (see (broken link)).
    FeatureForeignKeys = 27,
    /// Enables BETWEEN function signatures for UINT64/INT64 comparisons.
    FeatureBetweenUint64Int64 = 28,
    /// Enables check constraint (see (broken link)).
    FeatureCheckConstraint = 29,
    /// Enables statement parameters in the GRANTEE list of GRANT, REVOKE, CREATE
    /// ROW POLICY, and ALTER ROW POLICY statements.
    /// TODO: The behavior of this feature is intended to become
    /// mandatory.  This is a temporary feature, that preserves existing
    /// behavior prior to engine migrations.  Once all engines have migrated,
    /// this feature will be deprecated/removed and the new behavior will be
    /// mandatory.
    FeatureParametersInGranteeList = 30,
    /// Enables support for named arguments in function calls using a syntax like
    /// this: 'SELECT function(argname => 'value', otherarg => 42)'. Function
    /// arguments with associated names in the signature options may specify values
    /// by providing the argument name followed by an equals sign and greater than
    /// sign (=>) followed by a value for the argument. Function calls may include
    /// a mix of positional arguments and named arguments. The resolver will
    /// compare provided arguments against function signatures and handle signature
    /// matching appropriately. For more information, please refer to
    /// (broken link).
    FeatureNamedArguments = 31,
    /// Enables support for the old syntax for the DDL for ROW ACCESS POLICY,
    /// previously called ROW POLICY.
    /// When this feature is enabled, using the old syntax will not throw errors
    /// for CREATE ROW [ACCESS] POLICY and DROP ALL ROW [ACCESS] POLICIES, but the
    /// new syntax can still be used.
    /// When it is not enabled, the new syntax must be used for CREATE
    /// ROW ACCESS POLICY and DROP ALL ROW ACCESS POLICIES.
    /// The new syntax is always required for ALTER ROW ACCESS POLICY and DROP ROW
    /// ACCESS POLICY: at the time of this writing, these statements are new/not in
    /// use.
    /// This is a temporary feature that preserves legacy engine behavior that
    /// will be deprecated, and the new syntax will become mandatory (b/135116351).
    /// For more details on syntax changes, see (broken link).
    FeatureAllowLegacyRowAccessPolicySyntax = 32,
    /// Enables support for PARTITION BY with CREATE MATERIALIZED VIEW.
    /// See (broken link).
    FeatureCreateMaterializedViewPartitionBy = 33,
    /// Enables support for CLUSTER BY with CREATE MATERIALIZED VIEW.
    /// See (broken link).
    FeatureCreateMaterializedViewClusterBy = 34,
    /// Enables support for column definition list in CREATE EXTERNAL TABLE.
    /// Example: CREATE EXTERNAL TABLE t (x FLOAT64)
    FeatureCreateExternalTableWithTableElementList = 35,
    /// Enables support for PARTITION BY in CREATE EXTERNAL TABLE.
    /// Example: CREATE EXTERNAL TABLE t (x FLOAT64) PARTITION BY X OPTIONS()
    /// Make sure the feature FEATURE_CREATE_EXTERNAL_TABLE_WITH_TABLE_ELEMENT_LIST
    /// is enabled to support column lookup
    FeatureCreateExternalTableWithPartitionBy = 36,
    /// Enables support for CLUSTER BY in CREATE EXTERNAL TABLE.
    /// Example:
    /// CREATE EXTERNAL TABLE t (x FLOAT64) PARTITION BY X CLUSTER BY X OPTIONS()
    /// Make sure the feature FEATURE_CREATE_EXTERNAL_TABLE_WITH_TABLE_ELEMENT_LIST
    /// is enabled to support column lookup.
    FeatureCreateExternalTableWithClusterBy = 37,
    /// Enables support for NUMERIC data type as input in the binary statistics
    /// functions.
    FeatureNumericCovarCorrSignatures = 38,
    /// Enables support for NUMERIC data type as input in the unary statistics
    /// functions.
    FeatureNumericVarianceStddevSignatures = 39,
    /// Enables using NOT ENFORCED in primary keys.
    /// See (broken link).
    FeatureUnenforcedPrimaryKeys = 40,
    /// BIGNUMERIC data type. (broken link)
    FeatureBignumericType = 41,
    // -> Add more cross-version features here.
    // -> DO NOT add more versioned features into versions that are frozen.
    //    New features should be added for the *next* version number.

    // VERSIONED FEATURES
    // These are features or changes as of some version.
    // Each should start with a prefix FEATURE_V_x_y_.  The feature will be
    // included in the default set enabled for LanguageVersion VERSION_x_y.
    //
    // Features that will remain optional for compliance, and are not expected to
    // be implemented in all engines, should be added as cross-version features
    // (above) instead.
    //
    // Some versioned features may have dependencies and only make sense if
    // other features are also enabled.  Dependencies should be commented here.

    // Version 1.1 features
    // * Frozen in February 2017.
    // * Do not add new features here.
    /// Enable ORDER BY COLLATE.  See (broken link).
    FeatureV11OrderByCollate = 11001,
    /// Enable WITH clause on subqueries.  Without this, WITH is allowed
    /// only at the top level.  The WITH subqueries still cannot be
    /// correlated subqueries.
    FeatureV11WithOnSubquery = 11002,
    /// Enable the SELECT * EXCEPT and SELECT * REPLACE features.
    /// See (broken link).
    FeatureV11SelectStarExceptReplace = 11003,
    /// Enable the ORDER BY in aggregate functions.
    /// See (broken link)
    FeatureV11OrderByInAggregate = 11004,
    /// Enable casting between different array types.
    FeatureV11CastDifferentArrayTypes = 11005,
    /// Enable comparing array equality.
    FeatureV11ArrayEquality = 11006,
    /// Enable LIMIT in aggregate functions.
    FeatureV11LimitInAggregate = 11007,
    /// Enable HAVING modifier in aggregate functions.
    FeatureV11HavingInAggregate = 11008,
    /// Enable IGNORE/RESPECT NULLS modifier in analytic functions.
    FeatureV11NullHandlingModifierInAnalytic = 11009,
    /// Enable IGNORE/RESPECT NULLS modifier in aggregate functions.
    FeatureV11NullHandlingModifierInAggregate = 11010,
    /// Enable FOR SYSTEM_TIME AS OF (time travel).
    /// See (broken link).
    FeatureV11ForSystemTimeAsOf = 11011,
    // Version 1.2 features
    // * Frozen in January 2018.
    // * Do not add new features here.
    /// Enable TIME and DATETIME types and related functions.
    FeatureV12CivilTime = 12001,
    /// Enable SAFE mode function calls.  e.g. SAFE.FUNC(...) for FUNC(...).
    /// (broken link).
    FeatureV12SafeFunctionCall = 12002,
    /// Enable support for GROUP BY STRUCT.
    FeatureV12GroupByStruct = 12003,
    /// Enable use of proto extensions with NEW.
    FeatureV12ProtoExtensionsWithNew = 12004,
    /// Enable support for GROUP BY ARRAY.
    FeatureV12GroupByArray = 12005,
    /// Enable use of proto extensions with UPDATE ... SET.
    FeatureV12ProtoExtensionsWithSet = 12006,
    /// Allows nested DML statements to refer to names defined in the parent
    /// scopes. Without this, a nested DML statement can only refer to names
    /// created in the local statement - i.e. the array element.
    /// Examples that are allowed only with this option:
    ///   UPDATE Table t SET (UPDATE t.ArrayColumn elem SET elem = t.OtherColumn)
    ///   UPDATE Table t SET (DELETE t.ArrayColumn elem WHERE elem = t.OtherColumn)
    ///   UPDATE Table t SET (INSERT t.ArrayColumn VALUES (t.OtherColumn))
    ///   UPDATE Table t SET (INSERT t.ArrayColumn SELECT t.OtherColumn)
    FeatureV12CorrelatedRefsInNestedDml = 12007,
    /// Enable use of WEEK(<Weekday>) with the functions that support it.
    FeatureV12WeekWithWeekday = 12008,
    /// Enable use of array element [] syntax in targets with UPDATE ... SET.
    /// For example, allow UPDATE T SET a.b[OFFSET(0)].c = 5.
    FeatureV12ArrayElementsWithSet = 12009,
    /// Enable nested updates/deletes of the form
    /// UPDATE/DELETE ... WITH OFFSET AS ... .
    FeatureV12NestedUpdateDeleteWithOffset = 12010,
    /// Enable Generated Columns on CREATE and ALTER TABLE statements.
    /// See (broken link).
    FeatureV12GeneratedColumns = 12011,
    // Version 1.3 features

    // -> Add more versioned features here.
    // -> Always update AnalyzerOptions::GetLanguageFeaturesForVersion
    //    in ../common/language_options.cc.
    /// Enables support for the PROTO_DEFAULT_IF_NULL() function, see
    /// (broken link)
    FeatureV13ProtoDefaultIfNull = 13001,
    /// Enables support for proto field pseudo-accessors in the EXTRACT function.
    /// For example, EXTRACT(FIELD(x) from foo) will extract the value of the field
    /// x defined in message foo. EXTRACT(HAS(x) from foo) will return a boolean
    /// denoting if x is set in foo or NULL if foo is NULL. EXTRACT(RAW(x) from
    /// foo) will get the value of x on the wire (i.e., without applying any
    /// FieldFormat.Format annotations or automatic conversions). If the field is
    /// missing, the default is always returned, which is NULL for message fields
    /// and the field default (either the explicit default or the default default)
    /// for primitive fields. If the containing message is NULL, NULL is returned.
    /// (broken link)
    FeatureV13ExtractFromProto = 13002,
    /// If enabled, the analyzer will return an error when attempting to check
    /// if a proto3 scalar field has been explicitly set (e.g.,
    /// proto3.has_scalar_field and EXTRACT(HAS(scalar_field) from proto3)). This
    /// is not allowed because proto3 does not generate has_ accessors for scalar
    /// fields.
    FeatureV13DisallowProto3HasScalarField = 13003,
    /// Enable array ordering (and non-equality comparisons).  This enables
    /// arrays in the ORDER BY of a query, as well as in aggregate and analytic
    /// function arguments.  Also enables inequality comparisons between arrays
    /// (<, <=, >, >=).  This does not enable arrays for MIN/MAX/GREATEST/LEAST,
    /// since the semantics are not well defined over sets of arrays.
    ///
    /// Spec:  (broken link)
    FeatureV13ArrayOrdering = 13004,
    /// Allow omitting column and value lists in INSERT statement and INSERT clause
    /// of MERGE statement.
    /// Spec: (broken link)
    FeatureV13OmitInsertColumnList = 13005,
    /// If enabled, the 'use_defaults' and 'use_field_defaults' annotations are
    /// ignored for proto3 scalar fields. This results in the default value always
    /// being returned for proto3 scalar fields that are not explicitly set,
    /// including when they are annotated with 'use_defaults=false' or their parent
    /// message is annotated with 'use_field_defaults=false'. This aligns with
    /// proto3 semantics as proto3 does not expose whether scalar fields are set or
    /// not.
    FeatureV13IgnoreProto3UseDefaults = 13006,
    /// Enables support for the REPLACE_FIELDS() function. REPLACE_FIELDS(p,
    /// <value> AS <field_path>) returns the proto obtained by setting p.field_path
    /// = value. If value is NULL, this unsets field_path or returns an error if
    /// the last component of field_path is a required field. Multiple fields can
    /// be modified: REPLACE_FIELDS(p, <value_1> AS <field_path_1>, ..., <value_n>
    /// AS <field_path_n>). REPLACE_FIELDS() can also be used to modify structs
    /// using the similar syntax: REPLACE_FIELDS(s, <value> AS
    /// <struct_field_path>).
    /// (broken link)
    FeatureV13ReplaceFields = 13007,
    /// Enable NULLS FIRST/NULLS LAST in ORDER BY expressions.
    FeatureV13NullsFirstLastInOrderBy = 13008,
    /// Allows dashes in the first part of multi-part table name. This is to
    /// accommodate GCP project names which use dashes instead of underscores, e.g.
    /// crafty-tractor-287. So fully qualified table name which includes project
    /// name normally has to be quoted in the query, i.e. SELECT * FROM
    /// `crafty-tractor-287`.dataset.table This feature allows it to be used
    /// unquoted, i.e. SELECT * FROM crafty-tractor-287.dataset.table
    FeatureV13AllowDashesInTableName = 13009,
    /// CONCAT allows arguments of different types, automatically coerced to
    /// STRING for FN_CONCAT_STRING signature. Only types which have CAST to
    /// STRING defined are allowed, and BYTES is explicitly excluded (since BYTES
    /// should match FN_CONCAT_BYTES signature).
    FeatureV13ConcatMixedTypes = 13010,
    // EXPERIMENTAL FEATURES
    // These are features supported in the code that are not currently part of
    // officially supported ZetaSQL as of any version.
    //
    // An example:
    // FEATURE_EXPERIMENTAL_DECIMAL_DATA_TYPES = 999001;
    //
    // -> Add more experimental features here.
    //
    // TODO: Retire the convention of FEATURE_EXPERIMENTAL_xxx features
    // and just use the enum value annotations where appropriate.
    /// Enable ZetaSQL MODULES.  For an engine to fully opt into this feature,
    /// they must enable this feature flag and add support for the related
    /// StatementKinds: RESOLVED_IMPORT_STMT and RESOLVED_MODULE_STMT.
    /// See (broken link)
    FeatureExperimentalModules = 999002,
    /// These are not real features. They are just for unit testing the handling of
    /// various LanguageFeatureOptions.
    FeatureTestIdeallyEnabledButInDevelopment = 999991,
    FeatureTestIdeallyDisabled = 999992,
    FeatureTestIdeallyDisabledAndInDevelopment = 999993,
    /// User code that switches on this enum must have a default case so
    /// builds won't break if new enums get added.
    SwitchMustHaveADefault = -1,
}
/// This can be used to select strict name resolution mode.
///
/// In strict mode, implicit column names cannot be used unqualified.
/// This ensures that existing queries will not be broken if additional
/// elements are added to the schema in the future.
///
/// For example,
///   SELECT c1, c2 FROM table1, table2;
/// is not legal in strict mode because another column could be added to one of
/// these tables, making the query ambiguous.  The query must be written
/// with aliases in strict mode:
///   SELECT t1.c1, t2.c2 FROM table1 t1, table t2;
///
/// SELECT * is also not allowed in strict mode because the number of output
/// columns may change.
///
/// See (broken link) for full details.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum NameResolutionMode {
    NameResolutionDefault = 0,
    NameResolutionStrict = 1,
}
/// This identifies whether ZetaSQL works in INTERNAL (inside Google) mode,
/// or in EXTERNAL (exposed to non-Googlers in the products such as Cloud).
/// See (broken link) for details.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ProductMode {
    ProductInternal = 0,
    ProductExternal = 1,
}
/// This identifies whether statements are resolved in module context (i.e.,
/// as a statement contained in a module), or in normal/default context
/// (outside of a module).
/// See (broken link) for details about module context.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum StatementContext {
    ContextDefault = 0,
    ContextModule = 1,
}
/// Mode describing how errors should be constructed in the returned
/// absl::Status.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ErrorMessageMode {
    /// The error string does not contain a location.
    /// An ErrorLocation proto will be attached to the absl::Status with
    /// a location, when applicable.
    /// See error_helpers.h for working with these payloads.
    /// See error_location.proto for how line and column are defined.
    ErrorMessageWithPayload = 0,
    /// The error string contains a suffix " [at <line>:<column>]" when an
    /// error location is available.
    ErrorMessageOneLine = 1,
    /// The error string matches ERROR_MESSAGE_ONE_LINE, and also contains
    /// a second line with a substring of the input query, and a third line
    /// with a caret ("^") pointing at the error location above.
    ErrorMessageMultiLineWithCaret = 2,
}
/// Mode describing how parameters are defined and referenced.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ParameterMode {
    /// Parameters are defined by name (the default) and referenced using the
    /// syntax @param_name.
    ParameterNamed = 0,
    /// Parameters are defined positionally and referenced with ?. For example, if
    /// two parameters are bound, the first occurrence of ? in the query string
    /// refers to the first parameter and the second occurrence to the second
    /// parameter.
    ParameterPositional = 1,
    /// No parameters are allowed in the query.
    ParameterNone = 2,
}
/// This message stores the start and end byte offsets of a parsed string. It
/// also (optionally) stores the name of the file where this parsed string is
/// located.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ParseLocationRangeProto {
    /// If present, the name of the file containing the parsed string. It is
    /// expected that the start and end of a parsed string would both be located in
    /// just one file, so we have only one field to store the filename.
    #[prost(string, optional, tag = "1")]
    pub filename: ::std::option::Option<std::string::String>,
    /// Byte offset of the first character of the symbol string.
    #[prost(int32, optional, tag = "2")]
    pub start: ::std::option::Option<i32>,
    /// Byte offset of the character after the last character of the symbol string.
    #[prost(int32, optional, tag = "3")]
    pub end: ::std::option::Option<i32>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ParseResumeLocationProto {
    /// The filename associated with the input string (if any).
    #[prost(string, optional, tag = "4")]
    pub filename: ::std::option::Option<std::string::String>,
    /// The input string.
    #[prost(string, optional, tag = "1")]
    pub input: ::std::option::Option<std::string::String>,
    /// The current byte position, the parser will resume from this position in the
    /// next round.
    #[prost(int32, optional, tag = "2")]
    pub byte_position: ::std::option::Option<i32>,
    #[prost(bool, optional, tag = "3")]
    pub allow_resume: ::std::option::Option<bool>,
}
/// ValueProto represents the serialized form of the zetasql::Value.
/// Unlike zetasql::Value, ValueProto does not carry full type information with
/// every instance, and therefore can only be fully interpreted with accompanying
/// TypeProto.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ValueProto {
    /// Each non-null value will have exactly one of these fields specified.
    /// Null values will have no fields set.
    #[prost(
        oneof = "value_proto::Value",
        tags = "1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 255"
    )]
    pub value: ::std::option::Option<value_proto::Value>,
}
pub mod value_proto {
    /// An ordered collection of elements of arbitrary count.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Array {
        #[prost(message, repeated, tag = "1")]
        pub element: ::std::vec::Vec<super::ValueProto>,
    }
    /// A collection of fields. The count, order, and type of the fields is
    /// determined by the type associated with this value.
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Struct {
        #[prost(message, repeated, tag = "1")]
        pub field: ::std::vec::Vec<super::ValueProto>,
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Datetime {
        /// Represents bit field encoding of year/month/day/hour/minute/second.
        /// See class DatetimeValue in civil_time.h for details of encoding.
        #[prost(int64, optional, tag = "1")]
        pub bit_field_datetime_seconds: ::std::option::Option<i64>,
        /// Non-negative fractions of a second at nanosecond resolution.
        #[prost(int32, optional, tag = "2")]
        pub nanos: ::std::option::Option<i32>,
    }
    /// Each non-null value will have exactly one of these fields specified.
    /// Null values will have no fields set.
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Value {
        #[prost(int32, tag = "1")]
        Int32Value(i32),
        #[prost(int64, tag = "2")]
        Int64Value(i64),
        #[prost(uint32, tag = "3")]
        Uint32Value(u32),
        #[prost(uint64, tag = "4")]
        Uint64Value(u64),
        #[prost(bool, tag = "5")]
        BoolValue(bool),
        #[prost(float, tag = "6")]
        FloatValue(f32),
        #[prost(double, tag = "7")]
        DoubleValue(f64),
        #[prost(string, tag = "8")]
        StringValue(std::string::String),
        #[prost(bytes, tag = "9")]
        BytesValue(std::vec::Vec<u8>),
        #[prost(int32, tag = "10")]
        DateValue(i32),
        /// Tag 11 was used for specifying micros timestamps as int64, now obsolete.
        #[prost(int32, tag = "12")]
        EnumValue(i32),
        #[prost(message, tag = "13")]
        ArrayValue(Array),
        #[prost(message, tag = "14")]
        StructValue(Struct),
        /// Stores a serialized protocol message.
        #[prost(bytes, tag = "15")]
        ProtoValue(std::vec::Vec<u8>),
        #[prost(message, tag = "16")]
        TimestampValue(::prost_types::Timestamp),
        #[prost(message, tag = "17")]
        DatetimeValue(Datetime),
        /// Bit field encoding of hour/minute/second/nanos. See TimeValue class for
        /// details.
        #[prost(int64, tag = "18")]
        TimeValue(i64),
        /// Geography encoded using ::stlib::STGeographyEncoder
        #[prost(bytes, tag = "19")]
        GeographyValue(std::vec::Vec<u8>),
        /// Encoded numeric value. For the encoding format see documentation for
        /// NumericValue::SerializeAsProtoBytes().
        #[prost(bytes, tag = "20")]
        NumericValue(std::vec::Vec<u8>),
        /// Encoded bignumeric value. For the encoding format see documentation for
        /// BigNumericValue::SerializeAsProtoBytes().
        #[prost(bytes, tag = "21")]
        BignumericValue(std::vec::Vec<u8>),
        /// User code that switches on this oneoff enum must have a default case so
        /// builds won't break when new fields are added.
        #[prost(bool, tag = "255")]
        ValueProtoSwitchMustHaveADefault(bool),
    }
}
/// Reference to a ResolvedColumn.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedColumnProto {
    #[prost(int64, optional, tag = "1")]
    pub column_id: ::std::option::Option<i64>,
    #[prost(string, optional, tag = "2")]
    pub table_name: ::std::option::Option<std::string::String>,
    #[prost(string, optional, tag = "3")]
    pub name: ::std::option::Option<std::string::String>,
    #[prost(message, optional, tag = "4")]
    pub r#type: ::std::option::Option<TypeProto>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ValueWithTypeProto {
    #[prost(message, optional, tag = "1")]
    pub r#type: ::std::option::Option<TypeProto>,
    #[prost(message, optional, tag = "2")]
    pub value: ::std::option::Option<ValueProto>,
}
/// Reference to a table.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableRefProto {
    #[prost(string, optional, tag = "1")]
    pub name: ::std::option::Option<std::string::String>,
    #[prost(int64, optional, tag = "2")]
    pub serialization_id: ::std::option::Option<i64>,
    #[prost(string, optional, tag = "3")]
    pub full_name: ::std::option::Option<std::string::String>,
}
/// Reference to a model.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ModelRefProto {
    #[prost(string, optional, tag = "1")]
    pub name: ::std::option::Option<std::string::String>,
    #[prost(int64, optional, tag = "2")]
    pub serialization_id: ::std::option::Option<i64>,
    #[prost(string, optional, tag = "3")]
    pub full_name: ::std::option::Option<std::string::String>,
}
/// Reference to a connection
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ConnectionRefProto {
    #[prost(string, optional, tag = "1")]
    pub name: ::std::option::Option<std::string::String>,
    #[prost(string, optional, tag = "3")]
    pub full_name: ::std::option::Option<std::string::String>,
}
/// Reference to a named constant.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ConstantRefProto {
    /// Full name of the function, e.g., catalog1.catalog2.Constant.
    #[prost(string, optional, tag = "1")]
    pub name: ::std::option::Option<std::string::String>,
}
/// Reference to a function.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FunctionRefProto {
    /// TODO: Optimize this by generating unique serialization IDs.
    /// Full name of the function, e.g., group:pathname.
    #[prost(string, optional, tag = "1")]
    pub name: ::std::option::Option<std::string::String>,
}
/// Reference to a table-valued function.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableValuedFunctionRefProto {
    /// Full name of the function, e.g., group:pathname.
    #[prost(string, optional, tag = "1")]
    pub name: ::std::option::Option<std::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedNodeProto {
    /// Parse location range if present in the ResolvedNode.
    #[prost(message, optional, tag = "1")]
    pub parse_location_range: ::std::option::Option<ParseLocationRangeProto>,
}
/// Reference to a proto field descriptor.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FieldDescriptorRefProto {
    #[prost(message, optional, tag = "1")]
    pub containing_proto: ::std::option::Option<ProtoTypeProto>,
    #[prost(int32, optional, tag = "2")]
    pub number: ::std::option::Option<i32>,
}
/// Reference to a proto2::OneofDescriptor, which describes the fields of an
/// Oneof.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OneofDescriptorRefProto {
    #[prost(message, optional, tag = "1")]
    pub containing_proto: ::std::option::Option<ProtoTypeProto>,
    /// 0-based offset which aligns with the order Oneof fields are defined in the
    /// containing message.
    #[prost(int32, optional, tag = "2")]
    pub index: ::std::option::Option<i32>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ProcedureRefProto {
    #[prost(string, optional, tag = "1")]
    pub name: ::std::option::Option<std::string::String>,
}
// Wire format of Function related messages, these shouldn't be exposed to end
// users normally.

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TvfRelationColumnProto {
    #[prost(string, optional, tag = "1")]
    pub name: ::std::option::Option<std::string::String>,
    #[prost(message, optional, tag = "2")]
    pub r#type: ::std::option::Option<TypeProto>,
    #[prost(bool, optional, tag = "3")]
    pub is_pseudo_column: ::std::option::Option<bool>,
    /// Store the parse location ranges for column name and type.
    #[prost(message, optional, tag = "4")]
    pub name_parse_location_range: ::std::option::Option<ParseLocationRangeProto>,
    #[prost(message, optional, tag = "5")]
    pub type_parse_location_range: ::std::option::Option<ParseLocationRangeProto>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TvfRelationProto {
    #[prost(message, repeated, tag = "1")]
    pub column: ::std::vec::Vec<TvfRelationColumnProto>,
    #[prost(bool, optional, tag = "2", default = "false")]
    pub is_value_table: ::std::option::Option<bool>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TvfModelProto {
    #[prost(string, optional, tag = "1")]
    pub name: ::std::option::Option<std::string::String>,
    #[prost(string, optional, tag = "2")]
    pub full_name: ::std::option::Option<std::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TvfConnectionProto {
    #[prost(string, optional, tag = "1")]
    pub name: ::std::option::Option<std::string::String>,
    #[prost(string, optional, tag = "2")]
    pub full_name: ::std::option::Option<std::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TvfDescriptorProto {
    #[prost(string, repeated, tag = "1")]
    pub column_name: ::std::vec::Vec<std::string::String>,
}
/// The fields in here are in FunctionArgumentTypeOptions in the c++ API.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FunctionArgumentTypeOptionsProto {
    #[prost(
        enumeration = "function_enums::ArgumentCardinality",
        optional,
        tag = "1"
    )]
    pub cardinality: ::std::option::Option<i32>,
    #[prost(bool, optional, tag = "2")]
    pub must_be_constant: ::std::option::Option<bool>,
    #[prost(bool, optional, tag = "3")]
    pub must_be_non_null: ::std::option::Option<bool>,
    #[prost(bool, optional, tag = "4")]
    pub is_not_aggregate: ::std::option::Option<bool>,
    #[prost(bool, optional, tag = "5")]
    pub must_support_equality: ::std::option::Option<bool>,
    #[prost(bool, optional, tag = "6")]
    pub must_support_ordering: ::std::option::Option<bool>,
    #[prost(int64, optional, tag = "7")]
    pub min_value: ::std::option::Option<i64>,
    #[prost(int64, optional, tag = "8")]
    pub max_value: ::std::option::Option<i64>,
    #[prost(bool, optional, tag = "9")]
    pub extra_relation_input_columns_allowed: ::std::option::Option<bool>,
    #[prost(message, optional, tag = "10")]
    pub relation_input_schema: ::std::option::Option<TvfRelationProto>,
    #[prost(string, optional, tag = "11")]
    pub argument_name: ::std::option::Option<std::string::String>,
    #[prost(message, optional, tag = "12")]
    pub argument_name_parse_location: ::std::option::Option<ParseLocationRangeProto>,
    #[prost(message, optional, tag = "13")]
    pub argument_type_parse_location: ::std::option::Option<ParseLocationRangeProto>,
    #[prost(
        enumeration = "function_enums::ProcedureArgumentMode",
        optional,
        tag = "14"
    )]
    pub procedure_argument_mode: ::std::option::Option<i32>,
    #[prost(bool, optional, tag = "15", default = "false")]
    pub argument_name_is_mandatory: ::std::option::Option<bool>,
    #[prost(int32, optional, tag = "16", default = "-1")]
    pub descriptor_resolution_table_offset: ::std::option::Option<i32>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FunctionArgumentTypeProto {
    #[prost(enumeration = "SignatureArgumentKind", optional, tag = "1")]
    pub kind: ::std::option::Option<i32>,
    #[prost(message, optional, tag = "2")]
    pub r#type: ::std::option::Option<TypeProto>,
    #[prost(int32, optional, tag = "4")]
    pub num_occurrences: ::std::option::Option<i32>,
    #[prost(message, optional, tag = "3")]
    pub options: ::std::option::Option<FunctionArgumentTypeOptionsProto>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FunctionSignatureOptionsProto {
    /// optional uint64 timestamp_modes = 1;  bitset<TimestampMode>
    #[prost(bool, optional, tag = "2", default = "false")]
    pub is_deprecated: ::std::option::Option<bool>,
    #[prost(message, repeated, tag = "3")]
    pub additional_deprecation_warning: ::std::vec::Vec<FreestandingDeprecationWarning>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FunctionSignatureProto {
    #[prost(message, repeated, tag = "1")]
    pub argument: ::std::vec::Vec<FunctionArgumentTypeProto>,
    #[prost(message, optional, tag = "2")]
    pub return_type: ::std::option::Option<FunctionArgumentTypeProto>,
    #[prost(int64, optional, tag = "3")]
    pub context_id: ::std::option::Option<i64>,
    #[prost(message, optional, tag = "4")]
    pub options: ::std::option::Option<FunctionSignatureOptionsProto>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FunctionOptionsProto {
    #[prost(bool, optional, tag = "1", default = "false")]
    pub supports_over_clause: ::std::option::Option<bool>,
    #[prost(
        enumeration = "function_enums::WindowOrderSupport",
        optional,
        tag = "2",
        default = "OrderUnsupported"
    )]
    pub window_ordering_support: ::std::option::Option<i32>,
    #[prost(bool, optional, tag = "3", default = "false")]
    pub supports_window_framing: ::std::option::Option<bool>,
    #[prost(bool, optional, tag = "4", default = "true")]
    pub arguments_are_coercible: ::std::option::Option<bool>,
    #[prost(bool, optional, tag = "5", default = "false")]
    pub is_deprecated: ::std::option::Option<bool>,
    #[prost(string, optional, tag = "6")]
    pub alias_name: ::std::option::Option<std::string::String>,
    #[prost(string, optional, tag = "7")]
    pub sql_name: ::std::option::Option<std::string::String>,
    #[prost(bool, optional, tag = "8", default = "true")]
    pub allow_external_usage: ::std::option::Option<bool>,
    #[prost(
        enumeration = "function_enums::Volatility",
        optional,
        tag = "9",
        default = "Immutable"
    )]
    pub volatility: ::std::option::Option<i32>,
    #[prost(bool, optional, tag = "10", default = "false")]
    pub supports_order_by: ::std::option::Option<bool>,
    #[prost(
        enumeration = "LanguageFeature",
        repeated,
        packed = "false",
        tag = "11"
    )]
    pub required_language_feature: ::std::vec::Vec<i32>,
    #[prost(bool, optional, tag = "12", default = "false")]
    pub supports_limit: ::std::option::Option<bool>,
    #[prost(bool, optional, tag = "13", default = "false")]
    pub supports_null_handling_modifier: ::std::option::Option<bool>,
    #[prost(bool, optional, tag = "14", default = "true")]
    pub supports_safe_error_mode: ::std::option::Option<bool>,
    #[prost(bool, optional, tag = "15", default = "true")]
    pub supports_having_modifier: ::std::option::Option<bool>,
    #[prost(bool, optional, tag = "17", default = "true")]
    pub uses_upper_case_sql_name: ::std::option::Option<bool>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FunctionProto {
    #[prost(string, repeated, tag = "1")]
    pub name_path: ::std::vec::Vec<std::string::String>,
    #[prost(string, optional, tag = "2")]
    pub group: ::std::option::Option<std::string::String>,
    #[prost(enumeration = "function_enums::Mode", optional, tag = "3")]
    pub mode: ::std::option::Option<i32>,
    #[prost(message, repeated, tag = "4")]
    pub signature: ::std::vec::Vec<FunctionSignatureProto>,
    #[prost(message, optional, tag = "5")]
    pub options: ::std::option::Option<FunctionOptionsProto>,
    #[prost(message, optional, tag = "8")]
    pub parse_resume_location: ::std::option::Option<ParseResumeLocationProto>,
    #[prost(string, repeated, tag = "7")]
    pub templated_sql_function_argument_name: ::std::vec::Vec<std::string::String>,
}
/// Nothing in here for now.
/// TODO: add extra fields in here for derived context objects.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedFunctionCallInfoProto {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableValuedFunctionProto {
    #[prost(string, repeated, tag = "1")]
    pub name_path: ::std::vec::Vec<std::string::String>,
    #[prost(message, optional, tag = "2")]
    pub signature: ::std::option::Option<FunctionSignatureProto>,
    #[prost(
        enumeration = "function_enums::TableValuedFunctionType",
        optional,
        tag = "3"
    )]
    pub r#type: ::std::option::Option<i32>,
    #[prost(enumeration = "function_enums::Volatility", optional, tag = "8")]
    pub volatility: ::std::option::Option<i32>,
    #[prost(message, optional, tag = "6")]
    pub parse_resume_location: ::std::option::Option<ParseResumeLocationProto>,
    #[prost(string, repeated, tag = "5")]
    pub argument_name: ::std::vec::Vec<std::string::String>,
    #[prost(string, optional, tag = "7")]
    pub custom_context: ::std::option::Option<std::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TvfArgumentProto {
    #[prost(message, optional, tag = "1")]
    pub scalar_argument: ::std::option::Option<ValueWithTypeProto>,
    #[prost(message, optional, tag = "2")]
    pub relation_argument: ::std::option::Option<TvfRelationProto>,
    #[prost(message, optional, tag = "3")]
    pub model_argument: ::std::option::Option<TvfModelProto>,
    #[prost(message, optional, tag = "4")]
    pub connection_argument: ::std::option::Option<TvfConnectionProto>,
    #[prost(message, optional, tag = "5")]
    pub descriptor_argument: ::std::option::Option<TvfDescriptorProto>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TvfSignatureOptionsProto {
    #[prost(message, repeated, tag = "1")]
    pub additional_deprecation_warning: ::std::vec::Vec<FreestandingDeprecationWarning>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TvfSignatureProto {
    #[prost(message, repeated, tag = "1")]
    pub argument: ::std::vec::Vec<TvfArgumentProto>,
    #[prost(message, optional, tag = "2")]
    pub output_schema: ::std::option::Option<TvfRelationProto>,
    #[prost(message, optional, tag = "3")]
    pub options: ::std::option::Option<TvfSignatureOptionsProto>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ProcedureProto {
    #[prost(string, repeated, tag = "1")]
    pub name_path: ::std::vec::Vec<std::string::String>,
    #[prost(message, optional, tag = "2")]
    pub signature: ::std::option::Option<FunctionSignatureProto>,
}
// These are enums that are used in the Resolved AST nodes.
// Enum type and values under <cpp_class_name>Enums will be "imported" to the
// corresponding C++ class as typedefs and static consts, so that they can be
// referred to under the class name.
// E.g. ResolvedSubqueryExprEnums::SubqueryType::SCALAR can be referred to with
// ResolvedSubqueryExpr::SCALAR.

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedSubqueryExprEnums {}
pub mod resolved_subquery_expr_enums {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum SubqueryType {
        Scalar = 0,
        Array = 1,
        Exists = 2,
        In = 3,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedJoinScanEnums {}
pub mod resolved_join_scan_enums {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum JoinType {
        Inner = 0,
        Left = 1,
        Right = 2,
        Full = 3,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedSetOperationScanEnums {}
pub mod resolved_set_operation_scan_enums {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum SetOperationType {
        UnionAll = 0,
        UnionDistinct = 1,
        IntersectAll = 2,
        IntersectDistinct = 3,
        ExceptAll = 4,
        ExceptDistinct = 5,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedSampleScanEnums {}
pub mod resolved_sample_scan_enums {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum SampleUnit {
        Rows = 0,
        Percent = 1,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedOrderByItemEnums {}
pub mod resolved_order_by_item_enums {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum NullOrderMode {
        OrderUnspecified = 0,
        NullsFirst = 1,
        NullsLast = 2,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedCreateStatementEnums {}
pub mod resolved_create_statement_enums {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum CreateScope {
        CreateDefaultScope = 0,
        CreatePrivate = 1,
        CreatePublic = 2,
        CreateTemp = 3,
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum CreateMode {
        CreateDefault = 0,
        CreateOrReplace = 1,
        CreateIfNotExists = 2,
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum SqlSecurity {
        Unspecified = 0,
        Definer = 1,
        Invoker = 2,
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum DeterminismLevel {
        DeterminismUnspecified = 0,
        DeterminismDeterministic = 1,
        DeterminismNotDeterministic = 2,
        DeterminismImmutable = 3,
        DeterminismStable = 4,
        DeterminismVolatile = 5,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedBeginStmtEnums {}
pub mod resolved_begin_stmt_enums {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum ReadWriteMode {
        ModeUnspecified = 0,
        ModeReadOnly = 1,
        ModeReadWrite = 2,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedWindowFrameEnums {}
pub mod resolved_window_frame_enums {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum FrameUnit {
        Rows = 0,
        Range = 1,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedWindowFrameExprEnums {}
pub mod resolved_window_frame_expr_enums {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum BoundaryType {
        UnboundedPreceding = 0,
        OffsetPreceding = 1,
        CurrentRow = 2,
        OffsetFollowing = 3,
        UnboundedFollowing = 4,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedInsertStmtEnums {}
pub mod resolved_insert_stmt_enums {
    /// This defines the behavior of INSERT when there are duplicate rows.
    /// "Duplicate" generally mean rows with identical primary keys.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum InsertMode {
        /// Give an error.
        OrError = 0,
        /// Skip the duplicate row.
        OrIgnore = 1,
        /// Replace the row.
        OrReplace = 2,
        /// Merge inserted columns into the existing row.
        OrUpdate = 3,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedMergeWhenEnums {}
pub mod resolved_merge_when_enums {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum MatchType {
        /// WHEN MATCHED ... THEN clause.
        Matched = 0,
        /// WHEN NOT MATCHED BY SOURCE ... THEN clause.
        NotMatchedBySource = 1,
        /// WHEN NOT MATCHED BY TARGET ... THEN clause.
        NotMatchedByTarget = 2,
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum ActionType {
        Insert = 0,
        Update = 1,
        Delete = 2,
    }
}
/// Note: These enums are imported in both ResolvedArgument{Def,Ref}, using a
/// hack in gen_resolved_ast.py.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedArgumentDefEnums {}
pub mod resolved_argument_def_enums {
    /// This describes the type of argument in a CREATE FUNCTION signature.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum ArgumentKind {
        /// An argument to a scalar (non-aggregate) function.
        Scalar = 0,
        /// An aggregate argument to an aggregate function.
        Aggregate = 1,
        /// A non-aggregate argument to an aggregate function.
        NotAggregate = 2,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedFunctionCallBaseEnums {}
pub mod resolved_function_call_base_enums {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum ErrorMode {
        /// Return errors as usual.
        DefaultErrorMode = 0,
        /// If this function call returns a semantic error
        SafeErrorMode = 1,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedNonScalarFunctionCallBaseEnums {}
pub mod resolved_non_scalar_function_call_base_enums {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum NullHandlingModifier {
        /// Let each function decide how to handle nulls.
        DefaultNullHandling = 0,
        IgnoreNulls = 1,
        RespectNulls = 2,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedAggregateHavingModifierEnums {}
pub mod resolved_aggregate_having_modifier_enums {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum HavingModifierKind {
        Invalid = 0,
        Max = 1,
        Min = 2,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedStatementEnums {}
pub mod resolved_statement_enums {
    /// This describes the set of operations performed on objects.
    /// It is currently only used for ResolvedColumns.
    /// It can be READ, WRITE or both. This enum is a bitmap and values are
    /// intended to be bitwise OR'd together to produce the full set of operations.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum ObjectAccess {
        /// b0000
        None = 0,
        /// b0001
        Read = 1,
        /// b0010
        Write = 2,
        /// b0011
        ReadWrite = 3,
    }
}
/// LINT: LEGACY_NAMES
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedImportStmtEnums {}
pub mod resolved_import_stmt_enums {
    /// This describes the type of object imported in an IMPORT statement.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum ImportKind {
        Module = 0,
        Proto = 1,
        /// User code that switches on this enum must have a default case so
        /// builds won't break if new enums get added.  The user default code
        /// must also throw an error, since new import types will be implicitly
        /// unsupported.
        SwitchMustHaveADefault = -1,
    }
}
/// Enumerations for some of the foreign key column and table constraint
/// attributes.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedForeignKeyEnums {}
pub mod resolved_foreign_key_enums {
    /// FOREIGN KEY (a) REFERENCES t (r) MATCH <MatchMode>.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum MatchMode {
        Simple = 0,
        Full = 1,
        NotDistinct = 2,
    }
    /// FOREIGN KEY (a) REFERENCES t (r) ON UPDATE|DELETE <ActionOperation>.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum ActionOperation {
        NoAction = 0,
        Restrict = 1,
        Cascade = 2,
        SetNull = 3,
    }
}
/// AnyResolvedNodeProto is a container that can hold at most one proto
/// representation of a ResolvedNode
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AnyResolvedNodeProto {
    #[prost(oneof = "any_resolved_node_proto::Node", tags = "1, 2, 18, 36")]
    pub node: ::std::option::Option<any_resolved_node_proto::Node>,
}
pub mod any_resolved_node_proto {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Node {
        #[prost(message, tag = "1")]
        ResolvedArgumentNode(super::AnyResolvedArgumentProto),
        #[prost(message, tag = "2")]
        ResolvedExprNode(super::AnyResolvedExprProto),
        #[prost(message, tag = "18")]
        ResolvedScanNode(super::AnyResolvedScanProto),
        #[prost(message, tag = "36")]
        ResolvedStatementNode(super::AnyResolvedStatementProto),
    }
}
/// Argument nodes are not self-contained nodes in the tree.  They exist
/// only to describe parameters to another node (e.g. columns in an OrderBy).
/// This node is here for organizational purposes only, to cluster these
/// argument nodes.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AnyResolvedArgumentProto {
    #[prost(
        oneof = "any_resolved_argument_proto::Node",
        tags = "14, 23, 32, 33, 34, 52, 53, 54, 55, 56, 57, 58, 59, 61, 62, 65, 67, 77, 79, 82, 84, 85, 91, 92, 93, 94, 96, 100, 102, 104, 105, 109, 110, 113, 116, 126, 128, 141, 143, 144"
    )]
    pub node: ::std::option::Option<any_resolved_argument_proto::Node>,
}
pub mod any_resolved_argument_proto {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Node {
        #[prost(message, tag = "14")]
        ResolvedMakeProtoFieldNode(super::ResolvedMakeProtoFieldProto),
        #[prost(message, tag = "23")]
        ResolvedColumnHolderNode(super::ResolvedColumnHolderProto),
        #[prost(message, tag = "32")]
        ResolvedComputedColumnNode(super::ResolvedComputedColumnProto),
        #[prost(message, tag = "33")]
        ResolvedOrderByItemNode(super::ResolvedOrderByItemProto),
        #[prost(message, tag = "34")]
        ResolvedOutputColumnNode(super::ResolvedOutputColumnProto),
        #[prost(message, tag = "52")]
        ResolvedWithEntryNode(super::ResolvedWithEntryProto),
        #[prost(message, tag = "53")]
        ResolvedOptionNode(super::ResolvedOptionProto),
        #[prost(message, tag = "54")]
        ResolvedWindowPartitioningNode(super::ResolvedWindowPartitioningProto),
        #[prost(message, tag = "55")]
        ResolvedWindowOrderingNode(super::ResolvedWindowOrderingProto),
        #[prost(message, tag = "56")]
        ResolvedWindowFrameNode(super::ResolvedWindowFrameProto),
        #[prost(message, tag = "57")]
        ResolvedAnalyticFunctionGroupNode(super::ResolvedAnalyticFunctionGroupProto),
        #[prost(message, tag = "58")]
        ResolvedWindowFrameExprNode(super::ResolvedWindowFrameExprProto),
        #[prost(message, tag = "59")]
        ResolvedDmlvalueNode(super::ResolvedDmlValueProto),
        #[prost(message, tag = "61")]
        ResolvedAssertRowsModifiedNode(super::ResolvedAssertRowsModifiedProto),
        #[prost(message, tag = "62")]
        ResolvedInsertRowNode(super::ResolvedInsertRowProto),
        #[prost(message, tag = "65")]
        ResolvedUpdateItemNode(super::ResolvedUpdateItemProto),
        #[prost(message, tag = "67")]
        ResolvedPrivilegeNode(super::ResolvedPrivilegeProto),
        #[prost(message, tag = "77")]
        ResolvedArgumentDefNode(super::ResolvedArgumentDefProto),
        #[prost(message, tag = "79")]
        ResolvedArgumentListNode(super::ResolvedArgumentListProto),
        #[prost(message, tag = "82")]
        ResolvedTvfargumentNode(super::ResolvedTvfArgumentProto),
        #[prost(message, tag = "84")]
        ResolvedFunctionSignatureHolderNode(super::ResolvedFunctionSignatureHolderProto),
        #[prost(message, tag = "85")]
        ResolvedAggregateHavingModifierNode(super::ResolvedAggregateHavingModifierProto),
        #[prost(message, tag = "91")]
        ResolvedColumnDefinitionNode(super::ResolvedColumnDefinitionProto),
        #[prost(message, tag = "92")]
        ResolvedPrimaryKeyNode(super::ResolvedPrimaryKeyProto),
        #[prost(message, tag = "93")]
        ResolvedGroupingSetNode(super::ResolvedGroupingSetProto),
        #[prost(message, tag = "94")]
        ResolvedSetOperationItemNode(super::ResolvedSetOperationItemProto),
        #[prost(message, tag = "96")]
        ResolvedIndexItemNode(super::ResolvedIndexItemProto),
        #[prost(message, tag = "100")]
        ResolvedMergeWhenNode(super::ResolvedMergeWhenProto),
        #[prost(message, tag = "102")]
        ResolvedUpdateArrayItemNode(super::ResolvedUpdateArrayItemProto),
        #[prost(message, tag = "104")]
        ResolvedColumnAnnotationsNode(super::ResolvedColumnAnnotationsProto),
        #[prost(message, tag = "105")]
        ResolvedGeneratedColumnInfoNode(super::ResolvedGeneratedColumnInfoProto),
        #[prost(message, tag = "109")]
        ResolvedModelNode(super::ResolvedModelProto),
        #[prost(message, tag = "110")]
        ResolvedForeignKeyNode(super::ResolvedForeignKeyProto),
        #[prost(message, tag = "113")]
        ResolvedCheckConstraintNode(super::ResolvedCheckConstraintProto),
        #[prost(message, tag = "116")]
        ResolvedAlterActionNode(super::AnyResolvedAlterActionProto),
        #[prost(message, tag = "126")]
        ResolvedUnnestItemNode(super::ResolvedUnnestItemProto),
        #[prost(message, tag = "128")]
        ResolvedReplaceFieldItemNode(super::ResolvedReplaceFieldItemProto),
        #[prost(message, tag = "141")]
        ResolvedConnectionNode(super::ResolvedConnectionProto),
        #[prost(message, tag = "143")]
        ResolvedExecuteImmediateArgumentNode(super::ResolvedExecuteImmediateArgumentProto),
        #[prost(message, tag = "144")]
        ResolvedDescriptorNode(super::ResolvedDescriptorProto),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedArgumentProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedNodeProto>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AnyResolvedExprProto {
    #[prost(
        oneof = "any_resolved_expr_proto::Node",
        tags = "3, 4, 5, 6, 7, 11, 12, 13, 15, 16, 17, 60, 78, 103, 129, 139"
    )]
    pub node: ::std::option::Option<any_resolved_expr_proto::Node>,
}
pub mod any_resolved_expr_proto {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Node {
        #[prost(message, tag = "3")]
        ResolvedLiteralNode(super::ResolvedLiteralProto),
        #[prost(message, tag = "4")]
        ResolvedParameterNode(super::ResolvedParameterProto),
        #[prost(message, tag = "5")]
        ResolvedExpressionColumnNode(super::ResolvedExpressionColumnProto),
        #[prost(message, tag = "6")]
        ResolvedColumnRefNode(super::ResolvedColumnRefProto),
        #[prost(message, tag = "7")]
        ResolvedFunctionCallBaseNode(Box<super::AnyResolvedFunctionCallBaseProto>),
        #[prost(message, tag = "11")]
        ResolvedCastNode(Box<super::ResolvedCastProto>),
        #[prost(message, tag = "12")]
        ResolvedMakeStructNode(super::ResolvedMakeStructProto),
        #[prost(message, tag = "13")]
        ResolvedMakeProtoNode(super::ResolvedMakeProtoProto),
        #[prost(message, tag = "15")]
        ResolvedGetStructFieldNode(Box<super::ResolvedGetStructFieldProto>),
        #[prost(message, tag = "16")]
        ResolvedGetProtoFieldNode(Box<super::ResolvedGetProtoFieldProto>),
        #[prost(message, tag = "17")]
        ResolvedSubqueryExprNode(Box<super::ResolvedSubqueryExprProto>),
        #[prost(message, tag = "60")]
        ResolvedDmldefaultNode(super::ResolvedDmlDefaultProto),
        #[prost(message, tag = "78")]
        ResolvedArgumentRefNode(super::ResolvedArgumentRefProto),
        #[prost(message, tag = "103")]
        ResolvedConstantNode(super::ResolvedConstantProto),
        #[prost(message, tag = "129")]
        ResolvedReplaceFieldNode(Box<super::ResolvedReplaceFieldProto>),
        #[prost(message, tag = "139")]
        ResolvedSystemVariableNode(super::ResolvedSystemVariableProto),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedExprProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedNodeProto>,
    #[prost(message, optional, tag = "2")]
    pub r#type: ::std::option::Option<TypeProto>,
}
/// Any literal value, including NULL literals.
/// There is a special-cased constructor here that gets the type from the
/// Value.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedLiteralProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedExprProto>,
    #[prost(message, optional, tag = "2")]
    pub value: ::std::option::Option<ValueWithTypeProto>,
    /// If true, then the literal is explicitly typed and cannot be used
    /// for literal coercions.
    ///
    /// This exists mainly for resolver bookkeeping and should be ignored
    /// by engines.
    #[prost(bool, optional, tag = "3")]
    pub has_explicit_type: ::std::option::Option<bool>,
    /// Distinct ID of the literal, if it is a floating point value,
    /// within the resolved AST. When coercing from floating point
    /// to NUMERIC, the resolver uses the float_literal_id to find the
    /// original image of the literal to avoid precision loss. An ID of 0
    /// represents a literal without a cached image.
    #[prost(int64, optional, tag = "4")]
    pub float_literal_id: ::std::option::Option<i64>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedParameterProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedExprProto>,
    /// If non-empty, the name of the parameter.
    ///
    /// A ResolvedParameter will have either a name or a position but not
    /// both.
    #[prost(string, optional, tag = "2")]
    pub name: ::std::option::Option<std::string::String>,
    /// If non-zero, the 1-based position of the positional parameter.
    ///
    /// A ResolvedParameter will have either a name or a position but not
    /// both.
    #[prost(int64, optional, tag = "5")]
    pub position: ::std::option::Option<i64>,
    /// If true, then the parameter has no specified type.
    ///
    /// This exists mainly for resolver bookkeeping and should be ignored
    /// by engines.
    #[prost(bool, optional, tag = "3")]
    pub is_untyped: ::std::option::Option<bool>,
}
/// This represents a column when analyzing a standalone expression.
/// This is only used when the analyzer was called using AnalyzeExpression.
/// Expression column names and types come from
/// AnalyzerOptions::AddExpressionColumn.
/// <name> will always be in lowercase.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedExpressionColumnProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedExprProto>,
    #[prost(string, optional, tag = "2")]
    pub name: ::std::option::Option<std::string::String>,
}
/// An expression referencing the value of some column visible in the
/// current Scan node.
///
/// If <is_correlated> is false, this must be a column visible in the Scan
/// containing this expression, either because it was produced inside that
/// Scan or it is on the <column_list> of some child of this Scan.
///
/// If <is_correlated> is true, this references a column from outside a
/// subquery that is visible as a correlated column inside.
/// The column referenced here must show up on the parameters list for the
/// subquery.  See ResolvedSubqueryExpr.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedColumnRefProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedExprProto>,
    #[prost(message, optional, tag = "2")]
    pub column: ::std::option::Option<ResolvedColumnProto>,
    #[prost(bool, optional, tag = "3")]
    pub is_correlated: ::std::option::Option<bool>,
}
/// A reference to a named constant.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedConstantProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedExprProto>,
    /// The matching Constant from the Catalog.
    #[prost(message, optional, tag = "2")]
    pub constant: ::std::option::Option<ConstantRefProto>,
}
/// A reference to a system variable.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedSystemVariableProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedExprProto>,
    /// Path to system variable.
    #[prost(string, repeated, tag = "2")]
    pub name_path: ::std::vec::Vec<std::string::String>,
}
/// Common base class for scalar and aggregate function calls.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AnyResolvedFunctionCallBaseProto {
    #[prost(oneof = "any_resolved_function_call_base_proto::Node", tags = "8, 86")]
    pub node: ::std::option::Option<any_resolved_function_call_base_proto::Node>,
}
pub mod any_resolved_function_call_base_proto {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Node {
        #[prost(message, tag = "8")]
        ResolvedFunctionCallNode(super::ResolvedFunctionCallProto),
        #[prost(message, tag = "86")]
        ResolvedNonScalarFunctionCallBaseNode(
            Box<super::AnyResolvedNonScalarFunctionCallBaseProto>,
        ),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedFunctionCallBaseProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedExprProto>,
    /// The matching Function from the Catalog.
    #[prost(message, optional, tag = "2")]
    pub function: ::std::option::Option<FunctionRefProto>,
    /// The concrete FunctionSignature reflecting the matching Function
    /// signature and the function's resolved input <argument_list>.
    /// The function has the mode AGGREGATE iff it is an aggregate
    /// function, in which case this node must be either
    /// ResolvedAggregateFunctionCall or ResolvedAnalyticFunctionCall.
    #[prost(message, optional, tag = "3")]
    pub signature: ::std::option::Option<FunctionSignatureProto>,
    #[prost(message, repeated, tag = "4")]
    pub argument_list: ::std::vec::Vec<AnyResolvedExprProto>,
    /// If error_mode=SAFE_ERROR_MODE, and if this function call returns a
    /// semantic error (based on input data, not transient server
    /// problems), return NULL instead of an error. This is used for
    /// functions called using SAFE, as in SAFE.FUNCTION(...).
    #[prost(
        enumeration = "resolved_function_call_base_enums::ErrorMode",
        optional,
        tag = "5"
    )]
    pub error_mode: ::std::option::Option<i32>,
}
/// A regular function call.  The signature will always have mode SCALAR.
/// Most scalar expressions show up as FunctionCalls using builtin signatures.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedFunctionCallProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedFunctionCallBaseProto>,
    /// This contains optional custom information about a particular
    /// function call.
    ///
    /// If some Function subclass requires computing additional
    /// information at resolving time, that extra information can be
    /// stored as a subclass of ResolvedFunctionCallInfo here.
    /// For example, TemplatedSQLFunction stores the resolved template
    /// body here as a TemplatedSQLFunctionCall.
    ///
    /// This field is ignorable because for most types of function calls,
    /// there is no extra information to consider besides the arguments
    /// and other fields from ResolvedFunctionCallBase.
    #[prost(message, optional, tag = "2")]
    pub function_call_info: ::std::option::Option<ResolvedFunctionCallInfoProto>,
}
/// Common base class for scalar and aggregate function calls.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AnyResolvedNonScalarFunctionCallBaseProto {
    #[prost(
        oneof = "any_resolved_non_scalar_function_call_base_proto::Node",
        tags = "9, 10"
    )]
    pub node: ::std::option::Option<any_resolved_non_scalar_function_call_base_proto::Node>,
}
pub mod any_resolved_non_scalar_function_call_base_proto {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Node {
        #[prost(message, tag = "9")]
        ResolvedAggregateFunctionCallNode(Box<super::ResolvedAggregateFunctionCallProto>),
        #[prost(message, tag = "10")]
        ResolvedAnalyticFunctionCallNode(Box<super::ResolvedAnalyticFunctionCallProto>),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedNonScalarFunctionCallBaseProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedFunctionCallBaseProto>,
    /// Apply DISTINCT to the stream of input values before calling
    /// function.
    #[prost(bool, optional, tag = "2")]
    pub distinct: ::std::option::Option<bool>,
    /// Apply IGNORE/RESPECT NULLS filtering to the stream of input
    /// values.
    #[prost(
        enumeration = "resolved_non_scalar_function_call_base_enums::NullHandlingModifier",
        optional,
        tag = "3"
    )]
    pub null_handling_modifier: ::std::option::Option<i32>,
}
/// An aggregate function call.  The signature always has mode AGGREGATE.
/// This node only ever shows up as the outer function call in a
/// ResolvedAggregateScan::aggregate_list.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedAggregateFunctionCallProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedNonScalarFunctionCallBaseProto>,
    /// Apply HAVING MAX/MIN filtering to the stream of input values.
    #[prost(message, optional, boxed, tag = "5")]
    pub having_modifier:
        ::std::option::Option<::std::boxed::Box<ResolvedAggregateHavingModifierProto>>,
    /// Apply ordering to the stream of input values before calling
    /// function.
    #[prost(message, repeated, tag = "3")]
    pub order_by_item_list: ::std::vec::Vec<ResolvedOrderByItemProto>,
    #[prost(message, optional, boxed, tag = "4")]
    pub limit: ::std::option::Option<::std::boxed::Box<AnyResolvedExprProto>>,
    /// This contains optional custom information about a particular
    /// function call. Functions may introduce subclasses of this class to
    /// add custom information as needed on a per-function basis.
    ///
    /// This field is ignorable because for most types of function calls,
    /// there is no extra information to consider besides the arguments
    /// and other fields from ResolvedFunctionCallBase. However, for
    /// example, the TemplateSQLFunction in
    /// zetasql/public/templated_sql_function.h defines the
    /// TemplatedSQLFunctionCall subclass which includes the
    /// fully-resolved function body in context of the actual concrete
    /// types of the arguments provided to the function call.
    #[prost(message, optional, tag = "6")]
    pub function_call_info: ::std::option::Option<ResolvedFunctionCallInfoProto>,
}
/// An analytic function call. The mode of the function is either AGGREGATE
/// or ANALYTIC. This node only ever shows up as a function call in a
/// ResolvedAnalyticFunctionGroup::analytic_function_list. Its associated
/// window is not under this node but as a sibling of its parent node.
///
/// <window_frame> can be NULL.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedAnalyticFunctionCallProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedNonScalarFunctionCallBaseProto>,
    #[prost(message, optional, boxed, tag = "2")]
    pub window_frame: ::std::option::Option<::std::boxed::Box<ResolvedWindowFrameProto>>,
}
/// A cast expression, casting the result of an input expression to the
/// target Type.
///
/// Valid casts are defined in the CastHashMap (see cast.cc), which identifies
/// valid from-Type, to-Type pairs.  Consumers can access it through
/// GetZetaSQLCasts().
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedCastProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedExprProto>,
    #[prost(message, optional, boxed, tag = "2")]
    pub expr: ::std::option::Option<::std::boxed::Box<AnyResolvedExprProto>>,
    /// Whether to return NULL if the cast fails. This is set to true for
    /// SAFE_CAST.
    #[prost(bool, optional, tag = "3")]
    pub return_null_on_error: ::std::option::Option<bool>,
}
/// Construct a struct value.  <type> is always a StructType.
/// <field_list> matches 1:1 with the fields in <type> position-wise.
/// Each field's type will match the corresponding field in <type>.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedMakeStructProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedExprProto>,
    #[prost(message, repeated, tag = "2")]
    pub field_list: ::std::vec::Vec<AnyResolvedExprProto>,
}
/// Construct a proto value.  <type> is always a ProtoType.
/// <field_list> is a vector of (FieldDescriptor, expr) pairs to write.
/// <field_list> will contain all required fields, and no duplicate fields.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedMakeProtoProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedExprProto>,
    #[prost(message, repeated, tag = "2")]
    pub field_list: ::std::vec::Vec<ResolvedMakeProtoFieldProto>,
}
/// One field assignment in a ResolvedMakeProto expression.
/// The type of expr will match with the zetasql type of the proto field.
/// The type will be an array iff the field is repeated.
///
/// For NULL values of <expr>, the proto field should be cleared.
///
/// If any value of <expr> cannot be written into the field, this query
/// should fail.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedMakeProtoFieldProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedArgumentProto>,
    #[prost(message, optional, tag = "2")]
    pub field_descriptor: ::std::option::Option<FieldDescriptorRefProto>,
    /// Provides the Format annotation that should be used when building
    /// this field.  The annotation specifies both the ZetaSQL type and
    /// the encoding format for this field.
    #[prost(enumeration = "field_format::Format", optional, tag = "3")]
    pub format: ::std::option::Option<i32>,
    #[prost(message, optional, tag = "4")]
    pub expr: ::std::option::Option<AnyResolvedExprProto>,
}
/// Get the field in position <field_idx> (0-based) from <expr>, which has a
/// STRUCT type.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedGetStructFieldProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedExprProto>,
    #[prost(message, optional, boxed, tag = "2")]
    pub expr: ::std::option::Option<::std::boxed::Box<AnyResolvedExprProto>>,
    #[prost(int64, optional, tag = "3")]
    pub field_idx: ::std::option::Option<i64>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedGetProtoFieldProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedExprProto>,
    #[prost(message, optional, boxed, tag = "2")]
    pub expr: ::std::option::Option<::std::boxed::Box<AnyResolvedExprProto>>,
    /// The proto2 FieldDescriptor to extract.  This provides the tag
    /// number and wire type.  Additional decoding may be necessary if any
    /// of the other modifiers below are set.  Consumers should use those
    /// ZetaSQL-computed modifiers rather than examining field
    /// annotations directly.
    ///
    /// The field is an extension field iff
    /// field_descriptor->is_extension() is true.  NOTE: The extended
    /// descriptor's full_name must match the <expr>'s type's full_name,
    /// but may not be the same Descriptor. Extension FieldDescriptors may
    /// come from a different DescriptorPool.
    ///
    /// The field is required if field_descriptor->is_required().  If the
    /// field is required and not present, an error should result.
    #[prost(message, optional, tag = "3")]
    pub field_descriptor: ::std::option::Option<FieldDescriptorRefProto>,
    /// Default value to use when the proto field is not set. The default
    /// may be NULL (e.g. for proto2 fields with a use_defaults=false
    /// annotation).
    ///
    /// This will not be filled in (the Value will be uninitialized) if
    /// get_has_bit is true, or the field is required.
    ///
    /// If field_descriptor->is_required() and the field is not present,
    /// the engine should return an error.
    ///
    /// If the <expr> itself returns NULL, then extracting a field should
    /// also return NULL, unless <return_default_value_when_unset> is
    /// true. In that case, the default value is returned.
    ///
    /// TODO Make un-ignorable after clients migrate to start
    /// using it.
    #[prost(message, optional, tag = "4")]
    pub default_value: ::std::option::Option<ValueWithTypeProto>,
    /// Indicates whether to return a bool indicating if a value was
    /// present, rather than return the value (or NULL). Never set for
    /// repeated fields. This field cannot be set if
    /// <return_default_value_when_unset> is true, and vice versa.
    /// Expression type will be BOOL.
    #[prost(bool, optional, tag = "5")]
    pub get_has_bit: ::std::option::Option<bool>,
    /// Provides the Format annotation that should be used when reading
    /// this field.  The annotation specifies both the ZetaSQL type and
    /// the encoding format for this field. This cannot be set when
    /// get_has_bit is true.
    #[prost(enumeration = "field_format::Format", optional, tag = "6")]
    pub format: ::std::option::Option<i32>,
    /// Indicates that the default value should be returned if <expr>
    /// (the parent message) is NULL.  Note that this does *not* affect
    /// the return value when the extracted field itself is unset, in
    /// which case the return value depends on the extracted field's
    /// annotations (e.g., use_field_defaults).
    ///
    /// This can only be set for non-message fields. If the field is a
    /// proto2 field, then it must be annotated with
    /// zetasql.use_defaults=true. This cannot be set when <get_has_bit>
    /// is true or the field is required.
    #[prost(bool, optional, tag = "7")]
    pub return_default_value_when_unset: ::std::option::Option<bool>,
}
/// An argument to the REPLACE_FIELDS() function which specifies a field path
/// and a value that this field will be set to. The field path to be modified
/// can be constructed through the <struct_index_path> and <proto_field_path>
/// fields. These vectors correspond to field paths in a STRUCT and PROTO,
/// respectively. At least one of these vectors must be non-empty.
///
/// If only <struct_index_path> is non-empty, then the field path only
/// references top-level and nested struct fields.
///
/// If only <proto_field_path> is non-empty, then the field path only
/// references top-level and nested message fields.
///
/// If both <struct_index_path> and <proto_field_path> are non-empty, then the
/// field path should be expanded starting with <struct_index_path>. The last
/// field in <struct_index_path> will be the proto from which the first field
/// in <proto_field_path> is extracted.
///
/// <expr> and the field to be modified must be the same type.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedReplaceFieldItemProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedArgumentProto>,
    /// The value that the final field in <proto_field_path> will be set
    /// to.
    ///
    /// If <expr> is NULL, the field will be unset. If <proto_field_path>
    /// is a required field, the engine must return an error if it is set
    /// to NULL.
    #[prost(message, optional, tag = "2")]
    pub expr: ::std::option::Option<AnyResolvedExprProto>,
    /// A vector of integers that denotes the path to a struct field that
    /// will be modified. The integer values in this vector correspond to
    /// field positions (0-based) in a STRUCT. If <proto_field_path>
    /// is also non-empty, then the field corresponding to the last index
    /// in this vector should be of proto type.
    #[prost(int64, repeated, packed = "false", tag = "3")]
    pub struct_index_path: ::std::vec::Vec<i64>,
    /// A vector of FieldDescriptors that denotes the path to a proto
    /// field that will be modified. If <struct_index_path> is also
    /// non-empty, then the first element in this vector should be a
    /// subfield of the proto corresponding to the last element in
    /// <struct_index_path>.
    #[prost(message, repeated, tag = "4")]
    pub proto_field_path: ::std::vec::Vec<FieldDescriptorRefProto>,
}
/// Represents a call to the REPLACE_FIELDS() function. This function
/// can be used to copy a proto or struct, modify a few fields and
/// output the resulting proto or struct. The SQL syntax for this
/// function is REPLACE_FIELDS(<expr>, <replace_field_item_list>).
///
/// See (broken link) for more detail.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedReplaceFieldProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedExprProto>,
    /// The proto/struct to modify.
    #[prost(message, optional, boxed, tag = "2")]
    pub expr: ::std::option::Option<::std::boxed::Box<AnyResolvedExprProto>>,
    /// The list of field paths to be modified along with their new
    /// values.
    ///
    /// Engines must check at evaluation time that the modifications in
    /// <replace_field_item_list> obey the following rules
    /// regarding updating protos in ZetaSQL:
    /// - Modifying a subfield of a NULL-valued proto-valued field is an
    ///   error.
    /// - Clearing a required field or subfield is an error.
    #[prost(message, repeated, tag = "3")]
    pub replace_field_item_list: ::std::vec::Vec<ResolvedReplaceFieldItemProto>,
}
/// A subquery in an expression (not a FROM clause).  The subquery runs
/// in the context of a single input row and produces a single output value.
///
/// Correlated subqueries can be thought of like functions, with a parameter
/// list.  The <parameter_list> gives the set of ResolvedColumns from outside
/// the subquery that are used inside.
///
/// Inside the subquery, the only allowed references to values outside the
/// subquery are to the named ColumnRefs listed in <parameter_list>.
/// Any reference to one of these parameters will be represented as a
/// ResolvedColumnRef with <is_correlated> set to true.
///
/// These parameters are only visible through one level of expression
/// subquery.  An expression subquery inside an expression has to list
/// parameters again if parameters from the outer query are passed down
/// further.  (This does not apply for table subqueries inside an expression
/// subquery.  Table subqueries are never indicated in the resolved AST, so
/// Scan nodes inside an expression query may have come from a nested table
/// subquery, and they can still reference the expression subquery's
/// parameters.)
///
/// An empty <parameter_list> means that the subquery is uncorrelated.  It is
/// permissable to run an uncorrelated subquery only once and reuse the result.
/// TODO Do we want to specify semantics more firmly here?
///
/// The semantics vary based on SubqueryType:
///   SCALAR
///     Usage: ( <subquery> )
///     If the subquery produces zero rows, the output value is NULL.
///     If the subquery produces exactly one row, that row is the output value.
///     If the subquery produces more than one row, raise a runtime error.
///
///   ARRAY
///     Usage: ARRAY( <subquery> )
///     The subquery produces an array value with zero or more rows, with
///     one array element per subquery row produced.
///
///   EXISTS
///     Usage: EXISTS( <subquery> )
///     The output type is always bool.  The result is true if the subquery
///     produces at least one row, and false otherwise.
///
///   IN
///     Usage: <in_expr> [NOT] IN ( <subquery> )
///     The output type is always bool.  The result is true when <in_expr> is
///     equal to at least one row, and false otherwise.  The <subquery> row
///     contains only one column, and the types of <in_expr> and the
///     subquery column must exactly match a built-in signature for the
///     '$equals' comparison function (they must be the same type or one
///     must be INT64 and the other UINT64).  NOT will be expressed as a $not
///     FunctionCall wrapping this SubqueryExpr.
///
/// The subquery for a SCALAR or ARRAY or IN subquery must have exactly one
/// output column.
/// The output type for a SCALAR or ARRAY subquery is that column's type or
/// an array of that column's type.  (The subquery scan may include a Map
/// with a MakeStruct or MakeProto expression to construct a single value
/// from multiple columns.)
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedSubqueryExprProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedExprProto>,
    #[prost(
        enumeration = "resolved_subquery_expr_enums::SubqueryType",
        optional,
        tag = "2"
    )]
    pub subquery_type: ::std::option::Option<i32>,
    #[prost(message, repeated, tag = "3")]
    pub parameter_list: ::std::vec::Vec<ResolvedColumnRefProto>,
    /// Field is only populated for subquery of type IN.
    #[prost(message, optional, boxed, tag = "4")]
    pub in_expr: ::std::option::Option<::std::boxed::Box<AnyResolvedExprProto>>,
    #[prost(message, optional, boxed, tag = "5")]
    pub subquery: ::std::option::Option<::std::boxed::Box<AnyResolvedScanProto>>,
    /// Note: Hints currently happen only for EXISTS or IN subquery but
    /// not for ARRAY or SCALAR subquery.
    #[prost(message, repeated, tag = "6")]
    pub hint_list: ::std::vec::Vec<ResolvedOptionProto>,
}
/// Common superclass for all Scans, which are nodes that produce rows
/// (e.g. scans, joins, table subqueries).  A query's FROM clause is
/// represented as a single Scan that composes all input sources into
/// a single row stream.
///
/// Each Scan has a <column_list> that says what columns are produced.
/// The Scan logically produces a stream of output rows, where each row
/// has exactly these columns.
///
/// Each Scan may have an attached <hint_list>, storing each hint as
/// a ResolvedOption.
///
/// If <is_ordered> is true, this Scan produces an ordered output, either
/// by generating order itself (OrderByScan) or by preserving the order
/// of its single input scan (LimitOffsetScan, ProjectScan, or WithScan).
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AnyResolvedScanProto {
    #[prost(
        oneof = "any_resolved_scan_proto::Node",
        tags = "19, 20, 21, 22, 24, 26, 27, 28, 29, 30, 31, 35, 51, 81, 89, 111"
    )]
    pub node: ::std::option::Option<any_resolved_scan_proto::Node>,
}
pub mod any_resolved_scan_proto {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Node {
        #[prost(message, tag = "19")]
        ResolvedSingleRowScanNode(super::ResolvedSingleRowScanProto),
        #[prost(message, tag = "20")]
        ResolvedTableScanNode(Box<super::ResolvedTableScanProto>),
        #[prost(message, tag = "21")]
        ResolvedJoinScanNode(Box<super::ResolvedJoinScanProto>),
        #[prost(message, tag = "22")]
        ResolvedArrayScanNode(Box<super::ResolvedArrayScanProto>),
        #[prost(message, tag = "24")]
        ResolvedFilterScanNode(Box<super::ResolvedFilterScanProto>),
        #[prost(message, tag = "26")]
        ResolvedSetOperationScanNode(super::ResolvedSetOperationScanProto),
        #[prost(message, tag = "27")]
        ResolvedOrderByScanNode(Box<super::ResolvedOrderByScanProto>),
        #[prost(message, tag = "28")]
        ResolvedLimitOffsetScanNode(Box<super::ResolvedLimitOffsetScanProto>),
        #[prost(message, tag = "29")]
        ResolvedWithRefScanNode(super::ResolvedWithRefScanProto),
        #[prost(message, tag = "30")]
        ResolvedAnalyticScanNode(Box<super::ResolvedAnalyticScanProto>),
        #[prost(message, tag = "31")]
        ResolvedSampleScanNode(Box<super::ResolvedSampleScanProto>),
        #[prost(message, tag = "35")]
        ResolvedProjectScanNode(Box<super::ResolvedProjectScanProto>),
        #[prost(message, tag = "51")]
        ResolvedWithScanNode(Box<super::ResolvedWithScanProto>),
        #[prost(message, tag = "81")]
        ResolvedTvfscanNode(super::ResolvedTvfScanProto),
        #[prost(message, tag = "89")]
        ResolvedRelationArgumentScanNode(super::ResolvedRelationArgumentScanProto),
        #[prost(message, tag = "111")]
        ResolvedAggregateScanBaseNode(Box<super::AnyResolvedAggregateScanBaseProto>),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedScanProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedNodeProto>,
    #[prost(message, repeated, tag = "2")]
    pub column_list: ::std::vec::Vec<ResolvedColumnProto>,
    #[prost(message, repeated, tag = "3")]
    pub hint_list: ::std::vec::Vec<ResolvedOptionProto>,
    #[prost(bool, optional, tag = "4")]
    pub is_ordered: ::std::option::Option<bool>,
}
/// Represents a machine learning model as a TVF argument.
/// <model> is the machine learning model object known to the resolver
/// (usually through the catalog).
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedModelProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedArgumentProto>,
    #[prost(message, optional, tag = "2")]
    pub model: ::std::option::Option<ModelRefProto>,
}
/// Represents a connection object as a TVF argument.
/// <connection> is the connection object encapsulated metadata to connect to
/// an external data source.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedConnectionProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedArgumentProto>,
    #[prost(message, optional, tag = "2")]
    pub connection: ::std::option::Option<ConnectionRefProto>,
}
/// Represents a descriptor object as a TVF argument.
/// <descriptor_column_list> contains resolved columns from the related input
/// table argument if FunctionArgumentTypeOptions.get_resolve_descriptor_names_table_offset()
/// returns a valid argument offset.
/// <descriptor_column_name_list> contains strings which represent columns
/// names.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedDescriptorProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedArgumentProto>,
    #[prost(message, repeated, tag = "2")]
    pub descriptor_column_list: ::std::vec::Vec<ResolvedColumnProto>,
    #[prost(string, repeated, tag = "3")]
    pub descriptor_column_name_list: ::std::vec::Vec<std::string::String>,
}
/// Scan that produces a single row with no columns.  Used for queries without
/// a FROM clause, where all output comes from the select list.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedSingleRowScanProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedScanProto>,
}
/// Scan a Table.
/// The <column_list>[i] should be matched to a Table column by
/// <table>.GetColumn(<column_index_list>[i]).
///
/// If AnalyzerOptions::prune_unused_columns is true, the <column_list> and
/// <column_index_list> will include only columns that were referenced
/// in the user query. (SELECT * counts as referencing all columns.)
/// This column_list can then be used for column-level ACL checking on tables.
///
/// for_system_time_expr when non NULL resolves to TIMESTAMP used in
/// FOR SYSTEM_TIME AS OF clause. The expression is expected to be constant
/// and no columns are visible to it.
///
/// If provided, <alias> refers to an explicit alias which was used to
/// reference a Table in the user query. If the Table was given an implicitly
/// generated alias, then defaults to "".
///
/// TODO: Enforce <column_index_list> in the constructor arg list. For
/// historical reasons, some clients match <column_list> to Table columns by
/// name. All code building this should always set_column_index_list() to
/// provide the indexes of all columns in <table> right after the construction
/// of a ResolvedTableScan.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedTableScanProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedScanProto>,
    #[prost(message, optional, tag = "2")]
    pub table: ::std::option::Option<TableRefProto>,
    #[prost(message, optional, boxed, tag = "3")]
    pub for_system_time_expr: ::std::option::Option<::std::boxed::Box<AnyResolvedExprProto>>,
    #[prost(int64, repeated, packed = "false", tag = "4")]
    pub column_index_list: ::std::vec::Vec<i64>,
    #[prost(string, optional, tag = "5")]
    pub alias: ::std::option::Option<std::string::String>,
}
/// A Scan that joins two input scans.
/// The <column_list> will contain columns selected from the union
/// of the input scan's <column_lists>.
/// When the join is a LEFT/RIGHT/FULL join, ResolvedColumns that came from
/// the non-joined side get NULL values.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedJoinScanProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedScanProto>,
    #[prost(
        enumeration = "resolved_join_scan_enums::JoinType",
        optional,
        tag = "2"
    )]
    pub join_type: ::std::option::Option<i32>,
    #[prost(message, optional, boxed, tag = "3")]
    pub left_scan: ::std::option::Option<::std::boxed::Box<AnyResolvedScanProto>>,
    #[prost(message, optional, boxed, tag = "4")]
    pub right_scan: ::std::option::Option<::std::boxed::Box<AnyResolvedScanProto>>,
    #[prost(message, optional, boxed, tag = "5")]
    pub join_expr: ::std::option::Option<::std::boxed::Box<AnyResolvedExprProto>>,
}
/// Scan an array value, produced from some expression.
///
/// If input_scan is NULL, this scans the given array value and produces
/// one row per array element.  This can occur when using UNNEST(expression).
///
/// If <input_scan> is non-NULL, for each row in the stream produced by
/// input_scan, this evaluates the expression <array_expr> (which must return
/// an array type) and then produces a stream with one row per array element.
///
/// If <join_expr> is non-NULL, then this condition is evaluated as an ON
/// clause for the array join.  The named column produced in <array_expr>
/// may be used inside <join_expr>.
///
/// If the array is empty (after evaluating <join_expr>), then
/// 1. If <is_outer> is false, the scan produces zero rows.
/// 2. If <is_outer> is true, the scan produces one row with a NULL value for
///    the <element_column>.
///
/// <element_column> is the new column produced by this scan that stores the
/// array element value for each row.
///
/// If present, <array_offset_column> defines the column produced by this
/// scan that stores the array offset (0-based) for the corresponding
/// <element_column>.
///
/// This node's column_list can have columns from input_scan, <element_column>
/// and <array_offset_column>.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedArrayScanProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedScanProto>,
    #[prost(message, optional, boxed, tag = "2")]
    pub input_scan: ::std::option::Option<::std::boxed::Box<AnyResolvedScanProto>>,
    #[prost(message, optional, boxed, tag = "3")]
    pub array_expr: ::std::option::Option<::std::boxed::Box<AnyResolvedExprProto>>,
    #[prost(message, optional, tag = "4")]
    pub element_column: ::std::option::Option<ResolvedColumnProto>,
    #[prost(message, optional, tag = "5")]
    pub array_offset_column: ::std::option::Option<ResolvedColumnHolderProto>,
    #[prost(message, optional, boxed, tag = "6")]
    pub join_expr: ::std::option::Option<::std::boxed::Box<AnyResolvedExprProto>>,
    #[prost(bool, optional, tag = "7")]
    pub is_outer: ::std::option::Option<bool>,
}
/// This wrapper is used for an optional ResolvedColumn inside another node.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedColumnHolderProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedArgumentProto>,
    #[prost(message, optional, tag = "2")]
    pub column: ::std::option::Option<ResolvedColumnProto>,
}
/// Scan rows from input_scan, and emit all rows where filter_expr
/// evaluates to true.  filter_expr is always of type bool.
/// This node's column_list will be a subset of input_scan's column_list.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedFilterScanProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedScanProto>,
    #[prost(message, optional, boxed, tag = "2")]
    pub input_scan: ::std::option::Option<::std::boxed::Box<AnyResolvedScanProto>>,
    #[prost(message, optional, boxed, tag = "3")]
    pub filter_expr: ::std::option::Option<::std::boxed::Box<AnyResolvedExprProto>>,
}
/// List of group by columns that form a grouping set.
///
/// Columns must come from group_by_list in ResolvedAggregateScan.
/// group_by_column_list will not contain any duplicates. There may be more
/// than one ResolvedGroupingSet in the ResolvedAggregateScan with the same
/// columns, however.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedGroupingSetProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedArgumentProto>,
    #[prost(message, repeated, tag = "2")]
    pub group_by_column_list: ::std::vec::Vec<ResolvedColumnRefProto>,
}
/// Base class for aggregation scans. Apply aggregation to rows produced from
/// input_scan, and output aggregated rows.
///
/// Group by keys in <group_by_list>.  If <group_by_list> is empty,
/// aggregate all input rows into one output row.
///
/// Compute all aggregations in <aggregate_list>.  All expressions in
/// <aggregate_list> have a ResolvedAggregateFunctionCall with mode
/// Function::AGGREGATE as their outermost node.
///
/// The output <column_list> contains only columns produced from
/// <group_by_list> and <aggregate_list>.  No other columns are visible after
/// aggregation.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AnyResolvedAggregateScanBaseProto {
    #[prost(oneof = "any_resolved_aggregate_scan_base_proto::Node", tags = "25")]
    pub node: ::std::option::Option<any_resolved_aggregate_scan_base_proto::Node>,
}
pub mod any_resolved_aggregate_scan_base_proto {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Node {
        #[prost(message, tag = "25")]
        ResolvedAggregateScanNode(Box<super::ResolvedAggregateScanProto>),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedAggregateScanBaseProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedScanProto>,
    #[prost(message, optional, boxed, tag = "2")]
    pub input_scan: ::std::option::Option<::std::boxed::Box<AnyResolvedScanProto>>,
    #[prost(message, repeated, tag = "3")]
    pub group_by_list: ::std::vec::Vec<ResolvedComputedColumnProto>,
    #[prost(message, repeated, tag = "4")]
    pub aggregate_list: ::std::vec::Vec<ResolvedComputedColumnProto>,
}
/// Apply aggregation to rows produced from input_scan, and output aggregated
/// rows.
///
/// For each item in <grouping_set_list>, output additional rows computing the
/// same <aggregate_list> over the input rows using a particular grouping set.
/// The aggregation input values, including <input_scan>, computed columns in
/// <group_by_list>, and aggregate function arguments in <aggregate_list>,
/// should be computed just once and then reused as aggregation input for each
/// grouping set. (This ensures that ROLLUP rows have correct totals, even
/// with non-stable functions in the input.) For each grouping set, the
/// <group_by_list> elements not included in the <group_by_column_list> are
/// replaced with NULL.
///
/// <rollup_column_list> is the original list of columns from
/// GROUP BY ROLLUP(...), if there was a ROLLUP clause, and is used only for
/// rebuilding equivalent SQL for the resolved AST. Engines should refer to
/// <grouping_set_list> rather than <rollup_column_list>.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedAggregateScanProto {
    #[prost(message, optional, boxed, tag = "1")]
    pub parent: ::std::option::Option<::std::boxed::Box<ResolvedAggregateScanBaseProto>>,
    #[prost(message, repeated, tag = "5")]
    pub grouping_set_list: ::std::vec::Vec<ResolvedGroupingSetProto>,
    #[prost(message, repeated, tag = "6")]
    pub rollup_column_list: ::std::vec::Vec<ResolvedColumnRefProto>,
}
/// This is one input item in a ResolvedSetOperation.
/// The <output_column_list> matches 1:1 with the ResolvedSetOperation's
/// <column_list> and specifies how columns from <scan> map to output columns.
/// Each column from <scan> can map to zero or more output columns.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedSetOperationItemProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedArgumentProto>,
    #[prost(message, optional, tag = "2")]
    pub scan: ::std::option::Option<AnyResolvedScanProto>,
    #[prost(message, repeated, tag = "3")]
    pub output_column_list: ::std::vec::Vec<ResolvedColumnProto>,
}
/// Apply a set operation (specified by <op_type>) on two or more input scans.
///
/// <scan_list> will have at least two elements.
///
/// <column_list> is a set of new ResolvedColumns created by this scan.
/// Each input ResolvedSetOperationItem has an <output_column_list> which
/// matches 1:1 with <column_list> and specifies how the input <scan>'s
/// columns map into the final <column_list>.
///
/// - Results of {UNION, INTERSECT, EXCEPT} ALL can include duplicate rows.
///   More precisely, with two input scans, if a given row R appears exactly
///   m times in first input and n times in second input (m >= 0, n >= 0):
///   For UNION ALL, R will appear exactly m + n times in the result.
///   For INTERSECT ALL, R will appear exactly min(m, n) in the result.
///   For EXCEPT ALL, R will appear exactly max(m - n, 0) in the result.
///
/// - Results of {UNION, INTERSECT, EXCEPT} DISTINCT cannot contain any
///   duplicate rows. For UNION and INTERSECT, the DISTINCT is computed
///   after the result above is computed.  For EXCEPT DISTINCT, row R will
///   appear once in the output if m > 0 and n = 0.
///
/// - For n (>2) input scans, the above operations generalize so the output is
///   the same as if the inputs were combined incrementally from left to right.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedSetOperationScanProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedScanProto>,
    #[prost(
        enumeration = "resolved_set_operation_scan_enums::SetOperationType",
        optional,
        tag = "2"
    )]
    pub op_type: ::std::option::Option<i32>,
    #[prost(message, repeated, tag = "4")]
    pub input_item_list: ::std::vec::Vec<ResolvedSetOperationItemProto>,
}
/// Apply ordering to rows produced from input_scan, and output ordered
/// rows.
///
/// The <order_by_item_list> must not be empty.  Each element identifies
/// a sort column and indicates direction (ascending or descending).
///
/// Order Preservation:
///   A ResolvedScan produces an ordered output if it has <is_ordered>=true.
///   If <is_ordered>=false, the scan may discard order.  This can happen
///   even for a ResolvedOrderByScan, if it is the top-level scan in a
///   subquery (which discards order).
///
/// The following Scan nodes may have <is_ordered>=true, producing or
/// propagating an ordering:
///   * ResolvedOrderByScan
///   * ResolvedLimitOffsetScan
///   * ResolvedProjectScan
///   * ResolvedWithScan
/// Other Scan nodes will always discard ordering.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedOrderByScanProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedScanProto>,
    #[prost(message, optional, boxed, tag = "2")]
    pub input_scan: ::std::option::Option<::std::boxed::Box<AnyResolvedScanProto>>,
    #[prost(message, repeated, tag = "3")]
    pub order_by_item_list: ::std::vec::Vec<ResolvedOrderByItemProto>,
}
/// Apply a LIMIT and optional OFFSET to the rows from input_scan. Emit all
/// rows after OFFSET rows have been scanned and up to LIMIT total rows
/// emitted. The offset is the number of rows to skip.
/// E.g., OFFSET 1 means to skip one row, so the first row emitted will be the
/// second ROW, provided the LIMIT is greater than zero.
///
/// The arguments to LIMIT <int64> OFFSET <int64> must be non-negative
/// integer literals or (possibly casted) query parameters.  Query
/// parameter values must be checked at run-time by ZetaSQL compliant
/// backend systems.
///
/// OFFSET is optional and the absence of OFFSET implies OFFSET 0.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedLimitOffsetScanProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedScanProto>,
    #[prost(message, optional, boxed, tag = "2")]
    pub input_scan: ::std::option::Option<::std::boxed::Box<AnyResolvedScanProto>>,
    #[prost(message, optional, boxed, tag = "3")]
    pub limit: ::std::option::Option<::std::boxed::Box<AnyResolvedExprProto>>,
    #[prost(message, optional, boxed, tag = "4")]
    pub offset: ::std::option::Option<::std::boxed::Box<AnyResolvedExprProto>>,
}
/// Scan the subquery defined in a WITH statement.
/// See ResolvedWithScan for more detail.
/// The column_list produced here will match 1:1 with the column_list produced
/// by the referenced subquery and will given a new unique name to each
/// column produced for this scan.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedWithRefScanProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedScanProto>,
    #[prost(string, optional, tag = "2")]
    pub with_query_name: ::std::option::Option<std::string::String>,
}
/// Apply analytic functions to rows produced from input_scan.
///
/// The set of analytic functions are partitioned into a list of analytic
/// function groups <function_group_list> by the window PARTITION BY and the
/// window ORDER BY.
///
/// The output <column_list> contains all columns from <input_scan>,
/// one column per analytic function. It may also conain partitioning/ordering
/// expression columns if they reference to select columns.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedAnalyticScanProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedScanProto>,
    #[prost(message, optional, boxed, tag = "2")]
    pub input_scan: ::std::option::Option<::std::boxed::Box<AnyResolvedScanProto>>,
    #[prost(message, repeated, tag = "3")]
    pub function_group_list: ::std::vec::Vec<ResolvedAnalyticFunctionGroupProto>,
}
/// Samples rows from <input_scan>.
/// Specs: (broken link)
/// Specs for WITH WEIGHT and PARTITION BY: (broken link)
///
/// <method> is the identifier for the sampling algorithm and will always be
/// in lowercase.
/// For example BERNOULLI, RESERVOIR, SYSTEM. Engines can also support their
/// own implementation-specific set of sampling algorithms.
///
/// <size> and <unit> specifies the sample size.
/// If <unit> is "ROWS", <size> must be an <int64> and non-negative.
/// If <unit> is "PERCENT", <size> must either be a <double> or an <int64> and
/// in the range [0, 100].
/// <size> can only be a literal value or a (possibly casted) parameter.
///
/// <repeatable_argument> is present if we had a REPEATABLE(<argument>) in the
/// TABLESAMPLE clause and can only be a literal value or a (possibly
/// casted) parameter.
///
/// If present, <weight_column> defines the column produced by this scan that
/// stores the scaling weight for the corresponding sampled row.
///
/// <partition_by_list> can be empty. If <partition_by_list> is not empty,
/// <unit> must be ROWS and <method> must be RESERVOIR.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedSampleScanProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedScanProto>,
    #[prost(message, optional, boxed, tag = "2")]
    pub input_scan: ::std::option::Option<::std::boxed::Box<AnyResolvedScanProto>>,
    #[prost(string, optional, tag = "3")]
    pub method: ::std::option::Option<std::string::String>,
    #[prost(message, optional, boxed, tag = "4")]
    pub size: ::std::option::Option<::std::boxed::Box<AnyResolvedExprProto>>,
    #[prost(
        enumeration = "resolved_sample_scan_enums::SampleUnit",
        optional,
        tag = "5"
    )]
    pub unit: ::std::option::Option<i32>,
    #[prost(message, optional, boxed, tag = "6")]
    pub repeatable_argument: ::std::option::Option<::std::boxed::Box<AnyResolvedExprProto>>,
    #[prost(message, optional, tag = "7")]
    pub weight_column: ::std::option::Option<ResolvedColumnHolderProto>,
    #[prost(message, repeated, tag = "8")]
    pub partition_by_list: ::std::vec::Vec<AnyResolvedExprProto>,
}
/// This is used when an expression is computed and given a name (a new
/// ResolvedColumn) that can be referenced elsewhere.  The new ResolvedColumn
/// can appear in a column_list or in ResolvedColumnRefs in other expressions,
/// when appropriate.  This node is not an expression itself - it is a
/// container that holds an expression.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedComputedColumnProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedArgumentProto>,
    #[prost(message, optional, tag = "2")]
    pub column: ::std::option::Option<ResolvedColumnProto>,
    #[prost(message, optional, tag = "3")]
    pub expr: ::std::option::Option<AnyResolvedExprProto>,
}
/// This represents one column of an ORDER BY clause, with the requested
/// ordering direction.
///
/// <collation_name> indicates the COLLATE specific rules of ordering.
/// If non-NULL, must be a string literal or a string parameter.
/// See (broken link).
///
/// <null_order> indicates the ordering of NULL values relative to non-NULL
/// values. NULLS_FIRST indicates that NULLS sort prior to non-NULL values,
/// and NULLS_LAST indicates that NULLS sort after non-NULL values.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedOrderByItemProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedArgumentProto>,
    #[prost(message, optional, tag = "2")]
    pub column_ref: ::std::option::Option<ResolvedColumnRefProto>,
    #[prost(message, optional, tag = "3")]
    pub collation_name: ::std::option::Option<AnyResolvedExprProto>,
    #[prost(bool, optional, tag = "4")]
    pub is_descending: ::std::option::Option<bool>,
    #[prost(
        enumeration = "resolved_order_by_item_enums::NullOrderMode",
        optional,
        tag = "5"
    )]
    pub null_order: ::std::option::Option<i32>,
}
/// This is used in CREATE TABLE statements to provide column annotations
/// such as NOT NULL and OPTIONS().
///
/// This class is recursive. It mirrors the structure of the column type
/// except that child_list might be truncated.
///
/// For ARRAY:
///   If the element or its subfield has annotations, then child_list.size()
///   is 1, and child_list(0) stores the element annotations.
///   Otherwise child_list is empty.
/// For STRUCT:
///   If the i-th field has annotations then child_list(i) stores the
///   field annotations.
///   Otherwise either child_list.size() <= i or child_list(i) is trivial.
///   If none of the fields and none of their subfields has annotations, then
///   child_list is empty.
/// For other types, child_list is empty.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedColumnAnnotationsProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedArgumentProto>,
    #[prost(bool, optional, tag = "2")]
    pub not_null: ::std::option::Option<bool>,
    #[prost(message, repeated, tag = "3")]
    pub option_list: ::std::vec::Vec<ResolvedOptionProto>,
    #[prost(message, repeated, tag = "4")]
    pub child_list: ::std::vec::Vec<ResolvedColumnAnnotationsProto>,
}
/// <expression> indicates the expression that defines the column. The type of
/// the expression will always match the type of the column.
///   - The <expression> can contain ResolvedColumnRefs corresponding to
///   ResolvedColumnDefinition.<column> for any of the
///   ResolvedColumnDefinitions in the enclosing statement.
///   - The expression can never include a subquery.
///
/// <is_stored> indicates whether the value of the expression should be
/// pre-emptively computed to save work at read time. When is_stored is true,
/// <expression> cannot contain a volatile function (e.g. RAND).
///
/// <is_on_write> indicates that the value of this column should be calculated
/// at write time. As opposed to <is_stored> the <expression> can contain
/// volatile functions (e.g. RAND).
///
/// Only one of <is_stored> or <is_on_write> can be true.
///
/// See (broken link).
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedGeneratedColumnInfoProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedArgumentProto>,
    #[prost(message, optional, tag = "2")]
    pub expression: ::std::option::Option<AnyResolvedExprProto>,
    #[prost(bool, optional, tag = "3")]
    pub is_stored: ::std::option::Option<bool>,
    #[prost(bool, optional, tag = "4")]
    pub is_on_write: ::std::option::Option<bool>,
}
/// This is used in CREATE TABLE statements to provide an explicit column
/// definition.
///
/// if <is_hidden> is TRUE, then the column won't show up in SELECT * queries.
///
/// if <generated_column_info> is non-NULL, then this table column is a
/// generated column.
///
/// <column> defines an ID for the column, which may appear in expressions in
/// the PARTITION BY, CLUSTER BY clause or <generated_column_info> if either
/// is present.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedColumnDefinitionProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedArgumentProto>,
    #[prost(string, optional, tag = "2")]
    pub name: ::std::option::Option<std::string::String>,
    #[prost(message, optional, tag = "3")]
    pub r#type: ::std::option::Option<TypeProto>,
    #[prost(message, optional, tag = "4")]
    pub annotations: ::std::option::Option<ResolvedColumnAnnotationsProto>,
    #[prost(bool, optional, tag = "5")]
    pub is_hidden: ::std::option::Option<bool>,
    #[prost(message, optional, tag = "6")]
    pub column: ::std::option::Option<ResolvedColumnProto>,
    #[prost(message, optional, tag = "7")]
    pub generated_column_info: ::std::option::Option<ResolvedGeneratedColumnInfoProto>,
}
/// This represents the PRIMARY KEY constraint on a table.
/// <column_offset_list> provides the offsets of the column definitions that
///                      comprise the primary key. This is empty when a
///                      0-element primary key is defined.
///
/// <unenforced> specifies whether the constraint is unenforced.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedPrimaryKeyProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedArgumentProto>,
    #[prost(int64, repeated, packed = "false", tag = "2")]
    pub column_offset_list: ::std::vec::Vec<i64>,
    #[prost(message, repeated, tag = "3")]
    pub option_list: ::std::vec::Vec<ResolvedOptionProto>,
    #[prost(bool, optional, tag = "4")]
    pub unenforced: ::std::option::Option<bool>,
}
/// This represents the FOREIGN KEY constraint on a table. It is of the form:
///
///   CONSTRAINT <constraint_name>
///   FOREIGN KEY <referencing_column_offset_list>
///   REFERENCES <referenced_table> <referenced_column_offset_list>
///   <match_mode>
///   <update_action>
///   <delete_action>
///   <enforced>
///   <option_list>
///
/// <constraint_name> uniquely identifies the constraint.
///
/// <referencing_column_offset_list> provides the offsets of the column
/// definitions for the table defining the foreign key.
///
/// <referenced_table> identifies the table this constraint references.
///
/// <referenced_column_offset_list> provides the offsets of the column
/// definitions for the table referenced by the foreign key.
///
/// <match_mode> specifies how referencing keys with null values are handled.
///
/// <update_action> specifies what action to take, if any, when a referenced
/// value is updated.
///
/// <delete_action> specifies what action to take, if any, when a row with a
/// referenced values is deleted.
///
/// <enforced> specifies whether or not the constraint is enforced.
///
/// <option_list> for foreign key table constraints. Empty for foreign key
/// column attributes (see instead ResolvedColumnAnnotations).
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedForeignKeyProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedArgumentProto>,
    #[prost(string, optional, tag = "2")]
    pub constraint_name: ::std::option::Option<std::string::String>,
    #[prost(int64, repeated, packed = "false", tag = "3")]
    pub referencing_column_offset_list: ::std::vec::Vec<i64>,
    #[prost(message, optional, tag = "4")]
    pub referenced_table: ::std::option::Option<TableRefProto>,
    #[prost(int64, repeated, packed = "false", tag = "5")]
    pub referenced_column_offset_list: ::std::vec::Vec<i64>,
    #[prost(
        enumeration = "resolved_foreign_key_enums::MatchMode",
        optional,
        tag = "6"
    )]
    pub match_mode: ::std::option::Option<i32>,
    #[prost(
        enumeration = "resolved_foreign_key_enums::ActionOperation",
        optional,
        tag = "7"
    )]
    pub update_action: ::std::option::Option<i32>,
    #[prost(
        enumeration = "resolved_foreign_key_enums::ActionOperation",
        optional,
        tag = "8"
    )]
    pub delete_action: ::std::option::Option<i32>,
    #[prost(bool, optional, tag = "9")]
    pub enforced: ::std::option::Option<bool>,
    #[prost(message, repeated, tag = "10")]
    pub option_list: ::std::vec::Vec<ResolvedOptionProto>,
}
/// This represents the CHECK constraint on a table. It is of the form:
///
///   CONSTRAINT <constraint_name>
///   CHECK <expression>
///   <enforced>
///   <option_list>
///
/// <constraint_name> uniquely identifies the constraint.
///
/// <expression> defines a boolean expression to be evaluated when the row is
/// updated. If the result is FALSE, update to the row is not allowed.
///
/// <enforced> specifies whether or not the constraint is enforced.
///
/// <option_list> list of options for check constraint.
///
/// See (broken link).
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedCheckConstraintProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedArgumentProto>,
    #[prost(string, optional, tag = "2")]
    pub constraint_name: ::std::option::Option<std::string::String>,
    #[prost(message, optional, tag = "3")]
    pub expression: ::std::option::Option<AnyResolvedExprProto>,
    #[prost(bool, optional, tag = "4")]
    pub enforced: ::std::option::Option<bool>,
    #[prost(message, repeated, tag = "5")]
    pub option_list: ::std::vec::Vec<ResolvedOptionProto>,
}
/// This is used in ResolvedQueryStmt to provide a user-visible name
/// for each output column.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedOutputColumnProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedArgumentProto>,
    #[prost(string, optional, tag = "2")]
    pub name: ::std::option::Option<std::string::String>,
    #[prost(message, optional, tag = "3")]
    pub column: ::std::option::Option<ResolvedColumnProto>,
}
/// A Map node computes new expression values, and possibly drops
/// columns from the input Scan's column_list.
///
/// Each entry in <expr_list> is a new column computed from an expression.
///
/// The column_list can include any columns from input_scan, plus these
/// newly computed columns.
///
/// NOTE: This scan will propagate the is_ordered property of <input_scan>
/// by default.  To make this scan unordered, call set_is_ordered(false).
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedProjectScanProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedScanProto>,
    #[prost(message, repeated, tag = "2")]
    pub expr_list: ::std::vec::Vec<ResolvedComputedColumnProto>,
    #[prost(message, optional, boxed, tag = "3")]
    pub input_scan: ::std::option::Option<::std::boxed::Box<AnyResolvedScanProto>>,
}
/// This scan represents a call to a table-valued function (TVF). Each TVF
/// returns an entire output relation instead of a single scalar value. The
/// enclosing query may refer to the TVF as if it were a table subquery. The
/// TVF may accept scalar arguments and/or other input relations.
///
/// Scalar arguments work the same way as arguments for non-table-valued
/// functions: in the resolved AST, their types are equal to the required
/// argument types specified in the function signature.
///
/// The function signature may also include relation arguments, and any such
/// relation argument may specify a required schema. If such a required schema
/// is present, then in the resolved AST, the ResolvedScan for each relational
/// ResolvedTVFArgument is guaranteed to have the same number of columns as
/// the required schema, and the provided columns match position-wise with the
/// required columns. Each provided column has the same name and type as the
/// corresponding required column.
///
/// <column_list> is a set of new ResolvedColumns created by this scan.
/// These output columns match positionally with the columns in the output
/// schema of <signature>.
///
/// <tvf> The TableValuedFunction entry that the catalog returned for this TVF
///       scan. Contains non-concrete function signatures which may include
///       arguments with templated types.
/// <signature> The concrete table function signature for this TVF call,
///             including the types of all scalar arguments and the
///             number and types of columns of all table-valued
///             arguments. An engine may also subclass this object to
///             provide extra custom information and return an instance
///             of the subclass from the TableValuedFunction::Resolve
///             method.
/// <argument_list> The vector of resolved arguments for this TVF call.
/// <alias> The AS alias for the scan, or empty if none.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedTvfScanProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedScanProto>,
    #[prost(message, optional, tag = "2")]
    pub tvf: ::std::option::Option<TableValuedFunctionRefProto>,
    #[prost(message, optional, tag = "3")]
    pub signature: ::std::option::Option<TvfSignatureProto>,
    #[prost(message, repeated, tag = "5")]
    pub argument_list: ::std::vec::Vec<ResolvedTvfArgumentProto>,
    #[prost(string, optional, tag = "6")]
    pub alias: ::std::option::Option<std::string::String>,
}
/// This represents an argument to a table-valued function (TVF). The argument
/// can be semantically scalar, relational, represent a model, a connection or
/// a descriptor. Only one of the five fields will be set.
///
/// <expr> The expression representing a scalar TVF argument.
/// <scan> The scan representing a relational TVF argument.
/// <model> The model representing an ML model TVF argument.
/// <connection> The connection representing a connection object TVF argument.
/// <descriptor_arg> The descriptor representing a descriptor object TVF
/// argument.
///
/// <argument_column_list> maps columns from <scan> into specific columns
/// of the TVF argument's input schema, matching those columns positionally.
/// i.e. <scan>'s column_list may have fewer columns or out-of-order columns,
/// and this vector maps those columns into specific TVF input columns.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedTvfArgumentProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedArgumentProto>,
    #[prost(message, optional, tag = "2")]
    pub expr: ::std::option::Option<AnyResolvedExprProto>,
    #[prost(message, optional, tag = "3")]
    pub scan: ::std::option::Option<AnyResolvedScanProto>,
    #[prost(message, optional, tag = "5")]
    pub model: ::std::option::Option<ResolvedModelProto>,
    #[prost(message, optional, tag = "6")]
    pub connection: ::std::option::Option<ResolvedConnectionProto>,
    #[prost(message, optional, tag = "7")]
    pub descriptor_arg: ::std::option::Option<ResolvedDescriptorProto>,
    #[prost(message, repeated, tag = "4")]
    pub argument_column_list: ::std::vec::Vec<ResolvedColumnProto>,
}
/// The superclass of all ZetaSQL statements.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AnyResolvedStatementProto {
    #[prost(
        oneof = "any_resolved_statement_proto::Node",
        tags = "37, 38, 39, 43, 44, 45, 46, 47, 48, 49, 50, 63, 64, 66, 68, 71, 72, 73, 74, 80, 83, 86, 87, 95, 98, 101, 114, 120, 121, 122, 123, 124, 133, 140, 142"
    )]
    pub node: ::std::option::Option<any_resolved_statement_proto::Node>,
}
pub mod any_resolved_statement_proto {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Node {
        #[prost(message, tag = "37")]
        ResolvedExplainStmtNode(Box<super::ResolvedExplainStmtProto>),
        #[prost(message, tag = "38")]
        ResolvedQueryStmtNode(super::ResolvedQueryStmtProto),
        #[prost(message, tag = "39")]
        ResolvedCreateStatementNode(super::AnyResolvedCreateStatementProto),
        #[prost(message, tag = "43")]
        ResolvedExportDataStmtNode(super::ResolvedExportDataStmtProto),
        #[prost(message, tag = "44")]
        ResolvedDefineTableStmtNode(super::ResolvedDefineTableStmtProto),
        #[prost(message, tag = "45")]
        ResolvedDescribeStmtNode(super::ResolvedDescribeStmtProto),
        #[prost(message, tag = "46")]
        ResolvedShowStmtNode(super::ResolvedShowStmtProto),
        #[prost(message, tag = "47")]
        ResolvedBeginStmtNode(super::ResolvedBeginStmtProto),
        #[prost(message, tag = "48")]
        ResolvedCommitStmtNode(super::ResolvedCommitStmtProto),
        #[prost(message, tag = "49")]
        ResolvedRollbackStmtNode(super::ResolvedRollbackStmtProto),
        #[prost(message, tag = "50")]
        ResolvedDropStmtNode(super::ResolvedDropStmtProto),
        #[prost(message, tag = "63")]
        ResolvedInsertStmtNode(super::ResolvedInsertStmtProto),
        #[prost(message, tag = "64")]
        ResolvedDeleteStmtNode(super::ResolvedDeleteStmtProto),
        #[prost(message, tag = "66")]
        ResolvedUpdateStmtNode(super::ResolvedUpdateStmtProto),
        #[prost(message, tag = "68")]
        ResolvedGrantOrRevokeStmtNode(super::AnyResolvedGrantOrRevokeStmtProto),
        #[prost(message, tag = "71")]
        ResolvedAlterTableSetOptionsStmtNode(super::ResolvedAlterTableSetOptionsStmtProto),
        #[prost(message, tag = "72")]
        ResolvedRenameStmtNode(super::ResolvedRenameStmtProto),
        #[prost(message, tag = "73")]
        ResolvedCreateRowAccessPolicyStmtNode(super::ResolvedCreateRowAccessPolicyStmtProto),
        #[prost(message, tag = "74")]
        ResolvedDropRowAccessPolicyStmtNode(super::ResolvedDropRowAccessPolicyStmtProto),
        #[prost(message, tag = "80")]
        ResolvedDropFunctionStmtNode(super::ResolvedDropFunctionStmtProto),
        #[prost(message, tag = "83")]
        ResolvedCallStmtNode(super::ResolvedCallStmtProto),
        #[prost(message, tag = "86")]
        ResolvedImportStmtNode(super::ResolvedImportStmtProto),
        #[prost(message, tag = "87")]
        ResolvedModuleStmtNode(super::ResolvedModuleStmtProto),
        #[prost(message, tag = "95")]
        ResolvedCreateDatabaseStmtNode(super::ResolvedCreateDatabaseStmtProto),
        #[prost(message, tag = "98")]
        ResolvedAssertStmtNode(super::ResolvedAssertStmtProto),
        #[prost(message, tag = "101")]
        ResolvedMergeStmtNode(super::ResolvedMergeStmtProto),
        #[prost(message, tag = "114")]
        ResolvedAlterObjectStmtNode(super::AnyResolvedAlterObjectStmtProto),
        #[prost(message, tag = "120")]
        ResolvedSetTransactionStmtNode(super::ResolvedSetTransactionStmtProto),
        #[prost(message, tag = "121")]
        ResolvedDropMaterializedViewStmtNode(super::ResolvedDropMaterializedViewStmtProto),
        #[prost(message, tag = "122")]
        ResolvedStartBatchStmtNode(super::ResolvedStartBatchStmtProto),
        #[prost(message, tag = "123")]
        ResolvedRunBatchStmtNode(super::ResolvedRunBatchStmtProto),
        #[prost(message, tag = "124")]
        ResolvedAbortBatchStmtNode(super::ResolvedAbortBatchStmtProto),
        #[prost(message, tag = "133")]
        ResolvedTruncateStmtNode(super::ResolvedTruncateStmtProto),
        #[prost(message, tag = "140")]
        ResolvedExecuteImmediateStmtNode(super::ResolvedExecuteImmediateStmtProto),
        #[prost(message, tag = "142")]
        ResolvedAssignmentStmtNode(super::ResolvedAssignmentStmtProto),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedStatementProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedNodeProto>,
    #[prost(message, repeated, tag = "2")]
    pub hint_list: ::std::vec::Vec<ResolvedOptionProto>,
}
/// An Explain statement. This is always the root of a statement hierarchy.
/// Its child may be any statement type except another ResolvedExplainStmt.
///
/// It is implementation dependent what action a back end system takes for an
/// ExplainStatement.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedExplainStmtProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedStatementProto>,
    #[prost(message, optional, boxed, tag = "2")]
    pub statement: ::std::option::Option<::std::boxed::Box<AnyResolvedStatementProto>>,
}
/// A SQL query statement.  This is the outermost query statement that runs
/// and produces rows of output, like a SELECT.  (The contained query may be
/// a Scan corresponding to a non-Select top-level operation like UNION ALL
/// or WITH.)
///
/// <output_column_list> gives the user-visible column names that should be
/// returned in the API or query tools.  There may be duplicate names, and
/// multiple output columns may reference the same column from <query>.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedQueryStmtProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedStatementProto>,
    #[prost(message, repeated, tag = "2")]
    pub output_column_list: ::std::vec::Vec<ResolvedOutputColumnProto>,
    /// If true, the result of this query is a value table. Rather than
    /// producing rows with named columns, it produces rows with a single
    /// unnamed value type.  output_column_list will have exactly one
    /// column, with an empty name. See (broken link).
    #[prost(bool, optional, tag = "3")]
    pub is_value_table: ::std::option::Option<bool>,
    #[prost(message, optional, tag = "4")]
    pub query: ::std::option::Option<AnyResolvedScanProto>,
}
/// This statement:
///   CREATE DATABASE <name> [OPTIONS (...)]
/// <name_path> is a vector giving the identifier path in the database name.
/// <option_list> specifies the options of the database.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedCreateDatabaseStmtProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedStatementProto>,
    #[prost(string, repeated, tag = "2")]
    pub name_path: ::std::vec::Vec<std::string::String>,
    #[prost(message, repeated, tag = "3")]
    pub option_list: ::std::vec::Vec<ResolvedOptionProto>,
}
/// Common superclass for CREATE statements with standard modifiers like
///         CREATE [OR REPLACE] [TEMP|TEMPORARY|PUBLIC|PRIVATE] <object type>
///         [IF NOT EXISTS] <name> ...
///
/// <name_path> is a vector giving the identifier path in the table name.
/// <create_scope> is the relevant scope, i.e., DEFAULT, TEMP, PUBLIC,
///                or PRIVATE.  PUBLIC/PRIVATE are only valid in module
///                resolution context, see (broken link)
///                for details.
/// <create_mode> indicates if this was CREATE, CREATE OR REPLACE, or
///               CREATE IF NOT EXISTS.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AnyResolvedCreateStatementProto {
    #[prost(
        oneof = "any_resolved_create_statement_proto::Node",
        tags = "76, 88, 97, 99, 106, 107, 108, 125"
    )]
    pub node: ::std::option::Option<any_resolved_create_statement_proto::Node>,
}
pub mod any_resolved_create_statement_proto {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Node {
        #[prost(message, tag = "76")]
        ResolvedCreateFunctionStmtNode(super::ResolvedCreateFunctionStmtProto),
        #[prost(message, tag = "88")]
        ResolvedCreateTableFunctionStmtNode(super::ResolvedCreateTableFunctionStmtProto),
        #[prost(message, tag = "97")]
        ResolvedCreateIndexStmtNode(super::ResolvedCreateIndexStmtProto),
        #[prost(message, tag = "99")]
        ResolvedCreateConstantStmtNode(super::ResolvedCreateConstantStmtProto),
        #[prost(message, tag = "106")]
        ResolvedCreateTableStmtBaseNode(super::AnyResolvedCreateTableStmtBaseProto),
        #[prost(message, tag = "107")]
        ResolvedCreateModelStmtNode(super::ResolvedCreateModelStmtProto),
        #[prost(message, tag = "108")]
        ResolvedCreateViewBaseNode(super::AnyResolvedCreateViewBaseProto),
        #[prost(message, tag = "125")]
        ResolvedCreateProcedureStmtNode(super::ResolvedCreateProcedureStmtProto),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedCreateStatementProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedStatementProto>,
    #[prost(string, repeated, tag = "2")]
    pub name_path: ::std::vec::Vec<std::string::String>,
    #[prost(
        enumeration = "resolved_create_statement_enums::CreateScope",
        optional,
        tag = "5"
    )]
    pub create_scope: ::std::option::Option<i32>,
    #[prost(
        enumeration = "resolved_create_statement_enums::CreateMode",
        optional,
        tag = "4"
    )]
    pub create_mode: ::std::option::Option<i32>,
}
/// Represents one of indexed items in CREATE INDEX statement, with the
/// ordering direction specified.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedIndexItemProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedArgumentProto>,
    #[prost(message, optional, tag = "2")]
    pub column_ref: ::std::option::Option<ResolvedColumnRefProto>,
    #[prost(bool, optional, tag = "3")]
    pub descending: ::std::option::Option<bool>,
}
/// This is used in CREATE INDEX STMT to represent the unnest operation
/// performed on the base table. The produced element columns or array offset
/// columns (optional) can appear in other ResolvedUnnestItem or index keys.
///
/// <array_expr> is the expression of the array field, e.g., t.array_field.
/// <element_column> is the new column produced by this unnest item that
///                  stores the array element value for each row.
/// <array_offset_column> is optional. If present, it defines the column
///                       produced by this unnest item that stores the array
///                       offset (0-based) for the corresponding
///                       <element_column>.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedUnnestItemProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedArgumentProto>,
    #[prost(message, optional, tag = "2")]
    pub array_expr: ::std::option::Option<AnyResolvedExprProto>,
    #[prost(message, optional, tag = "3")]
    pub element_column: ::std::option::Option<ResolvedColumnProto>,
    #[prost(message, optional, tag = "4")]
    pub array_offset_column: ::std::option::Option<ResolvedColumnHolderProto>,
}
/// This statement:
/// CREATE [OR REPLACE] [UNIQUE] INDEX [IF NOT EXISTS] <index_name_path>
/// ON <table_name_path>
/// [STORING (Expression, ...)]
/// [UNNEST(path_expression) [[AS] alias] [WITH OFFSET [[AS] alias]], ...]
/// (path_expression [ASC|DESC], ...) [OPTIONS (name=value, ...)];
///
/// <table_name_path> is the name of table being indexed.
/// <table_scan> is a TableScan on the table being indexed.
/// <is_unique> specifies if the index has unique entries.
/// <index_item_list> has the columns being indexed, specified as references
///                   to 'computed_columns_list' entries or the columns of
///                   'table_scan'.
/// <storing_expression_list> has the expressions in the storing clause.
/// <option_list> has engine-specific directives for how and where to
///               materialize this index.
/// <computed_columns_list> has computed columns derived from the columns of
///                         'table_scan' or 'unnest_expressions_list'. For
///                         example, the extracted field (e.g., x.y.z).
/// <unnest_expressions_list> has unnest expressions derived from
///                           'table_scan' or previous unnest expressions in
///                           the list. So the list order is significant.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedCreateIndexStmtProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedCreateStatementProto>,
    #[prost(string, repeated, tag = "2")]
    pub table_name_path: ::std::vec::Vec<std::string::String>,
    #[prost(message, optional, tag = "3")]
    pub table_scan: ::std::option::Option<ResolvedTableScanProto>,
    #[prost(bool, optional, tag = "4")]
    pub is_unique: ::std::option::Option<bool>,
    #[prost(message, repeated, tag = "5")]
    pub index_item_list: ::std::vec::Vec<ResolvedIndexItemProto>,
    #[prost(message, repeated, tag = "9")]
    pub storing_expression_list: ::std::vec::Vec<AnyResolvedExprProto>,
    #[prost(message, repeated, tag = "6")]
    pub option_list: ::std::vec::Vec<ResolvedOptionProto>,
    #[prost(message, repeated, tag = "7")]
    pub computed_columns_list: ::std::vec::Vec<ResolvedComputedColumnProto>,
    #[prost(message, repeated, tag = "8")]
    pub unnest_expressions_list: ::std::vec::Vec<ResolvedUnnestItemProto>,
}
/// This statement:
///   CREATE [TEMP] TABLE <name> (column type, ...)
///   [PARTITION BY expr, ...] [CLUSTER BY expr, ...]
///   [OPTIONS (...)]
///
/// <option_list> has engine-specific directives for how and where to
///               materialize this table.
/// <column_definition_list> has the names and types of the columns in the
///                          created table. If <is_value_table> is true, it
///                          must contain exactly one column, with a generated
///                          name such as "$struct".
/// <pseudo_column_list> is a list of some pseudo-columns expected to be
///                      present on the created table (provided by
///                      AnalyzerOptions::SetDdlPseudoColumns*).  These can be
///                      referenced in expressions in <partition_by_list> and
///                      <cluster_by_list>.
/// <primary_key> specifies the PRIMARY KEY constraint on the table, it is
///               nullptr when no PRIMARY KEY is specified.
/// <foreign_key_list> specifies the FOREIGN KEY constraints on the table.
/// <check_constraint_list> specifies the CHECK constraints on the table.
/// <partition_by_list> specifies the partitioning expressions for the table.
/// <cluster_by_list> specifies the clustering expressions for the table.
/// <is_value_table> specifies whether the table is a value table.
///                  See (broken link).
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AnyResolvedCreateTableStmtBaseProto {
    #[prost(
        oneof = "any_resolved_create_table_stmt_base_proto::Node",
        tags = "40, 42, 90"
    )]
    pub node: ::std::option::Option<any_resolved_create_table_stmt_base_proto::Node>,
}
pub mod any_resolved_create_table_stmt_base_proto {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Node {
        #[prost(message, tag = "40")]
        ResolvedCreateTableAsSelectStmtNode(super::ResolvedCreateTableAsSelectStmtProto),
        #[prost(message, tag = "42")]
        ResolvedCreateExternalTableStmtNode(super::ResolvedCreateExternalTableStmtProto),
        #[prost(message, tag = "90")]
        ResolvedCreateTableStmtNode(super::ResolvedCreateTableStmtProto),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedCreateTableStmtBaseProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedCreateStatementProto>,
    #[prost(message, repeated, tag = "2")]
    pub option_list: ::std::vec::Vec<ResolvedOptionProto>,
    #[prost(message, repeated, tag = "3")]
    pub column_definition_list: ::std::vec::Vec<ResolvedColumnDefinitionProto>,
    #[prost(message, repeated, tag = "7")]
    pub pseudo_column_list: ::std::vec::Vec<ResolvedColumnProto>,
    #[prost(message, optional, tag = "4")]
    pub primary_key: ::std::option::Option<ResolvedPrimaryKeyProto>,
    #[prost(message, repeated, tag = "9")]
    pub foreign_key_list: ::std::vec::Vec<ResolvedForeignKeyProto>,
    #[prost(message, repeated, tag = "10")]
    pub check_constraint_list: ::std::vec::Vec<ResolvedCheckConstraintProto>,
    #[prost(message, repeated, tag = "5")]
    pub partition_by_list: ::std::vec::Vec<AnyResolvedExprProto>,
    #[prost(message, repeated, tag = "6")]
    pub cluster_by_list: ::std::vec::Vec<AnyResolvedExprProto>,
    #[prost(bool, optional, tag = "8")]
    pub is_value_table: ::std::option::Option<bool>,
}
/// This statement:
/// CREATE [TEMP] TABLE <name> (column schema, ...)
/// [PARTITION BY expr, ...] [CLUSTER BY expr, ...]
/// [OPTIONS (...)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedCreateTableStmtProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedCreateTableStmtBaseProto>,
}
/// This statement:
///   CREATE [TEMP] TABLE <name> [(column schema, ...)]
///   [PARTITION BY expr, ...] [CLUSTER BY expr, ...] [OPTIONS (...)]
///   AS SELECT ...
///
/// The <output_column_list> matches 1:1 with the <column_definition_list> in
/// ResolvedCreateTableStmtBase, and maps ResolvedColumns produced by <query>
/// into specific columns of the created table.  The output column names and
/// types must match the column definition names and types.  If the table is
/// a value table, <output_column_list> must have exactly one column, with a
/// generated name such as "$struct".
///
/// <output_column_list> does not contain all table schema information that
/// <column_definition_list> does. For example, NOT NULL annotations, column
/// OPTIONS, and primary keys are only available in <column_definition_list>.
/// Consumers are encouraged to read from <column_definition_list> rather
/// than than <output_column_list> to determine the table schema, if possible.
///
/// <query> is the query to run.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedCreateTableAsSelectStmtProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedCreateTableStmtBaseProto>,
    #[prost(message, repeated, tag = "2")]
    pub output_column_list: ::std::vec::Vec<ResolvedOutputColumnProto>,
    #[prost(message, optional, tag = "3")]
    pub query: ::std::option::Option<AnyResolvedScanProto>,
}
/// This statement:
///   CREATE [TEMP] MODEL <name> [TRANSFORM(...)] [OPTIONS (...)] AS SELECT ..
///
/// <option_list> has engine-specific directives for how to train this model.
/// <output_column_list> matches 1:1 with the <query>'s column_list and the
///                      <column_definition_list>, and identifies the names
///                      and types of the columns output from the select
///                      statement.
/// <query> is the select statement.
/// <transform_input_column_list> introduces new ResolvedColumns that have the
///   same names and types of the columns in the <output_column_list>. The
///   transform expressions resolve against these ResolvedColumns. It's only
///   set when <transform_list> is non-empty.
/// <transform_list> is the list of ResolvedComputedColumn in TRANSFORM
///   clause.
/// <transform_output_column_list> matches 1:1 with <transform_list> output.
///   It records the names of the output columns from TRANSFORM clause.
/// <transform_analytic_function_group_list> is the list of
///   AnalyticFunctionGroup for analytic functions inside TRANSFORM clause.
///   It records the input expression of the analytic functions. It can
///   see all the columns from <transform_input_column_list>. The only valid
///   group is for the full, unbounded window generated from empty OVER()
///   clause.
///   For example, CREATE MODEL statement
///   "create model Z
///     transform (max(c) over() as d)
///     options ()
///     as select 1 c, 2 b;"
///   will generate transform_analytic_function_group_list:
///   +-transform_analytic_function_group_list=
///     +-AnalyticFunctionGroup
///       +-analytic_function_list=
///         +-d#5 :=
///           +-AnalyticFunctionCall(ZetaSQL:max(INT64) -> INT64)
///             +-ColumnRef(type=INT64, column=Z.c#3)
///             +-window_frame=
///               +-WindowFrame(frame_unit=ROWS)
///                 +-start_expr=
///                 | +-WindowFrameExpr(boundary_type=UNBOUNDED PRECEDING)
///                 +-end_expr=
///                   +-WindowFrameExpr(boundary_type=UNBOUNDED FOLLOWING)
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedCreateModelStmtProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedCreateStatementProto>,
    #[prost(message, repeated, tag = "2")]
    pub option_list: ::std::vec::Vec<ResolvedOptionProto>,
    #[prost(message, repeated, tag = "3")]
    pub output_column_list: ::std::vec::Vec<ResolvedOutputColumnProto>,
    #[prost(message, optional, tag = "4")]
    pub query: ::std::option::Option<AnyResolvedScanProto>,
    #[prost(message, repeated, tag = "8")]
    pub transform_input_column_list: ::std::vec::Vec<ResolvedColumnDefinitionProto>,
    #[prost(message, repeated, tag = "5")]
    pub transform_list: ::std::vec::Vec<ResolvedComputedColumnProto>,
    #[prost(message, repeated, tag = "6")]
    pub transform_output_column_list: ::std::vec::Vec<ResolvedOutputColumnProto>,
    #[prost(message, repeated, tag = "7")]
    pub transform_analytic_function_group_list: ::std::vec::Vec<ResolvedAnalyticFunctionGroupProto>,
}
/// Common superclass for CREATE view/materialized view:
///   CREATE [TEMP|MATERIALIZED] VIEW <name> [OPTIONS (...)] AS SELECT ...
///
/// <option_list> has engine-specific directives for options attached to
///               this view.
/// <output_column_list> has the names and types of the columns in the
///                      created view, and maps from <query>'s column_list
///                      to these output columns.
/// <query> is the query to run.
/// <sql> is the view query text.
/// <sql_security> is the declared security mode for the function. Values
///        include 'INVOKER', 'DEFINER'.
///
/// Note that <query> and <sql> are both marked as IGNORABLE because
/// an engine could look at either one (but might not look at both).
/// An engine must look at one (and cannot ignore both) to be
/// semantically valid, but there is currently no way to enforce that.
///
/// The view must produce named columns with unique names.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AnyResolvedCreateViewBaseProto {
    #[prost(oneof = "any_resolved_create_view_base_proto::Node", tags = "41, 119")]
    pub node: ::std::option::Option<any_resolved_create_view_base_proto::Node>,
}
pub mod any_resolved_create_view_base_proto {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Node {
        #[prost(message, tag = "41")]
        ResolvedCreateViewStmtNode(super::ResolvedCreateViewStmtProto),
        #[prost(message, tag = "119")]
        ResolvedCreateMaterializedViewStmtNode(super::ResolvedCreateMaterializedViewStmtProto),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedCreateViewBaseProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedCreateStatementProto>,
    #[prost(message, repeated, tag = "2")]
    pub option_list: ::std::vec::Vec<ResolvedOptionProto>,
    #[prost(message, repeated, tag = "3")]
    pub output_column_list: ::std::vec::Vec<ResolvedOutputColumnProto>,
    #[prost(message, optional, tag = "5")]
    pub query: ::std::option::Option<AnyResolvedScanProto>,
    #[prost(string, optional, tag = "6")]
    pub sql: ::std::option::Option<std::string::String>,
    #[prost(
        enumeration = "resolved_create_statement_enums::SqlSecurity",
        optional,
        tag = "7"
    )]
    pub sql_security: ::std::option::Option<i32>,
    /// If true, this view produces a value table. Rather than producing
    /// rows with named columns, it produces rows with a single unnamed
    /// value type.  output_column_list will have exactly one column, with
    /// an empty name. See (broken link).
    #[prost(bool, optional, tag = "4")]
    pub is_value_table: ::std::option::Option<bool>,
}
/// This statement:
/// CREATE [TEMP] VIEW <name> [OPTIONS (...)] AS SELECT ...
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedCreateViewStmtProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedCreateViewBaseProto>,
}
/// This statement:
/// CREATE [TEMP] EXTERNAL TABLE <name> [(column type, ...)]
/// [PARTITION BY expr, ...] [CLUSTER BY expr, ...] OPTIONS (...)
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedCreateExternalTableStmtProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedCreateTableStmtBaseProto>,
}
/// This statement:
///   EXPORT DATA [WITH CONNECTION] <connection> (<option_list>) AS SELECT ...
/// which is used to run a query and export its result somewhere
/// without giving the result a table name.
/// <connection> connection reference for accessing destination source.
/// <option_list> has engine-specific directives for how and where to
///               materialize the query result.
/// <output_column_list> has the names and types of the columns produced by
///                      the query, and maps from <query>'s column_list
///                      to these output columns.  The engine may ignore
///                      the column names depending on the output format.
/// <query> is the query to run.
///
/// The query must produce named columns with unique names.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedExportDataStmtProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedStatementProto>,
    #[prost(message, optional, tag = "6")]
    pub connection: ::std::option::Option<ResolvedConnectionProto>,
    #[prost(message, repeated, tag = "2")]
    pub option_list: ::std::vec::Vec<ResolvedOptionProto>,
    #[prost(message, repeated, tag = "3")]
    pub output_column_list: ::std::vec::Vec<ResolvedOutputColumnProto>,
    /// If true, the result of this query is a value table. Rather than
    /// producing rows with named columns, it produces rows with a single
    /// unnamed value type.  output_column_list will have exactly one
    /// column, with an empty name. See (broken link).
    #[prost(bool, optional, tag = "4")]
    pub is_value_table: ::std::option::Option<bool>,
    #[prost(message, optional, tag = "5")]
    pub query: ::std::option::Option<AnyResolvedScanProto>,
}
/// This statement: DEFINE TABLE name (...);
///
/// <name_path> is a vector giving the identifier path in the table name.
/// <option_list> has engine-specific options of how the table is defined.
///
/// DEFINE TABLE normally has the same effect as CREATE TEMP EXTERNAL TABLE.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedDefineTableStmtProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedStatementProto>,
    #[prost(string, repeated, tag = "2")]
    pub name_path: ::std::vec::Vec<std::string::String>,
    #[prost(message, repeated, tag = "3")]
    pub option_list: ::std::vec::Vec<ResolvedOptionProto>,
}
/// This statement: DESCRIBE [<object_type>] <name> [FROM <from_name_path>];
///
/// <object_type> is an optional string identifier,
///               e.g., "INDEX", "FUNCTION", "TYPE", etc.
/// <name_path> is a vector giving the identifier path for the object to be
///             described.
/// <from_name_path> is an optional vector giving the identifier path of a
///                    containing object, e.g. a table.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedDescribeStmtProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedStatementProto>,
    #[prost(string, optional, tag = "2")]
    pub object_type: ::std::option::Option<std::string::String>,
    #[prost(string, repeated, tag = "3")]
    pub name_path: ::std::vec::Vec<std::string::String>,
    #[prost(string, repeated, tag = "4")]
    pub from_name_path: ::std::vec::Vec<std::string::String>,
}
/// This statement: SHOW <identifier> [FROM <name_path>] [LIKE <like_expr>];
///
/// <identifier> is a string that determines the type of objects to be shown,
///              e.g., TABLES, COLUMNS, INDEXES, STATUS,
/// <name_path> is an optional path to an object from which <identifier>
///             objects will be shown, e.g., if <identifier> = INDEXES and
///             <name> = table_name, the indexes of "table_name" will be
///             shown,
/// <like_expr> is an optional ResolvedLiteral of type string that if present
///             restricts the objects shown to have a name like this string.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedShowStmtProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedStatementProto>,
    #[prost(string, optional, tag = "2")]
    pub identifier: ::std::option::Option<std::string::String>,
    #[prost(string, repeated, tag = "3")]
    pub name_path: ::std::vec::Vec<std::string::String>,
    #[prost(message, optional, tag = "4")]
    pub like_expr: ::std::option::Option<ResolvedLiteralProto>,
}
/// This statement: BEGIN [TRANSACTION] [ <transaction_mode> [, ...] ]
///
/// Where transaction_mode is one of:
///      READ ONLY
///      READ WRITE
///      <isolation_level>
///
/// <isolation_level> is a string vector storing the identifiers after
///       ISOLATION LEVEL. The strings inside vector could be one of the
///       SQL standard isolation levels:
///
///                   READ UNCOMMITTED
///                   READ COMMITTED
///                   READ REPEATABLE
///                   SERIALIZABLE
///
///       or could be arbitrary strings. ZetaSQL does not validate that
///       the string is valid.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedBeginStmtProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedStatementProto>,
    #[prost(
        enumeration = "resolved_begin_stmt_enums::ReadWriteMode",
        optional,
        tag = "3"
    )]
    pub read_write_mode: ::std::option::Option<i32>,
    #[prost(string, repeated, tag = "2")]
    pub isolation_level_list: ::std::vec::Vec<std::string::String>,
}
/// This statement: SET TRANSACTION <transaction_mode> [, ...]
///
/// Where transaction_mode is one of:
///      READ ONLY
///      READ WRITE
///      <isolation_level>
///
/// <isolation_level> is a string vector storing the identifiers after
///       ISOLATION LEVEL. The strings inside vector could be one of the
///       SQL standard isolation levels:
///
///                   READ UNCOMMITTED
///                   READ COMMITTED
///                   READ REPEATABLE
///                   SERIALIZABLE
///
///       or could be arbitrary strings. ZetaSQL does not validate that
///       the string is valid.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedSetTransactionStmtProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedStatementProto>,
    #[prost(
        enumeration = "resolved_begin_stmt_enums::ReadWriteMode",
        optional,
        tag = "3"
    )]
    pub read_write_mode: ::std::option::Option<i32>,
    #[prost(string, repeated, tag = "2")]
    pub isolation_level_list: ::std::vec::Vec<std::string::String>,
}
/// This statement: COMMIT [TRANSACTION];
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedCommitStmtProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedStatementProto>,
}
/// This statement: ROLLBACK [TRANSACTION];
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedRollbackStmtProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedStatementProto>,
}
/// This statement: START BATCH [<batch_type>];
///
/// <batch_type> is an optional string identifier that identifies the type of
///              the batch. (e.g. "DML" or "DDL)
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedStartBatchStmtProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedStatementProto>,
    #[prost(string, optional, tag = "2")]
    pub batch_type: ::std::option::Option<std::string::String>,
}
/// This statement: RUN BATCH;
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedRunBatchStmtProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedStatementProto>,
}
/// This statement: ABORT BATCH;
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedAbortBatchStmtProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedStatementProto>,
}
/// This statement: DROP <object_type> [IF EXISTS] <name_path>;
///
/// <object_type> is an string identifier,
///               e.g., "TABLE", "VIEW", "INDEX", "FUNCTION", "TYPE", etc.
/// <name_path> is a vector giving the identifier path for the object to be
///             dropped.
/// <is_if_exists> silently ignore the "name_path does not exist" error.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedDropStmtProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedStatementProto>,
    #[prost(string, optional, tag = "2")]
    pub object_type: ::std::option::Option<std::string::String>,
    #[prost(bool, optional, tag = "3")]
    pub is_if_exists: ::std::option::Option<bool>,
    #[prost(string, repeated, tag = "4")]
    pub name_path: ::std::vec::Vec<std::string::String>,
}
/// This statement: DROP MATERIALIZED VIEW [IF EXISTS] <name_path>;
///
/// <name_path> is a vector giving the identifier path for the object to be
///             dropped.
/// <is_if_exists> silently ignore the "name_path does not exist" error.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedDropMaterializedViewStmtProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedStatementProto>,
    #[prost(bool, optional, tag = "3")]
    pub is_if_exists: ::std::option::Option<bool>,
    #[prost(string, repeated, tag = "4")]
    pub name_path: ::std::vec::Vec<std::string::String>,
}
/// This represents a SQL WITH query (or subquery) like
///   WITH <with_query_name1> AS (<with_subquery1>),
///        <with_query_name2> AS (<with_subquery2>)
///   <query>;
///
/// A <with_query_name> may be referenced (multiple times) inside a later
/// with_subquery, or in the final <query>.
///
/// If a WITH subquery is referenced multiple times, the full query should
/// behave as if the subquery runs only once and its result is reused.
///
/// There will be one ResolvedWithEntry here for each subquery in the SQL
/// WITH statement, in the same order as in the query.
///
/// Inside the resolved <query>, or any <with_entry_list> occurring after
/// its definition, a <with_query_name> used as a table scan will be
/// represented using a ResolvedWithRefScan.
///
/// The <with_query_name> aliases are always unique within a query, and should
/// be used to connect the ResolvedWithRefScan to the original query
/// definition.  The subqueries are not inlined and duplicated into the tree.
///
/// In ZetaSQL 1.0, WITH is allowed only on the outermost query and not in
/// subqueries, so the ResolvedWithScan node can only occur as the outermost
/// scan in a statement (e.g. a QueryStmt or CreateTableAsSelectStmt).
///
/// In ZetaSQL 1.1 (language option FEATURE_V_1_1_WITH_ON_SUBQUERY), WITH
/// is allowed on subqueries.  Then, ResolvedWithScan can occur anywhere in
/// the tree.  The alias introduced by a ResolvedWithEntry is visible only
/// in subsequent ResolvedWithEntry queries and in <query>.  The aliases used
/// must be globally unique in the resolved AST however, so consumers do not
/// need to implement any scoping for these names.  Because the aliases are
/// unique, it is legal to collect all ResolvedWithEntries in the tree and
/// treat them as if they were a single WITH clause at the outermost level.
///
/// The subqueries inside ResolvedWithEntries cannot be correlated.
///
/// If a WITH subquery is defined but never referenced, it will still be
/// resolved and still show up here.  Query engines may choose not to run it.
///
/// SQL-style WITH RECURSIVE is not currently supported.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedWithScanProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedScanProto>,
    #[prost(message, repeated, tag = "2")]
    pub with_entry_list: ::std::vec::Vec<ResolvedWithEntryProto>,
    #[prost(message, optional, boxed, tag = "3")]
    pub query: ::std::option::Option<::std::boxed::Box<AnyResolvedScanProto>>,
}
/// This represents one aliased subquery introduced in a WITH clause.
///
/// The <with_query_name>s must be globally unique in the full resolved AST.
/// The <with_subquery> cannot be correlated and cannot reference any
/// columns from outside.  It may reference other WITH subqueries.
///
/// See ResolvedWithScan for full details.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedWithEntryProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedArgumentProto>,
    #[prost(string, optional, tag = "2")]
    pub with_query_name: ::std::option::Option<std::string::String>,
    #[prost(message, optional, tag = "3")]
    pub with_subquery: ::std::option::Option<AnyResolvedScanProto>,
}
/// This represents one SQL hint key/value pair.
/// The SQL syntax @{ key1=value1, key2=value2, some_db.key3=value3 }
/// will expand to three ResolvedOptions.  Keyword hints (e.g. LOOKUP JOIN)
/// are interpreted as shorthand, and will be expanded to a ResolvedOption
/// attached to the appropriate node before any explicit long-form hints.
///
/// ResolvedOptions are attached to the ResolvedScan corresponding to the
/// operator that the SQL hint was associated with.
/// See (broken link) for more detail.
/// Hint semantics are implementation defined.
///
/// Each hint is resolved as a [<qualifier>.]<name>:=<value> pair.
///   <qualifier> will be empty if no qualifier was present.
///   <name> is always non-empty.
///   <value> can be a ResolvedLiteral or a ResolvedParameter,
///           a cast of a ResolvedParameter (for typed hints only),
///           or a general expression (on constant inputs).
///
/// If AllowedHintsAndOptions was set in AnalyzerOptions, and this hint or
/// option was included there and had an expected type, the type of <value>
/// will match that expected type.  Unknown hints (not listed in
/// AllowedHintsAndOptions) are not stripped and will still show up here.
///
/// If non-empty, <qualifier> should be interpreted as a target system name,
/// and a database system should ignore any hints targeted to different
/// systems.
///
/// The SQL syntax allows using an identifier as a hint value.
/// Such values are stored here as ResolvedLiterals with string type.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedOptionProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedArgumentProto>,
    #[prost(string, optional, tag = "2")]
    pub qualifier: ::std::option::Option<std::string::String>,
    #[prost(string, optional, tag = "3")]
    pub name: ::std::option::Option<std::string::String>,
    #[prost(message, optional, tag = "4")]
    pub value: ::std::option::Option<AnyResolvedExprProto>,
}
/// Window partitioning specification for an analytic function call.
///
/// PARTITION BY keys in <partition_by_list>.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedWindowPartitioningProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedArgumentProto>,
    #[prost(message, repeated, tag = "2")]
    pub partition_by_list: ::std::vec::Vec<ResolvedColumnRefProto>,
    #[prost(message, repeated, tag = "3")]
    pub hint_list: ::std::vec::Vec<ResolvedOptionProto>,
}
/// Window ordering specification for an analytic function call.
///
/// ORDER BY items in <order_by_list>. There should be exactly one ORDER
/// BY item if this is a window ORDER BY for a RANGE-based window.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedWindowOrderingProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedArgumentProto>,
    #[prost(message, repeated, tag = "2")]
    pub order_by_item_list: ::std::vec::Vec<ResolvedOrderByItemProto>,
    #[prost(message, repeated, tag = "3")]
    pub hint_list: ::std::vec::Vec<ResolvedOptionProto>,
}
/// Window framing specification for an analytic function call.
///
/// ROW-based window frames compute the frame based on physical offsets
/// from the current row.
/// RANGE-based window frames compute the frame based on a logical
/// range of rows around the current row based on the current row's
/// ORDER BY key value.
///
/// <start_expr> and <end_expr> cannot be NULL. If the window frame
/// is one-sided in the input query, the resolver will generate an
/// implicit ending boundary.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedWindowFrameProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedArgumentProto>,
    #[prost(
        enumeration = "resolved_window_frame_enums::FrameUnit",
        optional,
        tag = "2"
    )]
    pub frame_unit: ::std::option::Option<i32>,
    #[prost(message, optional, boxed, tag = "3")]
    pub start_expr: ::std::option::Option<::std::boxed::Box<ResolvedWindowFrameExprProto>>,
    #[prost(message, optional, boxed, tag = "4")]
    pub end_expr: ::std::option::Option<::std::boxed::Box<ResolvedWindowFrameExprProto>>,
}
/// This represents a group of analytic function calls that shares PARTITION
/// BY and ORDER BY.
///
/// <partition_by> can be NULL. <order_by> may be NULL depending on the
/// functions in <analytic_function_list> and the window frame unit. See
/// (broken link) for more details.
///
/// All expressions in <analytic_function_list> have a
/// ResolvedAggregateFunctionCall with a function in mode
/// Function::AGGREGATE or Function::ANALYTIC.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedAnalyticFunctionGroupProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedArgumentProto>,
    #[prost(message, optional, tag = "2")]
    pub partition_by: ::std::option::Option<ResolvedWindowPartitioningProto>,
    #[prost(message, optional, tag = "3")]
    pub order_by: ::std::option::Option<ResolvedWindowOrderingProto>,
    #[prost(message, repeated, tag = "4")]
    pub analytic_function_list: ::std::vec::Vec<ResolvedComputedColumnProto>,
}
/// Window frame boundary expression that determines the first/last row of
/// the moving window for each tuple.
///
/// <expression> cannot be NULL if the type is OFFSET_PRECEDING
/// or OFFSET_FOLLOWING. It must be a constant expression. If this is a
/// boundary for a ROW-based window, it must be integer type. Otherwise,
/// it must be numeric type and must match exactly the type of the window
/// ordering expression.  See (broken link) for more
/// details.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedWindowFrameExprProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedArgumentProto>,
    #[prost(
        enumeration = "resolved_window_frame_expr_enums::BoundaryType",
        optional,
        tag = "2"
    )]
    pub boundary_type: ::std::option::Option<i32>,
    #[prost(message, optional, boxed, tag = "3")]
    pub expression: ::std::option::Option<::std::boxed::Box<AnyResolvedExprProto>>,
}
/// This represents a value inside an INSERT or UPDATE statement.
///
/// The <value> is either an expression or a DMLDefault.
///
/// For proto fields, NULL values mean the field should be cleared.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedDmlValueProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedArgumentProto>,
    #[prost(message, optional, tag = "2")]
    pub value: ::std::option::Option<AnyResolvedExprProto>,
}
/// This is used to represent the value DEFAULT that shows up (in place of a
/// value expression) in INSERT and UPDATE statements.
/// For columns, engines should substitute the engine-defined default value
/// for that column, or give an error.
/// For proto fields, this always means to clear the field.
/// This will never show up inside expressions other than ResolvedDMLValue.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedDmlDefaultProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedExprProto>,
}
/// This represents the ASSERT statement:
///   ASSERT <expression> [AS <description>];
///
/// <expression> is any expression that returns a bool.
/// <description> is an optional string literal used to give a more
/// descriptive error message in case the ASSERT fails.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedAssertStmtProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedStatementProto>,
    #[prost(message, optional, tag = "2")]
    pub expression: ::std::option::Option<AnyResolvedExprProto>,
    #[prost(string, optional, tag = "3")]
    pub description: ::std::option::Option<std::string::String>,
}
/// This represents the ASSERT ROWS MODIFIED clause on a DML statement.
/// The value must be a literal or (possibly casted) parameter int64.
///
/// The statement should fail if the number of rows updated does not
/// exactly match this number.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedAssertRowsModifiedProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedArgumentProto>,
    #[prost(message, optional, tag = "2")]
    pub rows: ::std::option::Option<AnyResolvedExprProto>,
}
/// This represents one row in the VALUES clause of an INSERT.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedInsertRowProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedArgumentProto>,
    #[prost(message, repeated, tag = "2")]
    pub value_list: ::std::vec::Vec<ResolvedDmlValueProto>,
}
/// This represents an INSERT statement, or a nested INSERT inside an
/// UPDATE statement.
///
/// For top-level INSERT statements, <table_scan> gives the table to
/// scan and creates ResolvedColumns for its columns.  Those columns can be
/// referenced in <insert_column_list>.
///
/// For nested INSERTS, there is no <table_scan> or <insert_column_list>.
/// There is implicitly a single column to insert, and its type is the
/// element type of the array being updated in the ResolvedUpdateItem
/// containing this statement.
///
/// For nested INSERTs, alternate modes are not supported and <insert_mode>
/// will always be set to OR_ERROR.
///
/// The rows to insert come from <row_list> or the result of <query>.
/// Exactly one of these must be present.
///
/// If <row_list> is present, the columns in the row_list match
/// positionally with <insert_column_list>.
///
/// If <query> is present, <query_output_column_list> must also be present.
/// <query_output_column_list> is the list of output columns produced by
/// <query> that correspond positionally with the target <insert_column_list>
/// on the output table.  For nested INSERTs with no <insert_column_list>,
/// <query_output_column_list> must have exactly one column.
///
/// <query_parameter_list> is set for nested INSERTs where <query> is set and
/// references non-target values (columns or field values) from the table. It
/// is only set when FEATURE_V_1_2_CORRELATED_REFS_IN_NESTED_DML is enabled.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedInsertStmtProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedStatementProto>,
    #[prost(message, optional, tag = "2")]
    pub table_scan: ::std::option::Option<ResolvedTableScanProto>,
    /// Behavior on duplicate rows (normally defined to mean duplicate
    /// primary keys).
    #[prost(
        enumeration = "resolved_insert_stmt_enums::InsertMode",
        optional,
        tag = "3"
    )]
    pub insert_mode: ::std::option::Option<i32>,
    #[prost(message, optional, tag = "4")]
    pub assert_rows_modified: ::std::option::Option<ResolvedAssertRowsModifiedProto>,
    #[prost(message, repeated, tag = "5")]
    pub insert_column_list: ::std::vec::Vec<ResolvedColumnProto>,
    #[prost(message, repeated, tag = "9")]
    pub query_parameter_list: ::std::vec::Vec<ResolvedColumnRefProto>,
    #[prost(message, optional, tag = "6")]
    pub query: ::std::option::Option<AnyResolvedScanProto>,
    #[prost(message, repeated, tag = "8")]
    pub query_output_column_list: ::std::vec::Vec<ResolvedColumnProto>,
    #[prost(message, repeated, tag = "7")]
    pub row_list: ::std::vec::Vec<ResolvedInsertRowProto>,
}
/// This represents a DELETE statement or a nested DELETE inside an
/// UPDATE statement.
///
/// For top-level DELETE statements, <table_scan> gives the table to
/// scan and creates ResolvedColumns for its columns.  Those columns can
/// be referenced inside the <where_expr>.
///
/// For nested DELETEs, there is no <table_scan>.  The <where_expr> can
/// only reference:
///   (1) the element_column from the ResolvedUpdateItem containing this
///       statement,
///   (2) columns from the outer statements, and
///   (3) (optionally) <array_offset_column>, which represents the 0-based
///       offset of the array element being modified.
///
/// <where_expr> is required.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedDeleteStmtProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedStatementProto>,
    #[prost(message, optional, tag = "2")]
    pub table_scan: ::std::option::Option<ResolvedTableScanProto>,
    #[prost(message, optional, tag = "3")]
    pub assert_rows_modified: ::std::option::Option<ResolvedAssertRowsModifiedProto>,
    #[prost(message, optional, tag = "5")]
    pub array_offset_column: ::std::option::Option<ResolvedColumnHolderProto>,
    #[prost(message, optional, tag = "4")]
    pub where_expr: ::std::option::Option<AnyResolvedExprProto>,
}
/// This represents one item inside the SET clause of an UPDATE.
///
/// The entity being updated is specified by <target>.
///
/// For a regular
///   SET {target} = {expression} | DEFAULT
/// clause (not including an array element update like SET a[OFFSET(0)] = 5),
/// <target> and <set_value> will be present, and all other fields will be
/// unset.
///
/// For an array element update (e.g. SET a.b[<expr>].c = <value>),
///   - <target> is set to the array,
///   - <element_column> is a new ResolvedColumn that can be used inside the
///     update items to refer to the array element.
///   - <array_update_list> will have a node corresponding to the offset into
///     that array and the modification to that array element.
/// For example, for SET a.b[<expr>].c = <value>, we have
///    ResolvedUpdateItem
///    +-<target> = a.b
///    +-<element_column> = <x>
///    +-<array_update_list>
///      +-ResolvedUpdateArrayItem
///        +-<offset> = <expr>
///        +-<update_item> = ResolvedUpdateItem
///          +-<target> = <x>.c
///          +-<set_value> = <value>
///
/// The engine is required to fail the update if there are two elements of
/// <array_update_list> corresponding to offset expressions that evaluate to
/// the same value. These are considered to be conflicting updates.
///
/// Multiple updates to the same array are always represented as multiple
/// elements of <array_update_list> under a single ResolvedUpdateItem
/// corresponding to that array. <array_update_list> will only have one
/// element for modifications to an array-valued subfield of an array element.
/// E.g., for SET a[<expr1>].b[<expr2>] = 5, a[<expr3>].b[<expr4>] = 6, we
/// will have:
///     ResolvedUpdateItem
///     +-<target> = a
///     +-<element_column> = x
///     +-<array_update_list>
///       +-ResolvedUpdateArrayItem
///         +-<offset> = <expr1>
///         +-ResolvedUpdateItem for <x>.b[<expr2>] = 5
///       +-ResolvedUpdateArrayItem
///         +-<offset> = <expr3>
///         +-ResolvedUpdateItem for <x>.b[<expr4>] = 6
/// The engine must give a runtime error if <expr1> and <expr3> evaluate to
/// the same thing. Notably, it does not have to understand that the
/// two ResolvedUpdateItems corresponding to "b" refer to the same array iff
/// <expr1> and <expr3> evaluate to the same thing.
///
/// TODO: Consider allowing the engine to execute an update like
/// SET a[<expr1>].b = 1, a[<expr2>].c = 2 even if <expr1> == <expr2> since
/// "b" and "c" do not overlap. Also consider allowing a more complex example
/// like SET a[<expr1>].b[<expr2>] = ...,
/// a[<expr3>].b[<expr4>].c[<expr5>] = ... even if <expr1> == <expr3>, as long
/// as <expr2> != <expr4> in that case.
///
/// For nested DML, <target> and <element_column> will both be set, and one or
/// more of the nested statement lists will be non-empty. <target> must have
/// ARRAY type, and <element_column> introduces a ResolvedColumn representing
/// elements of that array. The nested statement lists will always be empty in
/// a ResolvedUpdateItem child of a ResolvedUpdateArrayItem node.
///
/// See (broken link) for more detail.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedUpdateItemProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedArgumentProto>,
    /// The target entity to be updated.
    ///
    /// This is an expression evaluated using the ResolvedColumns visible
    /// inside this statement.  This expression can contain only
    /// ResolvedColumnRefs, ResolvedGetProtoField and
    /// ResolvedGetStructField nodes.
    ///
    /// In a top-level UPDATE, the expression always starts with a
    /// ResolvedColumnRef referencing a column from the statement's
    /// TableScan.
    ///
    /// In a nested UPDATE, the expression always starts with a
    /// ResolvedColumnRef referencing the element_column from the
    /// ResolvedUpdateItem containing this scan.
    ///
    /// This node is also used to represent a modification of a single
    /// array element (when it occurs as a child of a
    /// ResolvedUpdateArrayItem node).  In that case, the expression
    /// starts with a ResolvedColumnRef referencing the <element_column>
    /// from its grandparent ResolvedUpdateItem. (E.g., for "SET a[<expr>]
    /// = 5", the grandparent ResolvedUpdateItem has <target> "a", the
    /// parent ResolvedUpdateArrayItem has offset <expr>, and this node
    /// has <set_value> 5 and target corresponding to the grandparent's
    /// <element_column> field.)
    ///
    /// For either a nested UPDATE or an array modification, there may be
    /// a path of field accesses after the initial ResolvedColumnRef,
    /// represented by a chain of GetField nodes.
    ///
    /// NOTE: We use the same GetField nodes as we do for queries, but
    /// they are not treated the same.  Here, they express a path inside
    /// an object that is being mutated, so they have reference semantics.
    #[prost(message, optional, tag = "2")]
    pub target: ::std::option::Option<AnyResolvedExprProto>,
    /// Set the target entity to this value.  The types must match.
    /// This can contain the same columns that can appear in the
    /// <where_expr> of the enclosing ResolvedUpdateStmt.
    ///
    /// This is mutually exclusive with all fields below, which are used
    /// for nested updates only.
    #[prost(message, optional, tag = "3")]
    pub set_value: ::std::option::Option<ResolvedDmlValueProto>,
    /// The ResolvedColumn introduced to represent the elements of the
    /// array being updated.  This works similarly to
    /// ArrayScan::element_column.
    ///
    /// <target> must have array type, and this column has the array's
    /// element type.
    ///
    /// This column can be referenced inside the nested statements below.
    #[prost(message, optional, tag = "4")]
    pub element_column: ::std::option::Option<ResolvedColumnHolderProto>,
    /// Array element modifications to apply. Each item runs on the value
    /// of <element_column> specified by ResolvedUpdateArrayItem.offset.
    /// This field is always empty if the analyzer option
    /// FEATURE_V_1_2_ARRAY_ELEMENTS_WITH_SET is disabled.
    ///
    /// The engine must fail if two elements in this list have offset
    /// expressions that evaluate to the same value.
    /// TODO: Consider generalizing this to allow
    /// SET a[<expr1>].b = ..., a[<expr2>].c = ...
    #[prost(message, repeated, tag = "8")]
    pub array_update_list: ::std::vec::Vec<ResolvedUpdateArrayItemProto>,
    /// Nested DELETE statements to apply.  Each delete runs on one value
    /// of <element_column> and may choose to delete that array element.
    ///
    /// DELETEs are applied before INSERTs or UPDATEs.
    ///
    /// It is legal for the same input element to match multiple DELETEs.
    #[prost(message, repeated, tag = "5")]
    pub delete_list: ::std::vec::Vec<ResolvedDeleteStmtProto>,
    /// Nested UPDATE statements to apply.  Each update runs on one value
    /// of <element_column> and may choose to update that array element.
    ///
    /// UPDATEs are applied after DELETEs and before INSERTs.
    ///
    /// It is an error if any element is matched by multiple UPDATEs.
    #[prost(message, repeated, tag = "6")]
    pub update_list: ::std::vec::Vec<ResolvedUpdateStmtProto>,
    /// Nested INSERT statements to apply.  Each insert will produce zero
    /// or more values for <element_column>.
    ///
    /// INSERTs are applied after DELETEs and UPDATEs.
    ///
    /// For nested UPDATEs, insert_mode will always be the default, and
    /// has no effect.
    #[prost(message, repeated, tag = "7")]
    pub insert_list: ::std::vec::Vec<ResolvedInsertStmtProto>,
}
/// For an array element modification, this node represents the offset
/// expression and the modification, but not the array. E.g., for
/// SET a[<expr>] = 5, this node represents a modification of "= 5" to offset
/// <expr> of the array defined by the parent node.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedUpdateArrayItemProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedArgumentProto>,
    /// The array offset to be modified.
    #[prost(message, optional, tag = "2")]
    pub offset: ::std::option::Option<AnyResolvedExprProto>,
    /// The modification to perform to the array element.
    #[prost(message, optional, tag = "3")]
    pub update_item: ::std::option::Option<ResolvedUpdateItemProto>,
}
/// This represents an UPDATE statement, or a nested UPDATE inside an
/// UPDATE statement.
///
/// For top-level UPDATE statements, <table_scan> gives the table to
/// scan and creates ResolvedColumns for its columns.  Those columns can be
/// referenced in the <update_item_list>. The top-level UPDATE statement may
/// also have <from_scan>, the output of which is joined with
/// the <table_scan> using expressions in the <where_expr>. The columns
/// exposed in the <from_scan> are visible in the right side of the
/// expressions in the <update_item_list> and in the <where_expr>.
/// <array_offset_column> is never set for top-level UPDATE statements.
///
/// Top-level UPDATE statements will also have <column_access_list> populated.
/// For each column, this vector indicates if the column was read and/or
/// written. The columns in this vector match those of
/// <table_scan.column_list>. If a column was not encountered when producing
/// the resolved AST, then the value at that index will be
/// ResolvedStatement::NONE.
///
/// For nested UPDATEs, there is no <table_scan>.  The <where_expr> can
/// only reference:
///   (1) the element_column from the ResolvedUpdateItem containing this
///       statement,
///   (2) columns from the outer statements, and
///   (3) (optionally) <array_offset_column>, which represents the 0-based
///       offset of the array element being modified.
/// The left hand sides of the expressions in <update_item_list> can only
/// reference (1). The right hand sides of those expressions can reference
/// (1), (2), and (3).
///
/// The updates in <update_item_list> will be non-overlapping.
/// If there are multiple nested statements updating the same entity,
/// they will be combined into one ResolvedUpdateItem.
///
/// See (broken link) for more detail on nested DML.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedUpdateStmtProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedStatementProto>,
    #[prost(message, optional, tag = "2")]
    pub table_scan: ::std::option::Option<ResolvedTableScanProto>,
    #[prost(
        enumeration = "resolved_statement_enums::ObjectAccess",
        repeated,
        packed = "false",
        tag = "8"
    )]
    pub column_access_list: ::std::vec::Vec<i32>,
    #[prost(message, optional, tag = "3")]
    pub assert_rows_modified: ::std::option::Option<ResolvedAssertRowsModifiedProto>,
    #[prost(message, optional, tag = "7")]
    pub array_offset_column: ::std::option::Option<ResolvedColumnHolderProto>,
    #[prost(message, optional, tag = "4")]
    pub where_expr: ::std::option::Option<AnyResolvedExprProto>,
    #[prost(message, repeated, tag = "5")]
    pub update_item_list: ::std::vec::Vec<ResolvedUpdateItemProto>,
    #[prost(message, optional, tag = "6")]
    pub from_scan: ::std::option::Option<AnyResolvedScanProto>,
}
/// This is used by ResolvedMergeStmt to represent one WHEN ... THEN clause
/// within MERGE statement.
///
/// There are three types of clauses, which are MATCHED, NOT_MATCHED_BY_SOURCE
/// and NOT_MATCHED_BY_TARGET. The <match_type> must have one of these values.
///
/// The <match_expr> defines an optional expression to apply to the join
/// result of <table_scan> and <from_scan> of the parent ResolvedMergeStmt.
///
/// Each ResolvedMergeWhen must define exactly one of three operations,
///   -- INSERT: <action_type> is ResolvedMergeWhen::INSERT.
///              Both <insert_column_list> and <insert_row> are non-empty.
///              The size of <insert_column_list> must be the same with the
///              value_list size of <insert_row>, and, the column data type
///              must match.
///   -- UPDATE: <action_type> is ResolvedMergeWhen::UPDATE.
///              <update_item_list> is non-empty.
///   -- DELETE: <action_type> is ResolvedMergeWhen::DELETE.
/// The INSERT, UPDATE and DELETE operations are mutually exclusive.
///
/// When <match_type> is MATCHED, <action_type> must be UPDATE or DELETE.
/// When <match_type> is NOT_MATCHED_BY_TARGET, <action_type> must be INSERT.
/// When <match_type> is NOT_MATCHED_BY_SOURCE, <action_type> must be UPDATE
/// or DELETE.
///
/// The column visibility within a ResolvedMergeWhen clause is defined as
/// following,
///   -- When <match_type> is MATCHED,
///      -- All columns from <table_scan> and <from_scan> are allowed in
///         <match_expr>.
///      -- If <action_type> is UPDATE, only columns from <table_scan> are
///         allowed on left side of expressions in <update_item_list>.
///         All columns from <table_scan> and <from_scan> are allowed on right
///         side of expressions in <update_item_list>.
///   -- When <match_type> is NOT_MATCHED_BY_TARGET,
///      -- Only columns from <from_scan> are allowed in <match_expr>.
///      -- Only columns from <table_scan> are allowed in
///         <insert_column_list>.
///      -- Only columns from <from_scan> are allowed in <insert_row>.
///   -- When <match_type> is NOT_MATCHED_BY_SOURCE,
///      -- Only columns from <table_scan> are allowed in <match_expr>.
///      -- If <action_type> is UPDATE, only columns from <table_scan> are
///         allowed in <update_item_list>.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedMergeWhenProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedArgumentProto>,
    #[prost(
        enumeration = "resolved_merge_when_enums::MatchType",
        optional,
        tag = "2"
    )]
    pub match_type: ::std::option::Option<i32>,
    #[prost(message, optional, tag = "3")]
    pub match_expr: ::std::option::Option<AnyResolvedExprProto>,
    #[prost(
        enumeration = "resolved_merge_when_enums::ActionType",
        optional,
        tag = "4"
    )]
    pub action_type: ::std::option::Option<i32>,
    #[prost(message, repeated, tag = "5")]
    pub insert_column_list: ::std::vec::Vec<ResolvedColumnProto>,
    #[prost(message, optional, tag = "6")]
    pub insert_row: ::std::option::Option<ResolvedInsertRowProto>,
    #[prost(message, repeated, tag = "7")]
    pub update_item_list: ::std::vec::Vec<ResolvedUpdateItemProto>,
}
/// This represents a MERGE statement.
///
/// <table_scan> gives the target table to scan and creates ResolvedColumns
/// for its columns.
///
/// <column_access_list> indicates for each column, whether it was read and/or
/// written. The columns in this vector match those of
/// <table_scan.column_list>. If a column was not encountered when producing
/// the resolved AST, then the value at that index will be
/// ResolvedStatement::NONE(0).
///
/// The output of <from_scan> is joined with <table_scan> using the join
/// expression <merge_expr>.
///
/// The order of elements in <when_clause_list> matters, as they are executed
/// sequentially. At most one of the <when_clause_list> clause will be applied
/// to each row from <table_scan>.
///
/// <table_scan>, <from_scan>, <merge_expr> and <when_clause_list> are
/// required. <when_clause_list> must be non-empty.
///
/// See (broken link) for more detail on MERGE statement.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedMergeStmtProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedStatementProto>,
    #[prost(message, optional, tag = "2")]
    pub table_scan: ::std::option::Option<ResolvedTableScanProto>,
    #[prost(
        enumeration = "resolved_statement_enums::ObjectAccess",
        repeated,
        packed = "false",
        tag = "6"
    )]
    pub column_access_list: ::std::vec::Vec<i32>,
    #[prost(message, optional, tag = "3")]
    pub from_scan: ::std::option::Option<AnyResolvedScanProto>,
    #[prost(message, optional, tag = "4")]
    pub merge_expr: ::std::option::Option<AnyResolvedExprProto>,
    #[prost(message, repeated, tag = "5")]
    pub when_clause_list: ::std::vec::Vec<ResolvedMergeWhenProto>,
}
/// This represents a TRUNCATE TABLE statement.
///
/// Statement:
///   TRUNCATE TABLE <table_name> [WHERE <boolean_expression>]
///
/// <table_scan> is a TableScan for the target table, which is used during
///              resolving and validation. Consumers can use either the table
///              object inside it or name_path to reference the table.
/// <where_expr> boolean expression that can reference columns in
///              ResolvedColumns (which the TableScan creates); the
///              <where_expr> should always correspond to entire partitions,
///              and is optional.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedTruncateStmtProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedStatementProto>,
    #[prost(message, optional, tag = "3")]
    pub table_scan: ::std::option::Option<ResolvedTableScanProto>,
    #[prost(message, optional, tag = "4")]
    pub where_expr: ::std::option::Option<AnyResolvedExprProto>,
}
/// A grantable privilege.
///
/// <action_type> is the type of privilege action, e.g. SELECT, INSERT, UPDATE
/// or DELETE.
/// <unit_list> is an optional list of units of the object (e.g. columns of a
/// table) the privilege is restricted to. Privilege on the whole object
/// should be granted/revoked if the list is empty.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedPrivilegeProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedArgumentProto>,
    #[prost(string, optional, tag = "2")]
    pub action_type: ::std::option::Option<std::string::String>,
    #[prost(string, repeated, tag = "3")]
    pub unit_list: ::std::vec::Vec<std::string::String>,
}
/// Common superclass of GRANT/REVOKE statements.
///
/// <privilege_list> is the list of privileges to be granted/revoked. ALL
/// PRIVILEGES should be granted/fromed if it is empty.
/// <object_type> is an optional string identifier, e.g., TABLE, VIEW.
/// <name_path> is a vector of segments of the object identifier's pathname.
/// <grantee_list> (DEPRECATED) is the list of grantees (strings).
/// <grantee_expr_list> is the list of grantees, and may include parameters.
///
/// Only one of <grantee_list> or <grantee_expr_list> will be populated,
/// depending on whether or not the FEATURE_PARAMETERS_IN_GRANTEE_LIST
/// is enabled.  The <grantee_list> is deprecated, and will be removed
/// along with the corresponding FEATURE once all engines have migrated to
/// use the <grantee_expr_list>.  Once <grantee_expr_list> is the only
/// one, then it should be marked as NOT_IGNORABLE.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AnyResolvedGrantOrRevokeStmtProto {
    #[prost(
        oneof = "any_resolved_grant_or_revoke_stmt_proto::Node",
        tags = "69, 70"
    )]
    pub node: ::std::option::Option<any_resolved_grant_or_revoke_stmt_proto::Node>,
}
pub mod any_resolved_grant_or_revoke_stmt_proto {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Node {
        #[prost(message, tag = "69")]
        ResolvedGrantStmtNode(super::ResolvedGrantStmtProto),
        #[prost(message, tag = "70")]
        ResolvedRevokeStmtNode(super::ResolvedRevokeStmtProto),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedGrantOrRevokeStmtProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedStatementProto>,
    #[prost(message, repeated, tag = "2")]
    pub privilege_list: ::std::vec::Vec<ResolvedPrivilegeProto>,
    #[prost(string, optional, tag = "3")]
    pub object_type: ::std::option::Option<std::string::String>,
    #[prost(string, repeated, tag = "4")]
    pub name_path: ::std::vec::Vec<std::string::String>,
    #[prost(string, repeated, tag = "5")]
    pub grantee_list: ::std::vec::Vec<std::string::String>,
    #[prost(message, repeated, tag = "6")]
    pub grantee_expr_list: ::std::vec::Vec<AnyResolvedExprProto>,
}
/// A GRANT statement. It represents the action to grant a list of privileges
/// on a specific object to/from list of grantees.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedGrantStmtProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedGrantOrRevokeStmtProto>,
}
/// A REVOKE statement. It represents the action to revoke a list of
/// privileges on a specific object to/from list of grantees.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedRevokeStmtProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedGrantOrRevokeStmtProto>,
}
/// Common super class for statements:
///   ALTER <object> [IF EXISTS] <name_path> <alter_action_list>
///
/// <name_path> is a vector giving the identifier path in the table <name>.
/// <alter_action_list> is a vector of actions to be done to the object.
/// <is_if_exists> silently ignores the "name_path does not exist" error.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AnyResolvedAlterObjectStmtProto {
    #[prost(
        oneof = "any_resolved_alter_object_stmt_proto::Node",
        tags = "75, 115, 118, 127, 134"
    )]
    pub node: ::std::option::Option<any_resolved_alter_object_stmt_proto::Node>,
}
pub mod any_resolved_alter_object_stmt_proto {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Node {
        #[prost(message, tag = "75")]
        ResolvedAlterRowAccessPolicyStmtNode(super::ResolvedAlterRowAccessPolicyStmtProto),
        #[prost(message, tag = "115")]
        ResolvedAlterTableStmtNode(super::ResolvedAlterTableStmtProto),
        #[prost(message, tag = "118")]
        ResolvedAlterViewStmtNode(super::ResolvedAlterViewStmtProto),
        #[prost(message, tag = "127")]
        ResolvedAlterMaterializedViewStmtNode(super::ResolvedAlterMaterializedViewStmtProto),
        #[prost(message, tag = "134")]
        ResolvedAlterDatabaseStmtNode(super::ResolvedAlterDatabaseStmtProto),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedAlterObjectStmtProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedStatementProto>,
    #[prost(string, repeated, tag = "2")]
    pub name_path: ::std::vec::Vec<std::string::String>,
    #[prost(message, repeated, tag = "3")]
    pub alter_action_list: ::std::vec::Vec<AnyResolvedAlterActionProto>,
    #[prost(bool, optional, tag = "4")]
    pub is_if_exists: ::std::option::Option<bool>,
}
/// This statement:
///   ALTER DATABASE [IF EXISTS] <name_path> <alter_action_list>
///
/// This statement could be used to change the database level options.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedAlterDatabaseStmtProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedAlterObjectStmtProto>,
}
/// This statement:
/// ALTER MATERIALIZED VIEW [IF EXISTS] <name_path> <alter_action_list>
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedAlterMaterializedViewStmtProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedAlterObjectStmtProto>,
}
/// This statement:
/// ALTER TABLE [IF EXISTS] <name_path> <alter_action_list>
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedAlterTableStmtProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedAlterObjectStmtProto>,
}
/// This statement:
/// ALTER VIEW [IF EXISTS] <name_path> <alter_action_list>
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedAlterViewStmtProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedAlterObjectStmtProto>,
}
/// A common super class for all actions in statement ALTER <object>
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AnyResolvedAlterActionProto {
    #[prost(
        oneof = "any_resolved_alter_action_proto::Node",
        tags = "117, 131, 132, 135, 136, 137, 138"
    )]
    pub node: ::std::option::Option<any_resolved_alter_action_proto::Node>,
}
pub mod any_resolved_alter_action_proto {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Node {
        #[prost(message, tag = "117")]
        ResolvedSetOptionsActionNode(super::ResolvedSetOptionsActionProto),
        #[prost(message, tag = "131")]
        ResolvedAddColumnActionNode(super::ResolvedAddColumnActionProto),
        #[prost(message, tag = "132")]
        ResolvedDropColumnActionNode(super::ResolvedDropColumnActionProto),
        #[prost(message, tag = "135")]
        ResolvedGrantToActionNode(super::ResolvedGrantToActionProto),
        #[prost(message, tag = "136")]
        ResolvedFilterUsingActionNode(super::ResolvedFilterUsingActionProto),
        #[prost(message, tag = "137")]
        ResolvedRevokeFromActionNode(super::ResolvedRevokeFromActionProto),
        #[prost(message, tag = "138")]
        ResolvedRenameToActionNode(super::ResolvedRenameToActionProto),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedAlterActionProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedArgumentProto>,
}
/// SET OPTIONS action for ALTER <object> statement
///
/// <option_list> has engine-specific directives that specify how to
///               alter the metadata for this object.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedSetOptionsActionProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedAlterActionProto>,
    #[prost(message, repeated, tag = "2")]
    pub option_list: ::std::vec::Vec<ResolvedOptionProto>,
}
/// ADD COLUMN action for ALTER TABLE statement
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedAddColumnActionProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedAlterActionProto>,
    #[prost(bool, optional, tag = "2")]
    pub is_if_not_exists: ::std::option::Option<bool>,
    #[prost(message, optional, tag = "3")]
    pub column_definition: ::std::option::Option<ResolvedColumnDefinitionProto>,
}
/// DROP COLUMN action for ALTER TABLE statement
///
/// <name> is the name of the column to drop.
/// <column_reference> references the column to be dropped, if it exists.
///        It might be missing if DROP IF EXISTS column does not exist.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedDropColumnActionProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedAlterActionProto>,
    #[prost(bool, optional, tag = "2")]
    pub is_if_exists: ::std::option::Option<bool>,
    #[prost(string, optional, tag = "3")]
    pub name: ::std::option::Option<std::string::String>,
    #[prost(message, optional, tag = "4")]
    pub column_reference: ::std::option::Option<ResolvedColumnRefProto>,
}
/// This statement:
///   ALTER TABLE [IF EXISTS] <name> SET OPTIONS (...)
///
/// NOTE: This is deprecated in favor of ResolvedAlterTableStmt.
///
/// <name_path> is a vector giving the identifier path in the table <name>.
/// <option_list> has engine-specific directives that specify how to
///               alter the metadata for this table.
/// <is_if_exists> silently ignore the "name_path does not exist" error.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedAlterTableSetOptionsStmtProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedStatementProto>,
    #[prost(string, repeated, tag = "2")]
    pub name_path: ::std::vec::Vec<std::string::String>,
    #[prost(message, repeated, tag = "3")]
    pub option_list: ::std::vec::Vec<ResolvedOptionProto>,
    #[prost(bool, optional, tag = "4")]
    pub is_if_exists: ::std::option::Option<bool>,
}
/// This statement: RENAME <object_type> <old_name_path> TO <new_name_path>;
///
/// <object_type> is an string identifier,
///               e.g., "TABLE", "VIEW", "INDEX", "FUNCTION", "TYPE", etc.
/// <old_name_path> is a vector giving the identifier path for the object to
///                 be renamed.
/// <new_name_path> is a vector giving the identifier path for the object to
///                 be renamed to.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedRenameStmtProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedStatementProto>,
    #[prost(string, optional, tag = "2")]
    pub object_type: ::std::option::Option<std::string::String>,
    #[prost(string, repeated, tag = "3")]
    pub old_name_path: ::std::vec::Vec<std::string::String>,
    #[prost(string, repeated, tag = "4")]
    pub new_name_path: ::std::vec::Vec<std::string::String>,
}
/// This statement: CREATE [OR REPLACE] ROW ACCESS POLICY [IF NOT EXISTS]
///                 [<name>] ON <target_name_path>
///                 [GRANT TO (<grantee_list>)]
///                 FILTER USING (<predicate>);
///
/// <create_mode> indicates if this was CREATE, CREATE OR REPLACE, or
///               CREATE IF NOT EXISTS.
/// <name> is the name of the row access policy to be created or an empty
///        string.
/// <target_name_path> is a vector giving the identifier path of the target
///                    table.
/// <table_scan> is a TableScan for the target table, which is used during
///              resolving and validation. Consumers can use either the table
///              object inside it or target_name_path to reference the table.
/// <grantee_list> (DEPRECATED) is the list of user principals the policy
///                should apply to.
/// <grantee_expr_list> is the list of user principals the policy should
///                     apply to, and may include parameters.
/// <predicate> is a boolean expression that selects the rows that are being
///             made visible.
/// <predicate_str> is the string form of the predicate.
///
/// Only one of <grantee_list> or <grantee_expr_list> will be populated,
/// depending on whether or not the FEATURE_PARAMETERS_IN_GRANTEE_LIST
/// is enabled.  The <grantee_list> is deprecated, and will be removed
/// along with the corresponding FEATURE once all engines have migrated to
/// use the <grantee_expr_list>.  Once <grantee_expr_list> is the only
/// one, then it should be marked as NOT_IGNORABLE.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedCreateRowAccessPolicyStmtProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedStatementProto>,
    #[prost(
        enumeration = "resolved_create_statement_enums::CreateMode",
        optional,
        tag = "2"
    )]
    pub create_mode: ::std::option::Option<i32>,
    #[prost(string, optional, tag = "3")]
    pub name: ::std::option::Option<std::string::String>,
    #[prost(string, repeated, tag = "4")]
    pub target_name_path: ::std::vec::Vec<std::string::String>,
    #[prost(string, repeated, tag = "5")]
    pub grantee_list: ::std::vec::Vec<std::string::String>,
    #[prost(message, repeated, tag = "9")]
    pub grantee_expr_list: ::std::vec::Vec<AnyResolvedExprProto>,
    #[prost(message, optional, tag = "6")]
    pub table_scan: ::std::option::Option<ResolvedTableScanProto>,
    #[prost(message, optional, tag = "7")]
    pub predicate: ::std::option::Option<AnyResolvedExprProto>,
    #[prost(string, optional, tag = "8")]
    pub predicate_str: ::std::option::Option<std::string::String>,
}
/// This statement:
///     DROP ROW ACCESS POLICY <name> ON <target_name_path>; or
///     DROP ALL ROW [ACCESS] POLICIES ON <target_name_path>;
///
/// <is_drop_all> indicates that all policies should be dropped.
/// <is_if_exists> silently ignore the "policy <name> does not exist" error.
///                This is not allowed if is_drop_all is true.
/// <name> is the name of the row policy to be dropped or an empty string.
/// <target_name_path> is a vector giving the identifier path of the target
///                    table.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedDropRowAccessPolicyStmtProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedStatementProto>,
    #[prost(bool, optional, tag = "2")]
    pub is_drop_all: ::std::option::Option<bool>,
    #[prost(bool, optional, tag = "3")]
    pub is_if_exists: ::std::option::Option<bool>,
    #[prost(string, optional, tag = "4")]
    pub name: ::std::option::Option<std::string::String>,
    #[prost(string, repeated, tag = "5")]
    pub target_name_path: ::std::vec::Vec<std::string::String>,
}
/// GRANT TO action for ALTER ROW ACCESS POLICY statement
///
/// <grantee_expr_list> is the list of grantees, and may include parameters.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedGrantToActionProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedAlterActionProto>,
    #[prost(message, repeated, tag = "2")]
    pub grantee_expr_list: ::std::vec::Vec<AnyResolvedExprProto>,
}
/// FILTER USING action for ALTER ROW ACCESS POLICY statement
///
/// <predicate> is a boolean expression that selects the rows that are being
///             made visible.
/// <predicate_str> is the string form of the predicate.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedFilterUsingActionProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedAlterActionProto>,
    #[prost(message, optional, tag = "2")]
    pub predicate: ::std::option::Option<AnyResolvedExprProto>,
    #[prost(string, optional, tag = "3")]
    pub predicate_str: ::std::option::Option<std::string::String>,
}
/// REVOKE FROM action for ALTER ROW ACCESS POLICY statement
///
/// <revokee_expr_list> is the list of revokees, and may include parameters.
/// <is_revoke_from_all> is a boolean indicating whether it was a REVOKE FROM
///                      ALL statement.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedRevokeFromActionProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedAlterActionProto>,
    #[prost(message, repeated, tag = "2")]
    pub revokee_expr_list: ::std::vec::Vec<AnyResolvedExprProto>,
    #[prost(bool, optional, tag = "3")]
    pub is_revoke_from_all: ::std::option::Option<bool>,
}
/// RENAME TO action for ALTER ROW ACCESS POLICY statement
///
/// <new_name> is the new name of the row access policy.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedRenameToActionProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedAlterActionProto>,
    #[prost(string, optional, tag = "2")]
    pub new_name: ::std::option::Option<std::string::String>,
}
/// This statement:
///     ALTER ROW ACCESS POLICY [IF EXISTS]
///     <name> ON <name_path>
///     <alter_action_list>
///
/// <name> is the name of the row access policy to be altered, scoped to the
///        table in the base <name_path>.
/// <table_scan> is a TableScan for the target table, which is used during
///              resolving and validation. Consumers can use either the table
///              object inside it or base <name_path> to reference the table.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedAlterRowAccessPolicyStmtProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedAlterObjectStmtProto>,
    #[prost(string, optional, tag = "2")]
    pub name: ::std::option::Option<std::string::String>,
    #[prost(message, optional, tag = "6")]
    pub table_scan: ::std::option::Option<ResolvedTableScanProto>,
}
/// This statement creates a user-defined named constant:
/// CREATE [OR REPLACE] [TEMP | TEMPORARY | PUBLIC | PRIVATE] CONSTANT
///   [IF NOT EXISTS] <name_path> = <expression>
///
/// <name_path> is the identifier path of the named constants.
/// <expr> is the expression that determines the type and the value of the
///        named constant. Note that <expr> need not be constant. Its value
///        is bound to the named constant which is then treated as
///        immutable. <expr> can be evaluated at the time this statement is
///        processed or later (lazy evaluation during query execution).
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedCreateConstantStmtProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedCreateStatementProto>,
    #[prost(message, optional, tag = "2")]
    pub expr: ::std::option::Option<AnyResolvedExprProto>,
}
/// This statement creates a user-defined function:
///   CREATE [TEMP] FUNCTION [IF NOT EXISTS] <name_path> (<arg_list>)
///     [RETURNS <return_type>] [<determinism_level>] [LANGUAGE <language>]
///     [AS <code> | AS ( <function_expression> )] [OPTIONS (<option_list>)]
///
///   <name_path> is the identifier path of the function.
///   <has_explicit_return_type> is true iff RETURNS clause is present.
///   <return_type> is the return type for the function, which can be any
///          valid ZetaSQL type, including ARRAY or STRUCT. It is inferred
///          from <function_expression> if not explicitly set.
///          TODO: Deprecate and remove this. The return type is
///          already specified by the <signature>.
///   <argument_name_list> The names of the function arguments.
///   <signature> is the FunctionSignature of the created function, with all
///          options.  This can be used to create a Function to load into a
///          Catalog for future queries.
///   <is_aggregate> is true if this is an aggregate function.  All arguments
///          are assumed to be aggregate input arguments that may vary for
///          every row.
///   <language> is the programming language used by the function. This field
///          is set to 'SQL' for SQL functions and otherwise to the language
///          name specified in the LANGUAGE clause.
///   <code> is a string literal that contains the function definition.  Some
///          engines may allow this argument to be omitted for certain types
///          of external functions. This will always be set for SQL functions.
///   <aggregate_expression_list> is a list of SQL aggregate functions to
///          compute prior to computing the final <function_expression>.
///          See below.
///   <function_expression> is the resolved SQL expression invoked for the
///          function. This will be unset for external language functions. For
///          non-template SQL functions, this is a resolved representation of
///          the expression in <code>.
///   <option_list> has engine-specific directives for modifying functions.
///   <sql_security> is the declared security mode for the function. Values
///          include 'INVOKER', 'DEFINER'.
///   <determinism_level> is the declared determinism level of the function.
///          Values are 'DETERMINISTIC', 'NOT DETERMINISTIC', 'IMMUTABLE',
///          'STABLE', 'VOLATILE'.
///
/// Note that <function_expression> and <code> are both marked as IGNORABLE
/// because an engine could look at either one (but might not look at both).
/// An engine must look at one (and cannot ignore both) to be semantically
/// valid, but there is currently no way to enforce that.
///
/// For aggregate functions, <is_aggregate> will be true.
/// Aggregate functions will only occur if LanguageOptions has
/// FEATURE_CREATE_AGGREGATE_FUNCTION enabled.
///
/// Arguments to aggregate functions must have
/// <FunctionSignatureArgumentTypeOptions::is_not_aggregate> true or false.
/// Non-aggregate arguments must be passed constant values only.
///
/// For SQL aggregate functions, there will be both an
/// <aggregate_expression_list>, with aggregate expressions to compute first,
/// and then a final <function_expression> to compute on the results
/// of the aggregates.  Each aggregate expression is a
/// ResolvedAggregateFunctionCall, and may reference any input arguments.
/// Each ResolvedComputedColumn in <aggregate_expression_list> gives the
/// aggregate expression a column id.  The final <function_expression> can
/// reference these created aggregate columns, and any input arguments
/// with <argument_kind>=NOT_AGGREGATE.
///
/// For example, with
///   CREATE TEMP FUNCTION my_avg(x) = (SUM(x) / COUNT(x));
/// we would have an <aggregate_expression_list> with
///   agg1#1 := SUM(ResolvedArgumentRef(x))
///   agg2#2 := COUNT(ResolvedArgumentRef(x))
/// and a <function_expression>
///   ResolvedColumnRef(agg1#1) / ResolvedColumnRef(agg2#2)
///
/// For example, with
///   CREATE FUNCTION scaled_avg(x,y NOT AGGREGATE) = (SUM(x) / COUNT(x) * y);
/// we would have an <aggregate_expression_list> with
///   agg1#1 := SUM(ResolvedArgumentRef(x))
///   agg2#2 := COUNT(ResolvedArgumentRef(x))
/// and a <function_expression>
///   ResolvedColumnRef(agg1#1) / ResolvedColumnRef(agg2#2) * ResolvedArgumentRef(y)
///
/// When resolving a query that calls an aggregate UDF, the query will
/// have a ResolvedAggregateScan that invokes the UDF function.  The engine
/// should remove the UDF aggregate function from the <aggregate_list>, and
/// instead compute the additional aggregates from the
/// UDF's <aggregate_expression_list>, and then add an additional Map
/// to compute the final <function_expression>, which should produce the
/// value for the original ResolvedAggregateScan's computed column for the
/// UDF.  Some rewrites of the ResolvedColumn references inside the UDF will
/// be required.  TODO If using ResolvedColumns makes this renaming
/// too complicated, we could switch to use ResolvedArgumentRefs, or
/// something new.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedCreateFunctionStmtProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedCreateStatementProto>,
    #[prost(bool, optional, tag = "13")]
    pub has_explicit_return_type: ::std::option::Option<bool>,
    #[prost(message, optional, tag = "3")]
    pub return_type: ::std::option::Option<TypeProto>,
    #[prost(string, repeated, tag = "11")]
    pub argument_name_list: ::std::vec::Vec<std::string::String>,
    #[prost(message, optional, tag = "10")]
    pub signature: ::std::option::Option<FunctionSignatureProto>,
    #[prost(bool, optional, tag = "8")]
    pub is_aggregate: ::std::option::Option<bool>,
    #[prost(string, optional, tag = "4")]
    pub language: ::std::option::Option<std::string::String>,
    #[prost(string, optional, tag = "5")]
    pub code: ::std::option::Option<std::string::String>,
    #[prost(message, repeated, tag = "9")]
    pub aggregate_expression_list: ::std::vec::Vec<ResolvedComputedColumnProto>,
    #[prost(message, optional, tag = "6")]
    pub function_expression: ::std::option::Option<AnyResolvedExprProto>,
    #[prost(message, repeated, tag = "7")]
    pub option_list: ::std::vec::Vec<ResolvedOptionProto>,
    #[prost(
        enumeration = "resolved_create_statement_enums::SqlSecurity",
        optional,
        tag = "12"
    )]
    pub sql_security: ::std::option::Option<i32>,
    #[prost(
        enumeration = "resolved_create_statement_enums::DeterminismLevel",
        optional,
        tag = "14"
    )]
    pub determinism_level: ::std::option::Option<i32>,
}
/// This represents an argument definition, e.g. in a function's argument
/// list.
///
/// <name> is the name of the argument; optional for DROP FUNCTION statements.
/// <type> is the type of the argument.
/// <argument_kind> indicates what kind of argument this is, including scalar
///         vs aggregate.  NOT_AGGREGATE means this is a non-aggregate
///         argument in an aggregate function, which can only passed constant
///         values only.
///
/// NOTE: Statements that create functions now include a FunctionSignature
/// directly, and an argument_name_list if applicable.  These completely
/// describe the function signature, so the ResolvedArgumentDef list can
/// be considered unnecessary and deprecated.
/// TODO We could remove this node in the future.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedArgumentDefProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedArgumentProto>,
    #[prost(string, optional, tag = "2")]
    pub name: ::std::option::Option<std::string::String>,
    #[prost(message, optional, tag = "3")]
    pub r#type: ::std::option::Option<TypeProto>,
    #[prost(
        enumeration = "resolved_argument_def_enums::ArgumentKind",
        optional,
        tag = "4"
    )]
    pub argument_kind: ::std::option::Option<i32>,
}
/// This represents an argument reference, e.g. in a function's body.
/// <name> is the name of the argument.
/// <argument_kind> is the ArgumentKind from the ResolvedArgumentDef.
///         For scalar functions, this is always SCALAR.
///         For aggregate functions, it can be AGGREGATE or NOT_AGGREGATE.
///         If NOT_AGGREGATE, then this is a non-aggregate argument
///         to an aggregate function, which has one constant value
///         for the entire function call (over all rows in all groups).
///         (This is copied from the ResolvedArgumentDef for convenience.)
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedArgumentRefProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedExprProto>,
    #[prost(string, optional, tag = "2")]
    pub name: ::std::option::Option<std::string::String>,
    #[prost(
        enumeration = "resolved_argument_def_enums::ArgumentKind",
        optional,
        tag = "3"
    )]
    pub argument_kind: ::std::option::Option<i32>,
}
/// This statement creates a user-defined table-valued function:
///   CREATE [TEMP] TABLE FUNCTION [IF NOT EXISTS]
///     <name_path> (<argument_name_list>)
///     [RETURNS <return_type>]
///     [OPTIONS (<option_list>)]
///     [LANGUAGE <language>]
///     [AS <code> | AS ( <query> )]
///
///   <argument_name_list> contains the names of the function arguments.
///   <signature> is the FunctionSignature of the created function, with all
///          options.  This can be used to create a Function to load into a
///          Catalog for future queries.
///   <option_list> has engine-specific directives for modifying functions.
///   <language> is the programming language used by the function. This field
///          is set to 'SQL' for SQL functions, to the language name specified
///          in the LANGUAGE clause if present, and to 'UNDECLARED' if both
///          the LANGUAGE clause and query are not present.
///   <code> is an optional string literal that contains the function
///          definition.  Some engines may allow this argument to be omitted
///          for certain types of external functions.  This will always be set
///          for SQL functions.
///   <query> is the SQL query invoked for the function.  This will be unset
///          for external language functions. For non-templated SQL functions,
///          this is a resolved representation of the query in <code>.
///   <output_column_list> is the list of resolved output
///          columns returned by the table-valued function.
///   <is_value_table> If true, this function returns a value table.
///          Rather than producing rows with named columns, it produces
///          rows with a single unnamed value type. <output_column_list> will
///          have exactly one anonymous column (with no name).
///          See (broken link).
///   <sql_security> is the declared security mode for the function. Values
///          include 'INVOKER', 'DEFINER'.
///
/// ----------------------
/// Table-Valued Functions
/// ----------------------
///
/// This is a statement to create a new table-valued function. Each
/// table-valued function returns an entire table as output instead of a
/// single scalar value. Table-valued functions can only be created if
/// LanguageOptions has FEATURE_CREATE_TABLE_FUNCTION enabled.
///
/// For SQL table-valued functions that include a defined SQL body, the
/// <query> is non-NULL and contains the resolved SQL body.
/// In this case, <output_column_list> contains a list of the
/// output columns of the SQL body. The <query> uses
/// ResolvedArgumentRefs to refer to scalar arguments and
/// ResolvedRelationArgumentScans to refer to relation arguments.
///
/// The table-valued function may include RETURNS TABLE<...> to explicitly
/// specify a schema for the output table returned by the function. If the
/// function declaration includes a SQL body, then the names and types of the
/// output columns of the corresponding <query> will have been
/// coerced to exactly match 1:1 with the names and types of the columns
/// specified in the RETURNS TABLE<...> section.
///
/// When resolving a query that calls a table-valued function, the query will
/// have a ResolvedTVFScan that invokes the function.
///
/// Value tables: If the function declaration includes a value-table
/// parameter, this is written as an argument of type "TABLE" where the table
/// contains a single anonymous column with a type but no name. In this case,
/// calls to the function may pass a (regular or value) table with a single
/// (named or unnamed) column for any of these parameters, and ZetaSQL
/// accepts these arguments as long as the column type matches.
///
/// Similarly, if the CREATE TABLE FUNCTION statement includes a "RETURNS
/// TABLE" section with a single column with no name, then this defines a
/// value-table return type. The function then returns a value table as long
/// as the SQL body returns a single column whose type matches (independent of
/// whether the SQL body result is a value table or not, and whether the
/// returned column is named or unnamed).
///
/// --------------------------------
/// Templated Table-Valued Functions
/// --------------------------------
///
/// ZetaSQL supports table-valued function declarations with parameters of
/// type ANY TABLE. This type indicates that any schema is valid for tables
/// passed for this parameter. In this case:
///
/// * the IsTemplated() method of the <signature> field returns true,
/// * the <output_column_list> field is empty,
/// * the <is_value_table> field is set to a default value of false (since
///   ZetaSQL cannot analyze the function body in the presence of templated
///   parameters, it is not possible to detect this property yet),
///
/// TODO: Update this description once ZetaSQL supports more types
/// of templated function parameters. Currently only ANY TABLE is supported.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedCreateTableFunctionStmtProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedCreateStatementProto>,
    #[prost(string, repeated, tag = "2")]
    pub argument_name_list: ::std::vec::Vec<std::string::String>,
    #[prost(message, optional, tag = "3")]
    pub signature: ::std::option::Option<FunctionSignatureProto>,
    #[prost(message, repeated, tag = "4")]
    pub option_list: ::std::vec::Vec<ResolvedOptionProto>,
    #[prost(string, optional, tag = "5")]
    pub language: ::std::option::Option<std::string::String>,
    #[prost(string, optional, tag = "6")]
    pub code: ::std::option::Option<std::string::String>,
    #[prost(message, optional, tag = "7")]
    pub query: ::std::option::Option<AnyResolvedScanProto>,
    #[prost(message, repeated, tag = "8")]
    pub output_column_list: ::std::vec::Vec<ResolvedOutputColumnProto>,
    #[prost(bool, optional, tag = "9")]
    pub is_value_table: ::std::option::Option<bool>,
    #[prost(
        enumeration = "resolved_create_statement_enums::SqlSecurity",
        optional,
        tag = "10"
    )]
    pub sql_security: ::std::option::Option<i32>,
}
/// This represents a relation argument reference in a table-valued function's
/// body. The 'column_list' of this ResolvedScan includes column names from
/// the relation argument in the table-valued function signature.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedRelationArgumentScanProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedScanProto>,
    /// This is the name of the relation argument for the table-valued
    /// function.  It is used to match this relation argument reference in
    /// a TVF SQL function body with one of possibly several relation
    /// arguments in the TVF call.
    #[prost(string, optional, tag = "2")]
    pub name: ::std::option::Option<std::string::String>,
    /// If true, the result of this query is a value table. Rather than
    /// producing rows with named columns, it produces rows with a single
    /// unnamed value type. See (broken link).
    #[prost(bool, optional, tag = "3")]
    pub is_value_table: ::std::option::Option<bool>,
}
/// This statement: [ (<arg_list>) ];
///
/// <arg_list> is an optional list of parameters.  If given, each parameter
///            may consist of a type, or a name and a type.
///
/// NOTE: This can be considered deprecated in favor of the FunctionSignature
///       stored directly in the statement.
///
/// NOTE: ResolvedArgumentList is not related to the ResolvedArgument class,
///       which just exists to organize node classes.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedArgumentListProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedArgumentProto>,
    #[prost(message, repeated, tag = "2")]
    pub arg_list: ::std::vec::Vec<ResolvedArgumentDefProto>,
}
/// This wrapper is used for an optional FunctionSignature.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedFunctionSignatureHolderProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedArgumentProto>,
    #[prost(message, optional, tag = "2")]
    pub signature: ::std::option::Option<FunctionSignatureProto>,
}
/// This statement: DROP FUNCTION [IF EXISTS] <name_path>
///   [ (<arguments>) ];
///
/// <is_if_exists> silently ignore the "name_path does not exist" error.
/// <name_path> is the identifier path of the function to be dropped.
/// <arguments> is an optional list of parameters.  If given, each parameter
///            may consist of a type, or a name and a type.  The name is
///            disregarded, and is allowed to permit copy-paste from CREATE
///            FUNCTION statements.
/// <signature> is the signature of the dropped function.  Argument names and
///            argument options are ignored because only the types matter
///            for matching signatures in DROP FUNCTION.  The return type
///            in this signature will always be <void>, since return type
///            is ignored when matching signatures for DROP.
///            TODO <arguments> could be deprecated in favor of this.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedDropFunctionStmtProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedStatementProto>,
    #[prost(bool, optional, tag = "2")]
    pub is_if_exists: ::std::option::Option<bool>,
    #[prost(string, repeated, tag = "3")]
    pub name_path: ::std::vec::Vec<std::string::String>,
    /// NOTE: arguments for DROP FUNCTION statements are matched only on
    /// type; names for any arguments in ResolvedArgumentList will be set
    /// to the empty string irrespective of whether or not argument names
    /// were given in the DROP FUNCTION statement.
    #[prost(message, optional, tag = "4")]
    pub arguments: ::std::option::Option<ResolvedArgumentListProto>,
    /// NOTE: arguments for DROP FUNCTION statements are matched only on
    /// type; names are irrelevant, so no argument names are saved to use
    /// with this signature.  Additionally, the return type will always be
    /// <void>, since return types are ignored for DROP FUNCTION.
    #[prost(message, optional, tag = "5")]
    pub signature: ::std::option::Option<ResolvedFunctionSignatureHolderProto>,
}
/// This statement: CALL <procedure>;
///
/// <procedure> Procedure to call.
/// <signature> Resolved FunctionSignature for this procedure.
/// <argument_list> Procedure arguments.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedCallStmtProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedStatementProto>,
    #[prost(message, optional, tag = "2")]
    pub procedure: ::std::option::Option<ProcedureRefProto>,
    #[prost(message, optional, tag = "3")]
    pub signature: ::std::option::Option<FunctionSignatureProto>,
    #[prost(message, repeated, tag = "4")]
    pub argument_list: ::std::vec::Vec<AnyResolvedExprProto>,
}
/// This statement: IMPORT <import_kind>
///                              [<name_path> [AS|INTO <alias_path>]
///                              |<file_path>]
///                        [<option_list>];
///
/// <import_kind> The type of the object, currently supports MODULE and PROTO.
/// <name_path>   The identifier path of the object to import, e.g., foo.bar,
///               used in IMPORT MODULE statement.
/// <file_path>   The file path of the object to import, e.g., "file.proto",
///               used in IMPORT PROTO statement.
/// <alias_path>  The AS alias path for the object.
/// <into_alias_path>  The INTO alias path for the object.
/// <option_list> Engine-specific directives for the import.
///
/// Either <name_path> or <file_path> will be populated but not both.
///       <name_path> will be populated for IMPORT MODULE.
///       <file_path> will be populated for IMPORT PROTO.
///
/// At most one of <alias_path> or <into_alias_path> will be populated.
///       <alias_path> may be populated for IMPORT MODULE.
///       <into_alias_path> may be populated for IMPORT PROTO.
///
/// IMPORT MODULE and IMPORT PROTO both support options.
///
/// See (broken link) for more detail on IMPORT MODULE.
/// See (broken link) for more detail on IMPORT PROTO.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedImportStmtProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedStatementProto>,
    #[prost(
        enumeration = "resolved_import_stmt_enums::ImportKind",
        optional,
        tag = "2"
    )]
    pub import_kind: ::std::option::Option<i32>,
    #[prost(string, repeated, tag = "3")]
    pub name_path: ::std::vec::Vec<std::string::String>,
    #[prost(string, optional, tag = "4")]
    pub file_path: ::std::option::Option<std::string::String>,
    #[prost(string, repeated, tag = "5")]
    pub alias_path: ::std::vec::Vec<std::string::String>,
    #[prost(string, repeated, tag = "7")]
    pub into_alias_path: ::std::vec::Vec<std::string::String>,
    #[prost(message, repeated, tag = "6")]
    pub option_list: ::std::vec::Vec<ResolvedOptionProto>,
}
/// This statement: MODULE <name_path> [<option_list>];
///
/// <name_path> is the identifier path of the module.
/// <option_list> Engine-specific directives for the module statement.
///
/// See (broken link) for more detail on MODULEs.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedModuleStmtProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedStatementProto>,
    #[prost(string, repeated, tag = "2")]
    pub name_path: ::std::vec::Vec<std::string::String>,
    #[prost(message, repeated, tag = "3")]
    pub option_list: ::std::vec::Vec<ResolvedOptionProto>,
}
/// This represents a HAVING MAX or HAVING MIN modifier in an aggregate
/// expression. If an aggregate has arguments (x HAVING {MAX/MIN} y),
/// the aggregate will be computed over only the x values in the rows with the
/// maximal/minimal values of y.
///
/// <kind> the MAX/MIN kind of this HAVING
/// <having_expr> the HAVING expression (y in the above example)
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedAggregateHavingModifierProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedArgumentProto>,
    #[prost(
        enumeration = "resolved_aggregate_having_modifier_enums::HavingModifierKind",
        optional,
        tag = "2"
    )]
    pub kind: ::std::option::Option<i32>,
    #[prost(message, optional, boxed, tag = "3")]
    pub having_expr: ::std::option::Option<::std::boxed::Box<AnyResolvedExprProto>>,
}
/// This statement:
///   CREATE MATERIALIZED VIEW <name> [PARTITION BY expr, ...]
///   [CLUSTER BY expr, ...] [OPTIONS (...)] AS SELECT ...
///
/// <column_definition_list> matches 1:1 with the <output_column_list> in
/// ResolvedCreateViewBase and provides explicit definition for each
/// ResolvedColumn produced by <query>. Output column names and types must
/// match column definition names and types. If the table is a value table,
/// <column_definition_list> must have exactly one column, with a generated
/// name such as "$struct".
///
/// Currently <column_definition_list> contains the same schema information
/// (column names and types) as <output_definition_list>, but when/if we
/// allow specifying column OPTIONS as part of CMV statement, this information
/// will be available only in <column_definition_list>. Therefore, consumers
/// are encouraged to read from <column_definition_list> rather than from
/// <output_column_list> to determine the schema, if possible.
///
/// <partition_by_list> specifies the partitioning expressions for the
///                     materialized view.
/// <cluster_by_list> specifies the clustering expressions for the
///                   materialized view.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedCreateMaterializedViewStmtProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedCreateViewBaseProto>,
    #[prost(message, repeated, tag = "2")]
    pub column_definition_list: ::std::vec::Vec<ResolvedColumnDefinitionProto>,
    #[prost(message, repeated, tag = "3")]
    pub partition_by_list: ::std::vec::Vec<AnyResolvedExprProto>,
    #[prost(message, repeated, tag = "4")]
    pub cluster_by_list: ::std::vec::Vec<AnyResolvedExprProto>,
}
/// This statement creates a user-defined procedure:
/// CREATE [OR REPLACE] [TEMP] PROCEDURE [IF NOT EXISTS] <name_path>
/// (<arg_list>) [OPTIONS (<option_list>)]
/// BEGIN
/// <procedure_body>
/// END;
///
/// <name_path> is the identifier path of the procedure.
/// <argument_name_list> The names of the function arguments.
/// <signature> is the FunctionSignature of the created procedure, with all
///        options.  This can be used to create a procedure to load into a
///        Catalog for future queries.
/// <option_list> has engine-specific directives for modifying procedures.
/// <procedure_body> is a string literal that contains the procedure body.
///        It includes everything from the BEGIN keyword to the END keyword,
///        inclusive.
///
///        The resolver will perform some basic validation on the procedure
///        body, for example, verifying that DECLARE statements are in the
///        proper position, and that variables are not declared more than
///        once, but any validation that requires the catalog (including
///        generating resolved tree nodes for individual statements) is
///        deferred until the procedure is actually called.  This deferral
///        makes it possible to define a procedure which references a table
///        or routine that does not yet exist, so long as the entity is
///        created before the procedure is called.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedCreateProcedureStmtProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedCreateStatementProto>,
    #[prost(string, repeated, tag = "2")]
    pub argument_name_list: ::std::vec::Vec<std::string::String>,
    #[prost(message, optional, tag = "3")]
    pub signature: ::std::option::Option<FunctionSignatureProto>,
    #[prost(message, repeated, tag = "4")]
    pub option_list: ::std::vec::Vec<ResolvedOptionProto>,
    #[prost(string, optional, tag = "5")]
    pub procedure_body: ::std::option::Option<std::string::String>,
}
/// An argument for an EXECUTE IMMEDIATE's USING clause.
///
/// <name> an optional name for this expression
/// <expression> the expression's value
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedExecuteImmediateArgumentProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedArgumentProto>,
    #[prost(string, optional, tag = "2")]
    pub name: ::std::option::Option<std::string::String>,
    #[prost(message, optional, tag = "3")]
    pub expression: ::std::option::Option<AnyResolvedExprProto>,
}
/// An EXECUTE IMMEDIATE statement
/// EXECUTE IMMEDIATE <sql> [<into_clause>] [<using_clause>]
///
/// <sql> a string expression indicating a SQL statement to be dynamically
///   executed
/// <into_identifier_list> the identifiers whose values should be set.
///   Identifiers should not be repeated in the list.
/// <using_argument_list> a list of arguments to supply for dynamic SQL.
///    The arguments should either be all named or all unnamed, and
///    arguments should not be repeated in the list.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedExecuteImmediateStmtProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedStatementProto>,
    #[prost(message, optional, tag = "2")]
    pub sql: ::std::option::Option<AnyResolvedExprProto>,
    #[prost(string, repeated, tag = "3")]
    pub into_identifier_list: ::std::vec::Vec<std::string::String>,
    #[prost(message, repeated, tag = "4")]
    pub using_argument_list: ::std::vec::Vec<ResolvedExecuteImmediateArgumentProto>,
}
/// An assignment of a value to another value.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResolvedAssignmentStmtProto {
    #[prost(message, optional, tag = "1")]
    pub parent: ::std::option::Option<ResolvedStatementProto>,
    /// Target of the assignment.  Currently, this will be either ResolvedSystemVariable, or a chain of ResolveGetField operations around it.
    #[prost(message, optional, tag = "2")]
    pub target: ::std::option::Option<AnyResolvedExprProto>,
    /// Value to assign into the target.  This will always be the same type as the target.
    #[prost(message, optional, tag = "3")]
    pub expr: ::std::option::Option<AnyResolvedExprProto>,
}
/// Enum for types of ResolvedNode classes.
/// Generated as a separate file to avoid circular dependencies.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ResolvedNodeKind {
    ResolvedLiteral = 3,
    ResolvedParameter = 4,
    ResolvedExpressionColumn = 5,
    ResolvedColumnRef = 6,
    ResolvedConstant = 103,
    ResolvedSystemVariable = 139,
    ResolvedFunctionCall = 8,
    ResolvedAggregateFunctionCall = 9,
    ResolvedAnalyticFunctionCall = 10,
    ResolvedCast = 11,
    ResolvedMakeStruct = 12,
    ResolvedMakeProto = 13,
    ResolvedMakeProtoField = 14,
    ResolvedGetStructField = 15,
    ResolvedGetProtoField = 16,
    ResolvedReplaceFieldItem = 128,
    ResolvedReplaceField = 129,
    ResolvedSubqueryExpr = 17,
    ResolvedModel = 109,
    ResolvedConnection = 141,
    ResolvedDescriptor = 144,
    ResolvedSingleRowScan = 19,
    ResolvedTableScan = 20,
    ResolvedJoinScan = 21,
    ResolvedArrayScan = 22,
    ResolvedColumnHolder = 23,
    ResolvedFilterScan = 24,
    ResolvedGroupingSet = 93,
    ResolvedAggregateScan = 25,
    ResolvedSetOperationItem = 94,
    ResolvedSetOperationScan = 26,
    ResolvedOrderByScan = 27,
    ResolvedLimitOffsetScan = 28,
    ResolvedWithRefScan = 29,
    ResolvedAnalyticScan = 30,
    ResolvedSampleScan = 31,
    ResolvedComputedColumn = 32,
    ResolvedOrderByItem = 33,
    ResolvedColumnAnnotations = 104,
    ResolvedGeneratedColumnInfo = 105,
    ResolvedColumnDefinition = 91,
    ResolvedPrimaryKey = 92,
    ResolvedForeignKey = 110,
    ResolvedCheckConstraint = 113,
    ResolvedOutputColumn = 34,
    ResolvedProjectScan = 35,
    ResolvedTvfscan = 81,
    ResolvedTvfargument = 82,
    ResolvedExplainStmt = 37,
    ResolvedQueryStmt = 38,
    ResolvedCreateDatabaseStmt = 95,
    ResolvedIndexItem = 96,
    ResolvedUnnestItem = 126,
    ResolvedCreateIndexStmt = 97,
    ResolvedCreateTableStmt = 90,
    ResolvedCreateTableAsSelectStmt = 40,
    ResolvedCreateModelStmt = 107,
    ResolvedCreateViewStmt = 41,
    ResolvedCreateExternalTableStmt = 42,
    ResolvedExportDataStmt = 43,
    ResolvedDefineTableStmt = 44,
    ResolvedDescribeStmt = 45,
    ResolvedShowStmt = 46,
    ResolvedBeginStmt = 47,
    ResolvedSetTransactionStmt = 120,
    ResolvedCommitStmt = 48,
    ResolvedRollbackStmt = 49,
    ResolvedStartBatchStmt = 122,
    ResolvedRunBatchStmt = 123,
    ResolvedAbortBatchStmt = 124,
    ResolvedDropStmt = 50,
    ResolvedDropMaterializedViewStmt = 121,
    ResolvedWithScan = 51,
    ResolvedWithEntry = 52,
    ResolvedOption = 53,
    ResolvedWindowPartitioning = 54,
    ResolvedWindowOrdering = 55,
    ResolvedWindowFrame = 56,
    ResolvedAnalyticFunctionGroup = 57,
    ResolvedWindowFrameExpr = 58,
    ResolvedDmlvalue = 59,
    ResolvedDmldefault = 60,
    ResolvedAssertStmt = 98,
    ResolvedAssertRowsModified = 61,
    ResolvedInsertRow = 62,
    ResolvedInsertStmt = 63,
    ResolvedDeleteStmt = 64,
    ResolvedUpdateItem = 65,
    ResolvedUpdateArrayItem = 102,
    ResolvedUpdateStmt = 66,
    ResolvedMergeWhen = 100,
    ResolvedMergeStmt = 101,
    ResolvedTruncateStmt = 133,
    ResolvedPrivilege = 67,
    ResolvedGrantStmt = 69,
    ResolvedRevokeStmt = 70,
    ResolvedAlterDatabaseStmt = 134,
    ResolvedAlterMaterializedViewStmt = 127,
    ResolvedAlterTableStmt = 115,
    ResolvedAlterViewStmt = 118,
    ResolvedSetOptionsAction = 117,
    ResolvedAddColumnAction = 131,
    ResolvedDropColumnAction = 132,
    ResolvedAlterTableSetOptionsStmt = 71,
    ResolvedRenameStmt = 72,
    ResolvedCreateRowAccessPolicyStmt = 73,
    ResolvedDropRowAccessPolicyStmt = 74,
    ResolvedGrantToAction = 135,
    ResolvedFilterUsingAction = 136,
    ResolvedRevokeFromAction = 137,
    ResolvedRenameToAction = 138,
    ResolvedAlterRowAccessPolicyStmt = 75,
    ResolvedCreateConstantStmt = 99,
    ResolvedCreateFunctionStmt = 76,
    ResolvedArgumentDef = 77,
    ResolvedArgumentRef = 78,
    ResolvedCreateTableFunctionStmt = 88,
    ResolvedRelationArgumentScan = 89,
    ResolvedArgumentList = 79,
    ResolvedFunctionSignatureHolder = 84,
    ResolvedDropFunctionStmt = 80,
    ResolvedCallStmt = 83,
    ResolvedImportStmt = 86,
    ResolvedModuleStmt = 87,
    ResolvedAggregateHavingModifier = 85,
    ResolvedCreateMaterializedViewStmt = 119,
    ResolvedCreateProcedureStmt = 125,
    ResolvedExecuteImmediateArgument = 143,
    ResolvedExecuteImmediateStmt = 140,
    ResolvedAssignmentStmt = 142,
    /// User code that switches on this enum must have a default case so
    /// builds won't break if new enums get added.
    SwitchMustHaveDefault = -1,
}
/// A unique ID for ZetaSQL function signatures.  Resolved ZetaSQL functions
/// will provide one of these enums, and ZetaSQL implementations should map
/// them to something they can evaluate.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum FunctionSignatureId {
    /// User code that switches on this enum must have a default case so
    /// builds won't break if new enums get added.
    SwitchMustHaveADefault = -1,
    FnInvalidFunctionId = 1,
    // The first set of functions do not use standard function call syntax,
    // reflecting operators, functions with infix notation (LIKE), and
    // other special functions (CASE).  FunctionSignatureIds are assigned
    // in ranges:
    //
    // 0002-0999 Non-standard function calls   (NextId: 266)
    // 1000-1099 String functions              (NextId: 1065)
    // 1100-1199 Control flow functions        (NextId: 1104)
    // 1200-1299 Time functions                (Fully used)
    // 1300-1399 Math functions                (NextId: 1393)
    // 1400-1499 Aggregate functions           (NextId: 1478)
    // 1500-1599 Analytic functions            (NextId: 1513)
    // 1600-1699 Misc functions                (NextId: 1682)
    // 1700-1799 Net functions                 (NextId: 1716)
    // 1800-1899 More time functions           (NextId: 1833)
    // 1900-1999 Hashing/encryption functions  (NextId: 1924)
    // 2000-2199 Geography functions           (NextId: 2063)
    /// enum value                       // Related function name
    /// ----------                       // ---------------------
    ///
    /// $add
    FnAddDouble = 2,
    /// $add
    FnAddInt64 = 4,
    /// $add
    FnAddUint64 = 119,
    /// $add
    FnAddNumeric = 248,
    /// $add
    FnAddBignumeric = 261,
    /// $and
    FnAnd = 5,
    /// $case_no_value
    FnCaseNoValue = 6,
    /// $case_with_value
    FnCaseWithValue = 7,
    /// $divide
    FnDivideDouble = 40,
    /// $divide
    FnDivideNumeric = 250,
    /// $divide
    FnDivideBignumeric = 263,
    /// $greater
    FnGreater = 107,
    /// $greater
    FnGreaterInt64Uint64 = 222,
    /// $greater
    FnGreaterUint64Int64 = 223,
    /// $greater_or_equal
    FnGreaterOrEqual = 108,
    /// $greater_or_equal
    FnGreaterOrEqualInt64Uint64 = 224,
    /// $greater_or_equal
    FnGreaterOrEqualUint64Int64 = 225,
    /// $less
    FnLess = 105,
    /// $less
    FnLessInt64Uint64 = 226,
    /// $less
    FnLessUint64Int64 = 227,
    /// $less_or_equal
    FnLessOrEqual = 106,
    /// $less_or_equal
    FnLessOrEqualInt64Uint64 = 228,
    /// $less_or_equal
    FnLessOrEqualUint64Int64 = 229,
    /// $equal
    FnEqual = 42,
    /// $equal
    FnEqualInt64Uint64 = 230,
    /// $equal
    FnEqualUint64Int64 = 231,
    /// $like
    FnStringLike = 97,
    /// $like
    FnByteLike = 98,
    /// $in
    FnIn = 100,
    /// $in_array
    FnInArray = 219,
    /// $between
    FnBetween = 110,
    /// $between
    FnBetweenInt64Uint64Uint64 = 254,
    /// $between
    FnBetweenInt64Uint64Int64 = 255,
    /// $between
    FnBetweenInt64Int64Uint64 = 256,
    /// $between
    FnBetweenUint64Int64Int64 = 257,
    /// $between
    FnBetweenUint64Uint64Int64 = 258,
    /// $between
    FnBetweenUint64Int64Uint64 = 259,
    /// $is_null
    FnIsNull = 101,
    /// $is_true
    FnIsTrue = 102,
    /// $is_false
    FnIsFalse = 103,
    /// $multiply
    FnMultiplyDouble = 41,
    /// $multiply
    FnMultiplyInt64 = 44,
    /// $multiply
    FnMultiplyUint64 = 114,
    /// $multiply
    FnMultiplyNumeric = 251,
    /// $multiply
    FnMultiplyBignumeric = 264,
    /// $not
    FnNot = 45,
    /// $not_equal
    FnNotEqual = 109,
    /// $not_equal
    FnNotEqualInt64Uint64 = 232,
    /// $not_equal
    FnNotEqualUint64Int64 = 233,
    /// $or
    FnOr = 46,
    /// $subtract
    FnSubtractDouble = 115,
    /// $subtract
    FnSubtractInt64 = 48,
    /// $subtract
    FnSubtractUint64 = 117,
    /// $subtract
    FnSubtractNumeric = 249,
    /// $subtract
    FnSubtractBignumeric = 262,
    /// $unary_minus
    FnUnaryMinusInt32 = 83,
    /// $unary_minus
    FnUnaryMinusInt64 = 84,
    /// $unary_minus
    FnUnaryMinusFloat = 87,
    /// $unary_minus
    FnUnaryMinusDouble = 88,
    /// $unary_minus
    FnUnaryMinusNumeric = 252,
    /// $unary_minus
    FnUnaryMinusBignumeric = 265,
    /// Bitwise unary operators.
    ///
    /// $bitwise_not
    FnBitwiseNotInt32 = 120,
    /// $bitwise_not
    FnBitwiseNotInt64 = 121,
    /// $bitwise_not
    FnBitwiseNotUint32 = 122,
    /// $bitwise_not
    FnBitwiseNotUint64 = 123,
    /// $bitwise_not
    FnBitwiseNotBytes = 241,
    /// Bitwise binary operators.
    ///
    /// $bitwise_or
    FnBitwiseOrInt32 = 124,
    /// $bitwise_or
    FnBitwiseOrInt64 = 125,
    /// $bitwise_or
    FnBitwiseOrUint32 = 126,
    /// $bitwise_or
    FnBitwiseOrUint64 = 127,
    /// $bitwise_or
    FnBitwiseOrBytes = 242,
    /// $bitwise_xor
    FnBitwiseXorInt32 = 128,
    /// $bitwise_xor
    FnBitwiseXorInt64 = 129,
    /// $bitwise_xor
    FnBitwiseXorUint32 = 130,
    /// $bitwise_xor
    FnBitwiseXorUint64 = 131,
    /// $bitwise_xor
    FnBitwiseXorBytes = 243,
    /// $bitwise_and
    FnBitwiseAndInt32 = 132,
    /// $bitwise_and
    FnBitwiseAndInt64 = 133,
    /// $bitwise_and
    FnBitwiseAndUint32 = 134,
    /// $bitwise_and
    FnBitwiseAndUint64 = 135,
    /// $bitwise_and
    FnBitwiseAndBytes = 244,
    /// For all bitwise shift operators, the second argument has int64 type.
    /// Expected behavior of bitwise shift operations:
    /// * Shifting by a negative offset is an error.
    /// * Shifting by >= 64 for uint64/int64 and >= 32 for int32/uint32 gives 0.
    /// * Shifting right on signed values does not do sign extension.
    ///
    /// $bitwise_left_shift
    FnBitwiseLeftShiftInt32 = 136,
    /// $bitwise_left_shift
    FnBitwiseLeftShiftInt64 = 137,
    /// $bitwise_left_shift
    FnBitwiseLeftShiftUint32 = 138,
    /// $bitwise_left_shift
    FnBitwiseLeftShiftUint64 = 139,
    /// $bitwise_left_shift
    FnBitwiseLeftShiftBytes = 245,
    /// $bitwise_right_shift
    FnBitwiseRightShiftInt32 = 140,
    /// $bitwise_right_shift
    FnBitwiseRightShiftInt64 = 141,
    /// $bitwise_right_shift
    FnBitwiseRightShiftUint32 = 142,
    /// $bitwise_right_shift
    FnBitwiseRightShiftUint64 = 143,
    /// $bitwise_right_shift
    FnBitwiseRightShiftBytes = 246,
    /// BIT_COUNT functions.
    ///
    /// bit_count(int32) -> int64
    FnBitCountInt32 = 144,
    /// bit_count(int64) -> int64
    FnBitCountInt64 = 145,
    /// bit_count(uint64) -> int64
    FnBitCountUint64 = 146,
    /// bit_count(bytes) -> int64
    FnBitCountBytes = 247,
    /// TODO: Need to assign these proper ids since they have
    /// standard function call syntax.
    ///
    /// array_length(array) -> int64
    FnArrayLength = 220,
    /// $make_array
    FnMakeArray = 218,
    /// $array_at_offset
    FnArrayAtOffset = 234,
    /// $array_at_ordinal
    FnArrayAtOrdinal = 235,
    /// $safe_array_at_offset
    FnSafeArrayAtOffset = 239,
    /// $safe_array_at_ordinal
    FnSafeArrayAtOrdinal = 240,
    /// array_concat(repeated array) -> array
    FnArrayConcat = 236,
    /// array_concat(array, array) -> array
    FnArrayConcatOp = 260,
    /// array_to_string(array, string[, string]) -> string
    FnArrayToString = 237,
    /// array_to_string(array, bytes[, bytes]) -> bytes
    FnArrayToBytes = 238,
    /// error(string) -> {unused result, coercible to any type}
    FnError = 253,
    /// $count_star
    FnCountStar = 57,
    // The following functions use standard function call syntax.
    // TODO: We may want to move all of these into another ID space
    // separating them from true built-in functions declared above.
    /// String functions
    ///
    /// concat(repeated string) -> string
    FnConcatString = 1000,
    /// concat(repeated bytes) -> bytes
    FnConcatBytes = 1001,
    /// concat(string, string) -> string
    FnConcatOpString = 1063,
    /// concat(bytes, bytes) -> bytes
    FnConcatOpBytes = 1064,
    /// strpos(string, string) -> string
    FnStrposString = 1002,
    /// strpos(bytes, bytes) -> bytes
    FnStrposBytes = 1003,
    /// lower(string) -> string
    FnLowerString = 1006,
    /// lower(bytes) -> bytes
    FnLowerBytes = 1007,
    /// upper(string) -> string
    FnUpperString = 1008,
    /// upper(bytes) -> bytes
    FnUpperBytes = 1009,
    /// length(string) -> int64
    FnLengthString = 1010,
    /// length(bytes) -> int64
    FnLengthBytes = 1011,
    /// starts_with(string, string) -> string
    FnStartsWithString = 1012,
    /// starts_with(bytes, bytes) -> bytes
    FnStartsWithBytes = 1013,
    /// ends_with(string, string) -> string
    FnEndsWithString = 1014,
    /// ends_with(bytes, bytes) -> bytes
    FnEndsWithBytes = 1015,
    /// substr(string, int64[, int64]) -> string
    FnSubstrString = 1016,
    /// substr(bytes, int64[, int64]) -> bytes
    FnSubstrBytes = 1017,
    /// trim(string[, string]) -> string
    FnTrimString = 1018,
    /// trim(bytes, bytes) -> bytes
    FnTrimBytes = 1019,
    /// ltrim(string[, string]) -> string
    FnLtrimString = 1020,
    /// ltrim(bytes, bytes) -> bytes
    FnLtrimBytes = 1021,
    /// rtrim(string[, string]) -> string
    FnRtrimString = 1022,
    /// rtrim(bytes, bytes) -> bytes
    FnRtrimBytes = 1023,
    /// replace(string, string, string) -> string
    FnReplaceString = 1024,
    /// replace(bytes, bytes, bytes) -> bytes
    FnReplaceBytes = 1025,
    /// regexp_match(string, string) -> bool
    FnRegexpMatchString = 1026,
    /// regexp_match(bytes, bytes) -> bool
    FnRegexpMatchBytes = 1027,
    /// regexp_extract(string, string) -> string
    FnRegexpExtractString = 1028,
    /// regexp_extract(bytes, bytes) -> bytes
    FnRegexpExtractBytes = 1029,
    FnRegexpReplaceString = 1030,
    /// regexp_replace(string, string, string) -> string
    FnRegexpReplaceBytes = 1031,
    /// regexp_replace(bytes, bytes, bytes) -> bytes
    FnRegexpExtractAllString = 1032,
    /// regexp_extract_all(string, string) -> array of string
    FnRegexpExtractAllBytes = 1033,
    /// regexp_extract_all(bytes, bytes) -> array of bytes
    ///
    /// byte_length(string) -> int64
    FnByteLengthString = 1034,
    /// byte_length(bytes) -> int64
    FnByteLengthBytes = 1035,
    /// semantically identical to FN_LENGTH_BYTES
    ///
    /// char_length(string) -> int64
    FnCharLengthString = 1036,
    /// semantically identical to FN_LENGTH_STRING
    ///
    /// split(string, string) -> array of string
    FnSplitString = 1038,
    /// split(bytes, bytes) -> array of bytes
    FnSplitBytes = 1039,
    /// regexp_contains(string, string) -> bool
    FnRegexpContainsString = 1040,
    /// regexp_contains(bytes, bytes) -> bool
    FnRegexpContainsBytes = 1041,
    /// Converts bytes to string by replacing invalid UTF-8 characters with
    /// replacement char U+FFFD.
    FnSafeConvertBytesToString = 1042,
    /// Unicode normalization and casefolding functions.
    ///
    /// normalize(string [, mode]) -> string
    FnNormalizeString = 1043,
    /// normalize_and_casefold(string [, mode]) -> string
    FnNormalizeAndCasefoldString = 1044,
    /// to_base64(bytes) -> string
    FnToBase64 = 1045,
    /// from_base64(string) -> bytes
    FnFromBase64 = 1046,
    /// to_hex(bytes) -> string
    FnToHex = 1059,
    /// from_hex(string) -> bytes
    FnFromHex = 1060,
    /// to_base32(bytes) -> string
    FnToBase32 = 1061,
    /// from_base32(string) -> bytes
    FnFromBase32 = 1062,
    /// to_code_points(string) -> array<int64>
    FnToCodePointsString = 1047,
    /// to_code_points(bytes) -> array<int64>
    FnToCodePointsBytes = 1048,
    /// code_points_to_string(array<int64>) -> string
    FnCodePointsToString = 1049,
    /// code_points_to_bytes(array<int64>) -> bytes
    FnCodePointsToBytes = 1050,
    /// lpad(bytes, int64[, bytes]) -> bytes
    FnLpadBytes = 1051,
    /// lpad(string, int64[, string]) -> string
    FnLpadString = 1052,
    /// rpad(bytes, int64[, bytes]) -> bytes
    FnRpadBytes = 1053,
    /// rpad(string, int64[, string]) -> string
    FnRpadString = 1054,
    /// repeat(bytes, int64) -> bytes
    FnRepeatBytes = 1055,
    /// repeat(string, int64) -> string
    FnRepeatString = 1056,
    /// reverse(string) -> string
    FnReverseString = 1057,
    /// reverse(bytes) -> bytes
    FnReverseBytes = 1058,
    /// Control flow functions
    ///
    /// if
    FnIf = 1100,
    /// Coalesce is used to express the output join column in FULL JOIN.
    ///
    /// coalesce
    FnCoalesce = 1101,
    /// ifnull
    FnIfnull = 1102,
    /// nullif
    FnNullif = 1103,
    /// Time functions
    ///
    /// current_date
    FnCurrentDate = 1200,
    /// current_datetime
    FnCurrentDatetime = 1804,
    /// current_time
    FnCurrentTime = 1805,
    /// current_timestamp
    FnCurrentTimestamp = 1260,
    /// date_add
    FnDateAddDate = 1205,
    /// datetime_add
    FnDatetimeAdd = 1812,
    /// time_add
    FnTimeAdd = 1813,
    /// timestamp_add
    FnTimestampAdd = 1261,
    /// date_diff
    FnDateDiffDate = 1210,
    /// datetime_diff
    FnDatetimeDiff = 1816,
    /// time_diff
    FnTimeDiff = 1817,
    /// timestamp_diff
    FnTimestampDiff = 1262,
    /// date_sub
    FnDateSubDate = 1215,
    /// datetime_sub
    FnDatetimeSub = 1814,
    /// time_sub
    FnTimeSub = 1815,
    /// timestamp_sub
    FnTimestampSub = 1263,
    /// date_trunc
    FnDateTruncDate = 1220,
    /// datetime_trunc
    FnDatetimeTrunc = 1818,
    /// time_trunc
    FnTimeTrunc = 1819,
    /// timestamp_trunc
    FnTimestampTrunc = 1264,
    /// date_from_unix_date
    FnDateFromUnixDate = 1225,
    /// timestamp_seconds
    FnTimestampFromInt64Seconds = 1289,
    /// timestamp_millis
    FnTimestampFromInt64Millis = 1290,
    /// timestamp_micros
    FnTimestampFromInt64Micros = 1291,
    /// timestamp_from_unix_seconds
    FnTimestampFromUnixSecondsInt64 = 1827,
    /// timestamp_from_unix_seconds
    FnTimestampFromUnixSecondsTimestamp = 1828,
    /// timestamp_from_unix_millis
    FnTimestampFromUnixMillisInt64 = 1829,
    /// timestamp_from_unix_millis
    FnTimestampFromUnixMillisTimestamp = 1830,
    /// timestamp_from_unix_micros
    FnTimestampFromUnixMicrosInt64 = 1831,
    /// timestamp_from_unix_micros
    FnTimestampFromUnixMicrosTimestamp = 1832,
    /// unix_date
    FnUnixDate = 1230,
    FnUnixSecondsFromTimestamp = 1268,
    FnUnixMillisFromTimestamp = 1269,
    FnUnixMicrosFromTimestamp = 1270,
    /// date
    FnDateFromTimestamp = 1271,
    /// date
    FnDateFromDatetime = 1826,
    /// date
    FnDateFromYearMonthDay = 1297,
    /// timestamp
    FnTimestampFromString = 1272,
    /// timestamp
    FnTimestampFromDate = 1273,
    /// timestamp
    FnTimestampFromDatetime = 1801,
    /// time
    FnTimeFromHourMinuteSecond = 1298,
    /// time
    FnTimeFromTimestamp = 1802,
    /// time
    FnTimeFromDatetime = 1825,
    /// datetime
    FnDatetimeFromDateAndTime = 1299,
    /// datetime
    FnDatetimeFromYearMonthDayHourMinuteSecond = 1800,
    /// datetime
    FnDatetimeFromTimestamp = 1803,
    /// datetime
    FnDatetimeFromDate = 1824,
    /// string
    FnStringFromTimestamp = 1274,
    /// Signatures for extracting date parts, taking a date/timestamp
    /// and the target date part as arguments.
    ///
    /// $extract
    FnExtractFromDate = 1251,
    /// $extract
    FnExtractFromDatetime = 1806,
    /// $extract
    FnExtractFromTime = 1807,
    /// $extract
    FnExtractFromTimestamp = 1275,
    /// Signatures specific to extracting the DATE date part from a DATETIME or a
    /// TIMESTAMP.
    ///
    /// $extract_date
    FnExtractDateFromDatetime = 1808,
    /// $extract_date
    FnExtractDateFromTimestamp = 1276,
    /// Signatures specific to extracting the TIME date part from a DATETIME or a
    /// TIMESTAMP.
    ///
    /// $extract_time
    FnExtractTimeFromDatetime = 1809,
    /// $extract_time
    FnExtractTimeFromTimestamp = 1810,
    /// Signature specific to extracting the DATETIME date part from a TIMESTAMP.
    ///
    /// $extract_datetime
    FnExtractDatetimeFromTimestamp = 1811,
    /// format_date
    FnFormatDate = 1293,
    /// format_datetime
    FnFormatDatetime = 1820,
    /// format_time
    FnFormatTime = 1821,
    /// format_timestamp
    FnFormatTimestamp = 1294,
    /// parse_date
    FnParseDate = 1295,
    /// parse_datetime
    FnParseDatetime = 1822,
    /// parse_time
    FnParseTime = 1823,
    /// parse_timestamp
    FnParseTimestamp = 1296,
    /// Math functions
    ///
    /// abs
    FnAbsInt32 = 1300,
    /// abs
    FnAbsInt64 = 1301,
    /// abs
    FnAbsUint32 = 1346,
    /// abs
    FnAbsUint64 = 1347,
    /// abs
    FnAbsFloat = 1302,
    /// abs
    FnAbsDouble = 1303,
    /// abs
    FnAbsNumeric = 1359,
    /// sign
    FnSignInt32 = 1341,
    /// sign
    FnSignInt64 = 1342,
    /// sign
    FnSignUint32 = 1356,
    /// sign
    FnSignUint64 = 1357,
    /// sign
    FnSignFloat = 1343,
    /// sign
    FnSignDouble = 1344,
    /// sign
    FnSignNumeric = 1360,
    /// round(double) -> double
    FnRoundDouble = 1305,
    /// round(float) -> float
    FnRoundFloat = 1306,
    /// round(numeric) -> numeric
    FnRoundNumeric = 1363,
    /// round(double, int64) -> double
    FnRoundWithDigitsDouble = 1307,
    /// round(float, int64) -> float
    FnRoundWithDigitsFloat = 1308,
    /// round(numeric, int64) -> numeric
    FnRoundWithDigitsNumeric = 1364,
    /// trunc(double) -> double
    FnTruncDouble = 1309,
    /// trunc(float) -> float
    FnTruncFloat = 1310,
    /// trunc(numeric) -> numeric
    FnTruncNumeric = 1365,
    /// trunc(double, int64) -> double
    FnTruncWithDigitsDouble = 1311,
    /// trunc(float, int64) -> float
    FnTruncWithDigitsFloat = 1312,
    /// trunc(numeric, int64) -> numeric
    FnTruncWithDigitsNumeric = 1366,
    /// ceil(double) -> double
    FnCeilDouble = 1313,
    /// ceil(float) -> float
    FnCeilFloat = 1314,
    /// ceil(numeric) -> numeric
    FnCeilNumeric = 1368,
    /// floor(double) -> double
    FnFloorDouble = 1315,
    /// floor(float) -> float
    FnFloorFloat = 1316,
    /// floor(numeric) -> numeric
    FnFloorNumeric = 1369,
    /// mod(int64, int64) -> int64
    FnModInt64 = 1349,
    /// mod(uint64, uint64) -> uint64
    FnModUint64 = 1351,
    /// mod(numeric, numeric) -> numeric
    FnModNumeric = 1367,
    /// div(int64, int64) -> int64
    FnDivInt64 = 1353,
    /// div(uint64, uint64) -> uint64
    FnDivUint64 = 1355,
    /// div(numeric, numeric) -> numeric
    FnDivNumeric = 1362,
    /// is_inf
    FnIsInf = 1317,
    /// is_nan
    FnIsNan = 1318,
    /// ieee_divide
    FnIeeeDivideDouble = 1319,
    /// ieee_divide
    FnIeeeDivideFloat = 1320,
    /// safe_divide
    FnSafeDivideDouble = 1358,
    /// safe_divide
    FnSafeDivideNumeric = 1361,
    /// safe_divide
    FnSafeDivideBignumeric = 1388,
    /// safe_add
    FnSafeAddInt64 = 1371,
    /// safe_add
    FnSafeAddUint64 = 1372,
    /// safe_add
    FnSafeAddDouble = 1373,
    /// safe_add
    FnSafeAddNumeric = 1374,
    /// safe_add
    FnSafeAddBignumeric = 1389,
    /// safe_subtract
    FnSafeSubtractInt64 = 1375,
    /// safe_subtract
    FnSafeSubtractUint64 = 1376,
    /// safe_subtract
    FnSafeSubtractDouble = 1377,
    /// safe_subtract
    FnSafeSubtractNumeric = 1378,
    /// safe_subtract
    FnSafeSubtractBignumeric = 1390,
    /// safe_multiply
    FnSafeMultiplyInt64 = 1379,
    /// safe_multiply
    FnSafeMultiplyUint64 = 1380,
    /// safe_multiply
    FnSafeMultiplyDouble = 1381,
    /// safe_multiply
    FnSafeMultiplyNumeric = 1382,
    /// safe_multiply
    FnSafeMultiplyBignumeric = 1391,
    /// safe_negate
    FnSafeUnaryMinusInt32 = 1383,
    /// safe_negate
    FnSafeUnaryMinusInt64 = 1384,
    /// safe_negate
    FnSafeUnaryMinusFloat = 1385,
    /// safe_negate
    FnSafeUnaryMinusDouble = 1386,
    /// safe_negate
    FnSafeUnaryMinusNumeric = 1387,
    /// safe_negate
    FnSafeUnaryMinusBignumeric = 1392,
    /// greatest
    FnGreatest = 1321,
    /// least
    FnLeast = 1322,
    /// sqrt
    FnSqrtDouble = 1323,
    /// pow
    FnPowDouble = 1324,
    /// pow(numeric, numeric) -> numeric
    FnPowNumeric = 1370,
    /// exp
    FnExpDouble = 1325,
    /// ln and log
    FnNaturalLogarithmDouble = 1326,
    /// log10
    FnDecimalLogarithmDouble = 1345,
    /// log
    FnLogarithmDouble = 1327,
    /// cos
    FnCosDouble = 1328,
    /// cosh
    FnCoshDouble = 1329,
    /// acos
    FnAcosDouble = 1330,
    /// acosh
    FnAcoshDouble = 1331,
    /// sin
    FnSinDouble = 1332,
    /// sinh
    FnSinhDouble = 1333,
    /// asin
    FnAsinDouble = 1334,
    /// asinh
    FnAsinhDouble = 1335,
    /// tan
    FnTanDouble = 1336,
    /// tanh
    FnTanhDouble = 1337,
    /// atan
    FnAtanDouble = 1338,
    /// atanh
    FnAtanhDouble = 1339,
    /// atan2
    FnAtan2Double = 1340,
    /// Aggregate functions.
    /// TODO: Add missing type signatures.
    ///
    /// any_value
    FnAnyValue = 1400,
    /// array_agg
    FnArrayAgg = 1401,
    /// array_concat_agg
    FnArrayConcatAgg = 1442,
    /// avg
    FnAvgInt64 = 1402,
    /// avg
    FnAvgUint64 = 1403,
    /// avg
    FnAvgDouble = 1404,
    /// avg
    FnAvgNumeric = 1468,
    /// count
    FnCount = 1405,
    /// max
    FnMax = 1406,
    /// min
    FnMin = 1407,
    /// string_agg(s)
    FnStringAggString = 1408,
    /// string_agg(s, delim_s)
    FnStringAggDelimString = 1409,
    /// string_agg(b)
    FnStringAggBytes = 1413,
    /// string_agg(b, delim_b)
    FnStringAggDelimBytes = 1414,
    /// sum
    FnSumInt64 = 1410,
    /// sum
    FnSumUint64 = 1411,
    /// sum
    FnSumDouble = 1412,
    /// sum
    FnSumNumeric = 1467,
    /// bit_and
    FnBitAndInt32 = 1415,
    /// bit_and
    FnBitAndInt64 = 1416,
    /// bit_and
    FnBitAndUint32 = 1417,
    /// bit_and
    FnBitAndUint64 = 1418,
    /// bit_or
    FnBitOrInt32 = 1419,
    /// bit_or
    FnBitOrInt64 = 1420,
    /// bit_or
    FnBitOrUint32 = 1421,
    /// bit_or
    FnBitOrUint64 = 1422,
    /// bit_xor
    FnBitXorInt32 = 1423,
    /// bit_xor
    FnBitXorInt64 = 1424,
    /// bit_xor
    FnBitXorUint32 = 1425,
    /// bit_xor
    FnBitXorUint64 = 1426,
    /// logical_and
    FnLogicalAnd = 1427,
    /// logical_or
    FnLogicalOr = 1428,
    /// Approximate aggregate functions.
    ///
    /// approx_count_distinct
    FnApproxCountDistinct = 1429,
    /// approx_quantiles
    FnApproxQuantiles = 1430,
    /// approx_top_count
    FnApproxTopCount = 1431,
    /// approx_top_sum
    FnApproxTopSumInt64 = 1432,
    /// approx_top_sum
    FnApproxTopSumUint64 = 1433,
    /// approx_top_sum
    FnApproxTopSumDouble = 1434,
    /// approx_top_sum
    FnApproxTopSumNumeric = 1469,
    /// Approximate count functions that expose the intermediate sketch.
    /// These are all found in the "hll_count.*" namespace.
    ///
    ///
    /// hll_count.merge(bytes)
    FnHllCountMerge = 1444,
    /// hll_count.extract(bytes), scalar
    FnHllCountExtract = 1445,
    /// hll_count.init(int64)
    FnHllCountInitInt64 = 1446,
    /// hll_count.init(uint64)
    FnHllCountInitUint64 = 1447,
    /// hll_count.init(numeric)
    FnHllCountInitNumeric = 1470,
    /// hll_count.init(string)
    FnHllCountInitString = 1448,
    /// hll_count.init(bytes)
    FnHllCountInitBytes = 1449,
    /// hll_count.merge_partial(bytes)
    FnHllCountMergePartial = 1450,
    /// Statistical aggregate functions.
    ///
    /// corr
    FnCorr = 1435,
    /// corr
    FnCorrNumeric = 1471,
    /// covar_pop
    FnCovarPop = 1436,
    /// covar_pop
    FnCovarPopNumeric = 1472,
    /// covar_samp
    FnCovarSamp = 1437,
    /// covar_samp
    FnCovarSampNumeric = 1473,
    /// stddev_pop
    FnStddevPop = 1438,
    /// stddev_pop
    FnStddevPopNumeric = 1474,
    /// stddev_samp
    FnStddevSamp = 1439,
    /// stddev_samp
    FnStddevSampNumeric = 1475,
    /// var_pop
    FnVarPop = 1440,
    /// var_pop
    FnVarPopNumeric = 1476,
    /// var_samp
    FnVarSamp = 1441,
    /// var_samp
    FnVarSampNumeric = 1477,
    /// countif
    FnCountif = 1443,
    /// Approximate quantiles functions that produce or consume intermediate
    /// sketches. All found in the "kll_quantiles.*" namespace.
    ///
    FnKllQuantilesInitInt64 = 1451,
    FnKllQuantilesInitUint64 = 1452,
    FnKllQuantilesInitDouble = 1453,
    FnKllQuantilesMergePartial = 1454,
    FnKllQuantilesMergeInt64 = 1455,
    FnKllQuantilesMergeUint64 = 1456,
    FnKllQuantilesMergeDouble = 1457,
    /// scalar
    FnKllQuantilesExtractInt64 = 1458,
    /// scalar
    FnKllQuantilesExtractUint64 = 1459,
    /// scalar
    FnKllQuantilesExtractDouble = 1460,
    FnKllQuantilesMergePointInt64 = 1461,
    FnKllQuantilesMergePointUint64 = 1462,
    FnKllQuantilesMergePointDouble = 1463,
    /// scalar
    FnKllQuantilesExtractPointInt64 = 1464,
    /// scalar
    FnKllQuantilesExtractPointUint64 = 1465,
    /// scalar
    FnKllQuantilesExtractPointDouble = 1466,
    /// Analytic functions.
    ///
    /// dense_rank
    FnDenseRank = 1500,
    /// rank
    FnRank = 1501,
    /// row_number
    FnRowNumber = 1502,
    /// percent_rank
    FnPercentRank = 1503,
    /// cume_dist
    FnCumeDist = 1504,
    /// ntile
    FnNtile = 1505,
    /// lead
    FnLead = 1506,
    /// lag
    FnLag = 1507,
    /// first_value
    FnFirstValue = 1508,
    /// last_value
    FnLastValue = 1509,
    /// nth_value
    FnNthValue = 1510,
    /// percentile_cont
    FnPercentileCont = 1511,
    /// percentile_disc
    FnPercentileDisc = 1512,
    //
    // Misc functions.
    /// bit_cast_to_int32(int32)
    FnBitCastInt32ToInt32 = 1604,
    /// bit_cast_to_int32(uint32)
    FnBitCastUint32ToInt32 = 1605,
    /// bit_cast_to_int64(int64)
    FnBitCastInt64ToInt64 = 1606,
    /// bit_cast_to_int64(uint64)
    FnBitCastUint64ToInt64 = 1607,
    /// bit_cast_to_uint32(uint32)
    FnBitCastUint32ToUint32 = 1608,
    /// bit_cast_to_uint32(int32)
    FnBitCastInt32ToUint32 = 1609,
    /// bit_cast_to_uint64(uint64)
    FnBitCastUint64ToUint64 = 1610,
    /// bit_cast_to_uint64(int64)
    FnBitCastInt64ToUint64 = 1611,
    /// session_user
    FnSessionUser = 1612,
    /// generate_array(int64)
    FnGenerateArrayInt64 = 1613,
    /// generate_array(uint64)
    FnGenerateArrayUint64 = 1614,
    /// generate_array(numeric)
    FnGenerateArrayNumeric = 1625,
    /// generate_array(double)
    FnGenerateArrayDouble = 1615,
    /// generate_date_array(date)
    FnGenerateDateArray = 1616,
    /// generate_timestamp_array(timestamp)
    FnGenerateTimestampArray = 1617,
    /// array_reverse(array) -> array
    FnArrayReverse = 1621,
    ///  range_bucket(T, array<T>) -> int64
    FnRangeBucket = 1680,
    /// rand() -> double
    FnRand = 1618,
    /// generate_uuid() -> string
    FnGenerateUuid = 1679,
    /// json_extract(string, string)
    FnJsonExtract = 1619,
    /// json_extract_scalar(string, string)
    FnJsonExtractScalar = 1620,
    /// json_extract_array(string[, string]) -> array
    FnJsonExtractArray = 1681,
    /// to_json_string(any[, bool]) -> string
    FnToJsonString = 1622,
    /// json_query(string, string)
    FnJsonQuery = 1623,
    /// json_value(string, string)
    FnJsonValue = 1624,
    /// from_proto(google.protobuf.Timestamp) -> timestamp
    FnFromProtoTimestamp = 1626,
    /// from_proto(google.type.Date) -> date
    FnFromProtoDate = 1627,
    /// from_proto(google.type.TimeOfDay) -> time
    FnFromProtoTimeOfDay = 1628,
    /// from_proto(google.protobuf.DoubleValue) -> double
    FnFromProtoDouble = 1630,
    /// from_proto(google.protobuf.FloatValue) -> float
    FnFromProtoFloat = 1631,
    /// from_proto(google.protobuf.Int64Value) -> int64
    FnFromProtoInt64 = 1632,
    /// from_proto(google.protobuf.UInt64Value) -> uint64
    FnFromProtoUint64 = 1633,
    /// from_proto(google.protobuf.Int32Value) -> int32
    FnFromProtoInt32 = 1634,
    /// from_proto(google.protobuf.UInt32Value) -> uint32
    FnFromProtoUint32 = 1635,
    /// from_proto(google.protobuf.BoolValue) -> bool
    FnFromProtoBool = 1636,
    /// from_proto(google.protobuf.BytesValue) -> bytes
    FnFromProtoBytes = 1637,
    /// from_proto(google.protobuf.StringValue) -> string
    FnFromProtoString = 1638,
    /// The idempotent signatures of from_proto just return the input value
    ///
    /// from_proto(timestamp) -> timestamp
    FnFromProtoIdempotentTimestamp = 1639,
    /// from_proto(date) -> date
    FnFromProtoIdempotentDate = 1640,
    /// from_proto(time) -> time
    FnFromProtoIdempotentTime = 1641,
    /// from_proto(double) -> double
    FnFromProtoIdempotentDouble = 1643,
    /// from_proto(float) -> float
    FnFromProtoIdempotentFloat = 1644,
    /// from_proto(int64) -> int64
    FnFromProtoIdempotentInt64 = 1645,
    /// from_proto(uint64) -> uint64
    FnFromProtoIdempotentUint64 = 1646,
    /// from_proto(int32) -> int32
    FnFromProtoIdempotentInt32 = 1647,
    /// from_proto(uint32) -> uint32
    FnFromProtoIdempotentUint32 = 1648,
    /// from_proto(bool) -> bool
    FnFromProtoIdempotentBool = 1649,
    /// from_proto(bytes) -> bytes
    FnFromProtoIdempotentBytes = 1650,
    /// from_proto(string) -> string
    FnFromProtoIdempotentString = 1651,
    /// to_proto(timestamp) -> google.protobuf.Timestamp
    FnToProtoTimestamp = 1652,
    /// to_proto(date) -> google.type.Date
    FnToProtoDate = 1653,
    /// to_proto(time) -> google.type.TimeOfDay
    FnToProtoTime = 1654,
    /// to_proto(double) -> google.protobuf.DoubleValue
    FnToProtoDouble = 1656,
    /// to_proto(float) -> google.protobuf.FloatValue
    FnToProtoFloat = 1657,
    /// to_proto(int64) -> google.protobuf.Int64Value
    FnToProtoInt64 = 1658,
    /// to_proto(uint64) -> google.protobuf.UInt64Value
    FnToProtoUint64 = 1659,
    /// to_proto(int32) -> google.protobuf.Int32Value
    FnToProtoInt32 = 1660,
    /// to_proto(uint32) -> google.protobuf.UInt32Value
    FnToProtoUint32 = 1661,
    /// to_proto(bool) -> google.protobuf.BoolValue
    FnToProtoBool = 1662,
    /// to_proto(bytes) -> google.protobuf.BytesValue
    FnToProtoBytes = 1663,
    /// to_proto(string) -> google.protobuf.StringValue
    FnToProtoString = 1664,
    /// The idempotent signatures of to_proto just return the input value
    ///
    /// to_proto(google.protobuf.Timestamp) -> google.protobuf.Timestamp
    FnToProtoIdempotentTimestamp = 1665,
    /// to_proto(google.type.Date) -> google.type.Date
    FnToProtoIdempotentDate = 1666,
    /// to_proto(google.type.TimeOfDay) -> google.type.TimeOfDay
    FnToProtoIdempotentTimeOfDay = 1667,
    /// to_proto(google.protobuf.DoubleValue)
    FnToProtoIdempotentDouble = 1669,
    /// -> google.protobuf.DoubleValue
    ///
    /// to_proto(google.protobuf.FloatValue)
    FnToProtoIdempotentFloat = 1670,
    /// -> google.protobuf.FloatValue
    ///
    /// to_proto(google.protobuf.Int64Value)
    FnToProtoIdempotentInt64 = 1671,
    /// -> google.protobuf.Int64Value
    ///
    /// to_proto(google.protobuf.UInt64Value)
    FnToProtoIdempotentUint64 = 1672,
    /// -> google.protobuf.UInt64Value
    ///
    /// to_proto(google.protobuf.Int32Value)
    FnToProtoIdempotentInt32 = 1673,
    /// -> google.protobuf.Int32Value
    ///
    /// to_proto(google.protobuf.UInt32Value)
    FnToProtoIdempotentUint32 = 1674,
    /// -> google.protobuf.UInt32Value
    ///
    /// to_proto(google.protobuf.BoolValue) -> google.protobuf.BoolValue
    FnToProtoIdempotentBool = 1675,
    /// to_proto(google.protobuf.BytesValue)
    FnToProtoIdempotentBytes = 1676,
    /// -> google.protobuf.BytesValue
    ///
    /// to_proto(google.protobuf.StringValue)
    FnToProtoIdempotentString = 1677,
    // -> google.protobuf.StringValue
    /// proto_default_if_null(<non-message optional field access>)
    FnProtoDefaultIfNull = 1678,
    /// Net functions. These are all found in the "net.*" namespace.
    FnNetFormatIp = 1700,
    FnNetParseIp = 1701,
    FnNetFormatPackedIp = 1702,
    FnNetParsePackedIp = 1703,
    FnNetIpInNet = 1704,
    FnNetMakeNet = 1705,
    /// net.host(string)
    FnNetHost = 1706,
    /// net.reg_domain(string)
    FnNetRegDomain = 1707,
    /// net.public_suffix(string)
    FnNetPublicSuffix = 1708,
    /// net.ip_from_string(string)
    FnNetIpFromString = 1709,
    /// net.safe_ip_from_string(string)
    FnNetSafeIpFromString = 1710,
    /// net.ip_to_string(bytes)
    FnNetIpToString = 1711,
    /// net.ip_net_mask(int64, int64)
    FnNetIpNetMask = 1712,
    /// net.ip_net_mask(bytes, int64)
    FnNetIpTrunc = 1713,
    /// net.ipv4_from_int64(int64)
    FnNetIpv4FromInt64 = 1714,
    /// net.ipv4_to_int64(bytes)
    FnNetIpv4ToInt64 = 1715,
    /// Hashing functions.
    ///
    /// md5(bytes)
    FnMd5Bytes = 1900,
    /// md5(string)
    FnMd5String = 1901,
    /// sha1(bytes)
    FnSha1Bytes = 1902,
    /// sha1(string)
    FnSha1String = 1903,
    /// sha256(bytes)
    FnSha256Bytes = 1904,
    /// sha256(string)
    FnSha256String = 1905,
    /// sha512(bytes)
    FnSha512Bytes = 1906,
    /// sha512(string)
    FnSha512String = 1907,
    /// Fingerprinting functions
    ///
    /// farm_fingerprint(bytes) -> int64
    FnFarmFingerprintBytes = 1908,
    /// farm_fingerprint(string) -> int64
    FnFarmFingerprintString = 1909,
    /// Keyset management, encryption, and decryption functions
    /// ((broken link)). Requires that FEATURE_ENCRYPTION is enabled.
    ///
    /// keys.new_keyset(string)
    FnKeysNewKeyset = 1910,
    /// keys.add_key_from_raw_bytes(bytes, string, bytes)
    FnKeysAddKeyFromRawBytes = 1911,
    /// keys.rotate_keyset(bytes, string)
    FnKeysRotateKeyset = 1912,
    /// keys.keyset_length(bytes)
    FnKeysKeysetLength = 1913,
    /// keys.keyset_to_json(bytes)
    FnKeysKeysetToJson = 1914,
    /// keys.keyset_from_json(string)
    FnKeysKeysetFromJson = 1915,
    /// aead.encrypt(bytes, string, string)
    FnAeadEncryptString = 1916,
    /// aead.encrypt(bytes, bytes, bytes)
    FnAeadEncryptBytes = 1917,
    /// aead.decrypt_string(bytes, bytes, string)
    FnAeadDecryptString = 1918,
    /// aead.decrypt_bytes(bytes, bytes, bytes)
    FnAeadDecryptBytes = 1919,
    /// kms.encrypt(string, string)
    FnKmsEncryptString = 1920,
    /// kms.encrypt(string, bytes)
    FnKmsEncryptBytes = 1921,
    /// kms.decrypt_string(string, bytes)
    FnKmsDecryptString = 1922,
    /// kms.decrypt_bytes(string, bytes)
    FnKmsDecryptBytes = 1923,
    /// ST_ family of functions (Geography related - (broken link))
    /// Constructors
    FnStGeogPoint = 2000,
    FnStMakeLine = 2001,
    FnStMakeLineArray = 2002,
    FnStMakePolygon = 2003,
    FnStMakePolygonOriented = 2004,
    /// Transformations
    FnStIntersection = 2007,
    FnStUnion = 2008,
    FnStUnionArray = 2009,
    FnStDifference = 2010,
    FnStUnaryUnion = 2011,
    FnStCentroid = 2012,
    FnStBuffer = 2013,
    FnStBufferWithTolerance = 2014,
    FnStSimplify = 2015,
    FnStSnapToGrid = 2016,
    FnStClosestPoint = 2017,
    FnStBoundary = 2018,
    /// Predicates
    FnStEquals = 2020,
    FnStIntersects = 2021,
    FnStContains = 2022,
    FnStCovers = 2023,
    FnStDisjoint = 2024,
    FnStIntersectsBox = 2025,
    FnStDwithin = 2026,
    FnStWithin = 2027,
    FnStCoveredby = 2028,
    FnStTouches = 2029,
    /// Accessors
    FnStIsEmpty = 2030,
    FnStIsCollection = 2031,
    FnStDimension = 2032,
    FnStNumPoints = 2033,
    /// Measures
    FnStLength = 2040,
    FnStPerimeter = 2041,
    FnStArea = 2042,
    FnStDistance = 2043,
    FnStMaxDistance = 2044,
    /// Parsers/formatters
    FnStGeogFromText = 2050,
    FnStGeogFromKml = 2051,
    FnStGeogFromGeoJson = 2052,
    FnStGeogFromWkb = 2056,
    FnStAsText = 2053,
    FnStAsKml = 2054,
    FnStAsGeoJson = 2055,
    FnStAsBinary = 2057,
    FnStGeohash = 2058,
    FnStGeogPointFromGeohash = 2059,
    /// Aggregate functions
    FnStUnionAgg = 2061,
    FnStAccum = 2062,
    FnStCentroidAgg = 2063,
    /// Other geography functions
    FnStX = 2070,
    FnStY = 2071,
}
/// Specify what built-in functions should be load.
/// Used for getting built-in functions through local server.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ZetaSqlBuiltinFunctionOptionsProto {
    #[prost(message, optional, tag = "1")]
    pub language_options: ::std::option::Option<LanguageOptionsProto>,
    #[prost(
        enumeration = "FunctionSignatureId",
        repeated,
        packed = "false",
        tag = "2"
    )]
    pub include_function_ids: ::std::vec::Vec<i32>,
    #[prost(
        enumeration = "FunctionSignatureId",
        repeated,
        packed = "false",
        tag = "3"
    )]
    pub exclude_function_ids: ::std::vec::Vec<i32>,
}
/// Serialized form of LanguageOptions.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LanguageOptionsProto {
    #[prost(enumeration = "NameResolutionMode", optional, tag = "2")]
    pub name_resolution_mode: ::std::option::Option<i32>,
    #[prost(enumeration = "ProductMode", optional, tag = "3")]
    pub product_mode: ::std::option::Option<i32>,
    #[prost(bool, optional, tag = "4")]
    pub error_on_deprecated_syntax: ::std::option::Option<bool>,
    #[prost(enumeration = "LanguageFeature", repeated, packed = "false", tag = "5")]
    pub enabled_language_features: ::std::vec::Vec<i32>,
    #[prost(
        enumeration = "ResolvedNodeKind",
        repeated,
        packed = "false",
        tag = "6"
    )]
    pub supported_statement_kinds: ::std::vec::Vec<i32>,
}
/// Serialized form of AllowedHintsAndOptions.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AllowedHintsAndOptionsProto {
    #[prost(bool, optional, tag = "1")]
    pub disallow_unknown_options: ::std::option::Option<bool>,
    #[prost(string, repeated, tag = "2")]
    pub disallow_unknown_hints_with_qualifier: ::std::vec::Vec<std::string::String>,
    #[prost(message, repeated, tag = "3")]
    pub hint: ::std::vec::Vec<allowed_hints_and_options_proto::HintProto>,
    #[prost(message, repeated, tag = "4")]
    pub option: ::std::vec::Vec<allowed_hints_and_options_proto::OptionProto>,
}
pub mod allowed_hints_and_options_proto {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct HintProto {
        #[prost(string, optional, tag = "1")]
        pub qualifier: ::std::option::Option<std::string::String>,
        #[prost(string, optional, tag = "2")]
        pub name: ::std::option::Option<std::string::String>,
        #[prost(message, optional, tag = "3")]
        pub r#type: ::std::option::Option<super::TypeProto>,
        #[prost(bool, optional, tag = "4")]
        pub allow_unqualified: ::std::option::Option<bool>,
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct OptionProto {
        #[prost(string, optional, tag = "1")]
        pub name: ::std::option::Option<std::string::String>,
        #[prost(message, optional, tag = "2")]
        pub r#type: ::std::option::Option<super::TypeProto>,
    }
}
/// Serialized form of AnalyzerOptions.
/// next id: 21
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AnalyzerOptionsProto {
    #[prost(message, optional, tag = "1")]
    pub language_options: ::std::option::Option<LanguageOptionsProto>,
    #[prost(message, repeated, tag = "2")]
    pub query_parameters: ::std::vec::Vec<analyzer_options_proto::QueryParameterProto>,
    #[prost(message, repeated, tag = "12")]
    pub positional_query_parameters: ::std::vec::Vec<TypeProto>,
    #[prost(message, repeated, tag = "3")]
    pub expression_columns: ::std::vec::Vec<analyzer_options_proto::QueryParameterProto>,
    #[prost(message, optional, tag = "4")]
    pub in_scope_expression_column:
        ::std::option::Option<analyzer_options_proto::QueryParameterProto>,
    #[prost(message, repeated, tag = "15")]
    pub ddl_pseudo_columns: ::std::vec::Vec<analyzer_options_proto::QueryParameterProto>,
    /// base::SequenceNumber does not support getting and setting the current
    /// value, so it is not serializable. Reserving tag number 5 in case we want
    /// to support it in some other way later.
    #[prost(enumeration = "ErrorMessageMode", optional, tag = "6")]
    pub error_message_mode: ::std::option::Option<i32>,
    /// In the form that can be parsed by C++ absl::LoadTimeZone().
    #[prost(string, optional, tag = "7")]
    pub default_timezone: ::std::option::Option<std::string::String>,
    #[prost(bool, optional, tag = "8")]
    pub record_parse_locations: ::std::option::Option<bool>,
    #[prost(bool, optional, tag = "20")]
    pub create_new_column_for_each_projected_output: ::std::option::Option<bool>,
    #[prost(bool, optional, tag = "9")]
    pub prune_unused_columns: ::std::option::Option<bool>,
    #[prost(bool, optional, tag = "10")]
    pub allow_undeclared_parameters: ::std::option::Option<bool>,
    #[prost(enumeration = "ParameterMode", optional, tag = "13")]
    pub parameter_mode: ::std::option::Option<i32>,
    #[prost(message, optional, tag = "11")]
    pub allowed_hints_and_options: ::std::option::Option<AllowedHintsAndOptionsProto>,
    #[prost(enumeration = "StatementContext", optional, tag = "14")]
    pub statement_context: ::std::option::Option<i32>,
    #[prost(bool, optional, tag = "17")]
    pub preserve_column_aliases: ::std::option::Option<bool>,
    #[prost(message, repeated, tag = "18")]
    pub system_variables: ::std::vec::Vec<analyzer_options_proto::SystemVariableProto>,
    #[prost(message, repeated, tag = "19")]
    pub target_column_types: ::std::vec::Vec<TypeProto>,
}
pub mod analyzer_options_proto {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct QueryParameterProto {
        #[prost(string, optional, tag = "1")]
        pub name: ::std::option::Option<std::string::String>,
        #[prost(message, optional, tag = "2")]
        pub r#type: ::std::option::Option<super::TypeProto>,
    }
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct SystemVariableProto {
        #[prost(string, repeated, tag = "1")]
        pub name_path: ::std::vec::Vec<std::string::String>,
        #[prost(message, optional, tag = "2")]
        pub r#type: ::std::option::Option<super::TypeProto>,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SimpleConstantProto {
    #[prost(string, repeated, tag = "1")]
    pub name_path: ::std::vec::Vec<std::string::String>,
    #[prost(message, optional, tag = "2")]
    pub r#type: ::std::option::Option<TypeProto>,
    #[prost(message, optional, tag = "3")]
    pub value: ::std::option::Option<ValueProto>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SimpleTableProto {
    #[prost(string, optional, tag = "1")]
    pub name: ::std::option::Option<std::string::String>,
    #[prost(int64, optional, tag = "2")]
    pub serialization_id: ::std::option::Option<i64>,
    #[prost(bool, optional, tag = "3")]
    pub is_value_table: ::std::option::Option<bool>,
    #[prost(message, repeated, tag = "4")]
    pub column: ::std::vec::Vec<SimpleColumnProto>,
    #[prost(int32, repeated, packed = "false", tag = "9")]
    pub primary_key_column_index: ::std::vec::Vec<i32>,
    /// Alias name of the table when it is added to the parent catalog.  This is
    /// only set when the Table is added to the Catalog using a different name
    /// than the Table's name.  This name is not part of the SimpleTable, but
    /// will be used as the Table's name in the Catalog.
    #[prost(string, optional, tag = "5")]
    pub name_in_catalog: ::std::option::Option<std::string::String>,
    #[prost(bool, optional, tag = "6")]
    pub allow_anonymous_column_name: ::std::option::Option<bool>,
    #[prost(bool, optional, tag = "7")]
    pub allow_duplicate_column_names: ::std::option::Option<bool>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SimpleColumnProto {
    #[prost(string, optional, tag = "1")]
    pub name: ::std::option::Option<std::string::String>,
    #[prost(message, optional, tag = "2")]
    pub r#type: ::std::option::Option<TypeProto>,
    #[prost(bool, optional, tag = "3")]
    pub is_pseudo_column: ::std::option::Option<bool>,
    #[prost(bool, optional, tag = "4", default = "true")]
    pub is_writable_column: ::std::option::Option<bool>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SimpleCatalogProto {
    #[prost(string, optional, tag = "1")]
    pub name: ::std::option::Option<std::string::String>,
    #[prost(message, repeated, tag = "2")]
    pub table: ::std::vec::Vec<SimpleTableProto>,
    #[prost(message, repeated, tag = "3")]
    pub named_type: ::std::vec::Vec<simple_catalog_proto::NamedTypeProto>,
    #[prost(message, repeated, tag = "4")]
    pub catalog: ::std::vec::Vec<SimpleCatalogProto>,
    /// Specify built-in functions to load.
    #[prost(message, optional, tag = "5")]
    pub builtin_function_options: ::std::option::Option<ZetaSqlBuiltinFunctionOptionsProto>,
    #[prost(message, repeated, tag = "6")]
    pub custom_function: ::std::vec::Vec<FunctionProto>,
    #[prost(message, repeated, tag = "9")]
    pub custom_tvf: ::std::vec::Vec<TableValuedFunctionProto>,
    /// The index of the FileDescriptorSet in the top-level request proto.
    /// If set, SimpleCatalog::SetDescriptorPool will be called with the
    /// DescriptorPool deserialized from the referred FileDescriptorSet.
    #[prost(int32, optional, tag = "7", default = "-1")]
    pub file_descriptor_set_index: ::std::option::Option<i32>,
    #[prost(message, repeated, tag = "8")]
    pub procedure: ::std::vec::Vec<ProcedureProto>,
    #[prost(message, repeated, tag = "10")]
    pub constant: ::std::vec::Vec<SimpleConstantProto>,
}
pub mod simple_catalog_proto {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct NamedTypeProto {
        #[prost(string, optional, tag = "1")]
        pub name: ::std::option::Option<std::string::String>,
        #[prost(message, optional, tag = "2")]
        pub r#type: ::std::option::Option<super::TypeProto>,
    }
}
/// Serialized form of ParseLocationPoint, only to be used inside the
/// ZetaSQL library to attach an error location in internal form to a
/// absl::Status. This should never leave the library: externally we should
/// attach an ErrorLocation proto.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct InternalErrorLocation {
    #[prost(int32, optional, tag = "3")]
    pub byte_offset: ::std::option::Option<i32>,
    #[prost(string, optional, tag = "4")]
    pub filename: ::std::option::Option<std::string::String>,
    /// An optional list of error source information for the related Status.
    /// The last element in this list is the immediate error cause, with
    /// the previous element being its cause, etc.
    #[prost(message, repeated, tag = "5")]
    pub error_source: ::std::vec::Vec<ErrorSource>,
}
/// This is a hack so that the generated module has at least one Descriptor in
/// it, so code that needs to reference this module can use that Descriptor to
/// get a reference to the file's FileDescriptor.
/// Otherwise, this should not be used.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WireFormatAnnotationEmptyMessage {}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum TableType {
    /// No TableType annotation.
    /// This is meant as a no-annotation marker in code and should not actually
    /// be written as an annotation in .proto files.
    DefaultTableType = 0,
    /// A normal SQL table, where each row has columns, and each column
    /// has a name and a type.
    SqlTable = 1,
    /// A value table, where each row has a row type, and the row is just a
    /// value of that type, and there is no column name.
    /// See (broken link).
    ValueTable = 2,
}
