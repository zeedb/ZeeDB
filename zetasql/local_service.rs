#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PrepareRequest {
    #[prost(string, optional, tag = "1")]
    pub sql: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(message, optional, tag = "2")]
    pub options: ::core::option::Option<super::AnalyzerOptionsProto>,
    /// Serialized descriptor pools of all types in the request.
    #[prost(message, repeated, tag = "3")]
    pub file_descriptor_set: ::prost::alloc::vec::Vec<::prost_types::FileDescriptorSet>,
    #[prost(message, optional, tag = "4")]
    pub simple_catalog: ::core::option::Option<super::SimpleCatalogProto>,
    #[prost(int64, optional, tag = "5")]
    pub registered_catalog_id: ::core::option::Option<i64>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PreparedState {
    #[prost(int64, optional, tag = "1")]
    pub prepared_expression_id: ::core::option::Option<i64>,
    /// No file_descriptor_set returned. Use the same descriptor pools as sent in
    /// the request deserialize the type.
    #[prost(message, optional, tag = "2")]
    pub output_type: ::core::option::Option<super::TypeProto>,
    #[prost(string, repeated, tag = "3")]
    pub referenced_columns: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, repeated, tag = "4")]
    pub referenced_parameters: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(int64, optional, tag = "5")]
    pub positional_parameter_count: ::core::option::Option<i64>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PrepareResponse {
    /// TODO: Remove these fields
    #[prost(int64, optional, tag = "1")]
    pub prepared_expression_id: ::core::option::Option<i64>,
    #[prost(message, optional, tag = "2")]
    pub output_type: ::core::option::Option<super::TypeProto>,
    /// Never add fields to this proto, add them to PreparedState instead.
    #[prost(message, optional, tag = "3")]
    pub prepared: ::core::option::Option<PreparedState>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EvaluateRequest {
    #[prost(string, optional, tag = "1")]
    pub sql: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(message, repeated, tag = "2")]
    pub columns: ::prost::alloc::vec::Vec<evaluate_request::Parameter>,
    #[prost(message, repeated, tag = "3")]
    pub params: ::prost::alloc::vec::Vec<evaluate_request::Parameter>,
    /// Serialized descriptor pools of all types in the request.
    #[prost(message, repeated, tag = "4")]
    pub file_descriptor_set: ::prost::alloc::vec::Vec<::prost_types::FileDescriptorSet>,
    /// Set if the expression is already prepared, in which case sql and
    /// file_descriptor_set will be ignored.
    #[prost(int64, optional, tag = "5")]
    pub prepared_expression_id: ::core::option::Option<i64>,
    #[prost(message, optional, tag = "6")]
    pub options: ::core::option::Option<super::AnalyzerOptionsProto>,
}
/// Nested message and enum types in `EvaluateRequest`.
pub mod evaluate_request {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Parameter {
        #[prost(string, optional, tag = "1")]
        pub name: ::core::option::Option<::prost::alloc::string::String>,
        #[prost(message, optional, tag = "2")]
        pub value: ::core::option::Option<super::super::ValueProto>,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EvaluateResponse {
    #[prost(message, optional, tag = "1")]
    pub value: ::core::option::Option<super::ValueProto>,
    /// TODO: Remove these fields
    #[prost(message, optional, tag = "2")]
    pub r#type: ::core::option::Option<super::TypeProto>,
    #[prost(int64, optional, tag = "3")]
    pub prepared_expression_id: ::core::option::Option<i64>,
    #[prost(message, optional, tag = "4")]
    pub prepared: ::core::option::Option<PreparedState>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EvaluateRequestBatch {
    #[prost(message, repeated, tag = "1")]
    pub request: ::prost::alloc::vec::Vec<EvaluateRequest>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EvaluateResponseBatch {
    #[prost(message, repeated, tag = "1")]
    pub response: ::prost::alloc::vec::Vec<EvaluateResponse>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UnprepareRequest {
    #[prost(int64, optional, tag = "1")]
    pub prepared_expression_id: ::core::option::Option<i64>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableFromProtoRequest {
    #[prost(message, optional, tag = "1")]
    pub proto: ::core::option::Option<super::ProtoTypeProto>,
    #[prost(message, optional, tag = "2")]
    pub file_descriptor_set: ::core::option::Option<::prost_types::FileDescriptorSet>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RegisteredParseResumeLocationProto {
    #[prost(int64, optional, tag = "1")]
    pub registered_id: ::core::option::Option<i64>,
    #[prost(int32, optional, tag = "2")]
    pub byte_position: ::core::option::Option<i32>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AnalyzeRequest {
    #[prost(message, optional, tag = "1")]
    pub options: ::core::option::Option<super::AnalyzerOptionsProto>,
    #[prost(message, optional, tag = "2")]
    pub simple_catalog: ::core::option::Option<super::SimpleCatalogProto>,
    #[prost(message, repeated, tag = "3")]
    pub file_descriptor_set: ::prost::alloc::vec::Vec<::prost_types::FileDescriptorSet>,
    /// Set if using a registered catalog, in which case simple_catalog and
    /// file_descriptor_set will be ignored.
    #[prost(int64, optional, tag = "4")]
    pub registered_catalog_id: ::core::option::Option<i64>,
    #[prost(oneof = "analyze_request::Target", tags = "5, 6, 7, 8")]
    pub target: ::core::option::Option<analyze_request::Target>,
}
/// Nested message and enum types in `AnalyzeRequest`.
pub mod analyze_request {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Target {
        /// Single statement.
        #[prost(string, tag = "5")]
        SqlStatement(::prost::alloc::string::String),
        /// Multiple statement.
        #[prost(message, tag = "6")]
        ParseResumeLocation(super::super::ParseResumeLocationProto),
        /// Set if using a registered parse resume location.
        #[prost(message, tag = "7")]
        RegisteredParseResumeLocation(super::RegisteredParseResumeLocationProto),
        /// Expression.
        #[prost(string, tag = "8")]
        SqlExpression(::prost::alloc::string::String),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AnalyzeResponse {
    /// Set only if the request had parse_resume_location.
    #[prost(int32, optional, tag = "2")]
    pub resume_byte_position: ::core::option::Option<i32>,
    #[prost(oneof = "analyze_response::Result", tags = "1, 3")]
    pub result: ::core::option::Option<analyze_response::Result>,
}
/// Nested message and enum types in `AnalyzeResponse`.
pub mod analyze_response {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Result {
        #[prost(message, tag = "1")]
        ResolvedStatement(super::super::AnyResolvedStatementProto),
        #[prost(message, tag = "3")]
        ResolvedExpression(super::super::AnyResolvedExprProto),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BuildSqlRequest {
    #[prost(message, optional, tag = "1")]
    pub simple_catalog: ::core::option::Option<super::SimpleCatalogProto>,
    #[prost(message, repeated, tag = "2")]
    pub file_descriptor_set: ::prost::alloc::vec::Vec<::prost_types::FileDescriptorSet>,
    /// Set if using a registered catalog, in which case simple_catalog and
    /// file_descriptor_set will be ignored.
    #[prost(int64, optional, tag = "3")]
    pub registered_catalog_id: ::core::option::Option<i64>,
    #[prost(oneof = "build_sql_request::Target", tags = "4, 5")]
    pub target: ::core::option::Option<build_sql_request::Target>,
}
/// Nested message and enum types in `BuildSqlRequest`.
pub mod build_sql_request {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Target {
        #[prost(message, tag = "4")]
        ResolvedStatement(super::super::AnyResolvedStatementProto),
        #[prost(message, tag = "5")]
        ResolvedExpression(super::super::AnyResolvedExprProto),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BuildSqlResponse {
    #[prost(string, optional, tag = "1")]
    pub sql: ::core::option::Option<::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExtractTableNamesFromStatementRequest {
    #[prost(string, optional, tag = "1")]
    pub sql_statement: ::core::option::Option<::prost::alloc::string::String>,
    /// If language options are not provided, the parser would use a default
    /// LanguageOptions object. See ExtractTableNamesFromNextStatementRequest
    /// for further details.
    #[prost(message, optional, tag = "2")]
    pub options: ::core::option::Option<super::LanguageOptionsProto>,
    /// sql_statement is interpreted as a script rather than a single statement.
    #[prost(bool, optional, tag = "3")]
    pub allow_script: ::core::option::Option<bool>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExtractTableNamesFromStatementResponse {
    #[prost(message, repeated, tag = "1")]
    pub table_name:
        ::prost::alloc::vec::Vec<extract_table_names_from_statement_response::TableName>,
}
/// Nested message and enum types in `ExtractTableNamesFromStatementResponse`.
pub mod extract_table_names_from_statement_response {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct TableName {
        #[prost(string, repeated, tag = "1")]
        pub table_name_segment: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExtractTableNamesFromNextStatementRequest {
    #[prost(message, required, tag = "1")]
    pub parse_resume_location: super::ParseResumeLocationProto,
    /// If language options are not provided, the parser would use a default
    /// LanguageOptions object. Refer to the LanguageOptions class definition
    /// for the exact implementation details.
    ///
    /// Note that There may be untrivial differences between providing an empty
    /// options/ field and not providing one which depending on the
    /// LanguageOptions implementation details.
    ///
    /// The current implementation of the LanguageOptions class has default value
    /// of supported_statement_kinds set to {RESOLVED_QUERY_STMT}. This means that
    /// if you don't provide any options, then you're limited to this one
    /// kind of statement. If you provide an empty options proto, you're
    /// explicitly setting supported_statement_kinds to an empty set,
    /// allowing all types of statements.
    ///
    /// See LanguageOptions::SupportsStatementKind and
    /// LanguageOptions::supported_statement_kinds_ definitions for the source of
    /// truth on this example.
    #[prost(message, optional, tag = "2")]
    pub options: ::core::option::Option<super::LanguageOptionsProto>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExtractTableNamesFromNextStatementResponse {
    #[prost(message, repeated, tag = "1")]
    pub table_name:
        ::prost::alloc::vec::Vec<extract_table_names_from_next_statement_response::TableName>,
    #[prost(int32, optional, tag = "2")]
    pub resume_byte_position: ::core::option::Option<i32>,
}
/// Nested message and enum types in `ExtractTableNamesFromNextStatementResponse`.
pub mod extract_table_names_from_next_statement_response {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct TableName {
        #[prost(string, repeated, tag = "1")]
        pub table_name_segment: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FormatSqlRequest {
    #[prost(string, optional, tag = "1")]
    pub sql: ::core::option::Option<::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FormatSqlResponse {
    #[prost(string, optional, tag = "1")]
    pub sql: ::core::option::Option<::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RegisterCatalogRequest {
    #[prost(message, optional, tag = "1")]
    pub simple_catalog: ::core::option::Option<super::SimpleCatalogProto>,
    #[prost(message, repeated, tag = "2")]
    pub file_descriptor_set: ::prost::alloc::vec::Vec<::prost_types::FileDescriptorSet>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RegisterResponse {
    #[prost(int64, optional, tag = "1")]
    pub registered_id: ::core::option::Option<i64>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UnregisterRequest {
    #[prost(int64, optional, tag = "1")]
    pub registered_id: ::core::option::Option<i64>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetBuiltinFunctionsResponse {
    /// No file_descriptor_set returned. For now, only Datetime functions
    /// have arguments of enum type which need to be added manually when
    /// deserializing.
    #[prost(message, repeated, tag = "1")]
    pub function: ::prost::alloc::vec::Vec<super::FunctionProto>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AddSimpleTableRequest {
    #[prost(int64, optional, tag = "1")]
    pub registered_catalog_id: ::core::option::Option<i64>,
    #[prost(message, optional, tag = "2")]
    pub table: ::core::option::Option<super::SimpleTableProto>,
    #[prost(message, repeated, tag = "3")]
    pub file_descriptor_set: ::prost::alloc::vec::Vec<::prost_types::FileDescriptorSet>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LanguageOptionsRequest {
    #[prost(bool, optional, tag = "1")]
    pub maximum_features: ::core::option::Option<bool>,
    #[prost(enumeration = "super::LanguageVersion", optional, tag = "2")]
    pub language_version: ::core::option::Option<i32>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AnalyzerOptionsRequest {}
const METHOD_ZETA_SQL_LOCAL_SERVICE_PREPARE: ::grpcio::Method<PrepareRequest, PrepareResponse> =
    ::grpcio::Method {
        ty: ::grpcio::MethodType::Unary,
        name: "/zetasql.local_service.ZetaSqlLocalService/Prepare",
        req_mar: ::grpcio::Marshaller {
            ser: ::grpcio::pr_ser,
            de: ::grpcio::pr_de,
        },
        resp_mar: ::grpcio::Marshaller {
            ser: ::grpcio::pr_ser,
            de: ::grpcio::pr_de,
        },
    };
const METHOD_ZETA_SQL_LOCAL_SERVICE_EVALUATE: ::grpcio::Method<EvaluateRequest, EvaluateResponse> =
    ::grpcio::Method {
        ty: ::grpcio::MethodType::Unary,
        name: "/zetasql.local_service.ZetaSqlLocalService/Evaluate",
        req_mar: ::grpcio::Marshaller {
            ser: ::grpcio::pr_ser,
            de: ::grpcio::pr_de,
        },
        resp_mar: ::grpcio::Marshaller {
            ser: ::grpcio::pr_ser,
            de: ::grpcio::pr_de,
        },
    };
const METHOD_ZETA_SQL_LOCAL_SERVICE_EVALUATE_STREAM: ::grpcio::Method<
    EvaluateRequestBatch,
    EvaluateResponseBatch,
> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Duplex,
    name: "/zetasql.local_service.ZetaSqlLocalService/EvaluateStream",
    req_mar: ::grpcio::Marshaller {
        ser: ::grpcio::pr_ser,
        de: ::grpcio::pr_de,
    },
    resp_mar: ::grpcio::Marshaller {
        ser: ::grpcio::pr_ser,
        de: ::grpcio::pr_de,
    },
};
const METHOD_ZETA_SQL_LOCAL_SERVICE_UNPREPARE: ::grpcio::Method<UnprepareRequest, ()> =
    ::grpcio::Method {
        ty: ::grpcio::MethodType::Unary,
        name: "/zetasql.local_service.ZetaSqlLocalService/Unprepare",
        req_mar: ::grpcio::Marshaller {
            ser: ::grpcio::pr_ser,
            de: ::grpcio::pr_de,
        },
        resp_mar: ::grpcio::Marshaller {
            ser: ::grpcio::pr_ser,
            de: ::grpcio::pr_de,
        },
    };
const METHOD_ZETA_SQL_LOCAL_SERVICE_GET_TABLE_FROM_PROTO: ::grpcio::Method<
    TableFromProtoRequest,
    super::SimpleTableProto,
> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/zetasql.local_service.ZetaSqlLocalService/GetTableFromProto",
    req_mar: ::grpcio::Marshaller {
        ser: ::grpcio::pr_ser,
        de: ::grpcio::pr_de,
    },
    resp_mar: ::grpcio::Marshaller {
        ser: ::grpcio::pr_ser,
        de: ::grpcio::pr_de,
    },
};
const METHOD_ZETA_SQL_LOCAL_SERVICE_REGISTER_CATALOG: ::grpcio::Method<
    RegisterCatalogRequest,
    RegisterResponse,
> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/zetasql.local_service.ZetaSqlLocalService/RegisterCatalog",
    req_mar: ::grpcio::Marshaller {
        ser: ::grpcio::pr_ser,
        de: ::grpcio::pr_de,
    },
    resp_mar: ::grpcio::Marshaller {
        ser: ::grpcio::pr_ser,
        de: ::grpcio::pr_de,
    },
};
const METHOD_ZETA_SQL_LOCAL_SERVICE_REGISTER_PARSE_RESUME_LOCATION: ::grpcio::Method<
    super::ParseResumeLocationProto,
    RegisterResponse,
> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/zetasql.local_service.ZetaSqlLocalService/RegisterParseResumeLocation",
    req_mar: ::grpcio::Marshaller {
        ser: ::grpcio::pr_ser,
        de: ::grpcio::pr_de,
    },
    resp_mar: ::grpcio::Marshaller {
        ser: ::grpcio::pr_ser,
        de: ::grpcio::pr_de,
    },
};
const METHOD_ZETA_SQL_LOCAL_SERVICE_ANALYZE: ::grpcio::Method<AnalyzeRequest, AnalyzeResponse> =
    ::grpcio::Method {
        ty: ::grpcio::MethodType::Unary,
        name: "/zetasql.local_service.ZetaSqlLocalService/Analyze",
        req_mar: ::grpcio::Marshaller {
            ser: ::grpcio::pr_ser,
            de: ::grpcio::pr_de,
        },
        resp_mar: ::grpcio::Marshaller {
            ser: ::grpcio::pr_ser,
            de: ::grpcio::pr_de,
        },
    };
const METHOD_ZETA_SQL_LOCAL_SERVICE_BUILD_SQL: ::grpcio::Method<BuildSqlRequest, BuildSqlResponse> =
    ::grpcio::Method {
        ty: ::grpcio::MethodType::Unary,
        name: "/zetasql.local_service.ZetaSqlLocalService/BuildSql",
        req_mar: ::grpcio::Marshaller {
            ser: ::grpcio::pr_ser,
            de: ::grpcio::pr_de,
        },
        resp_mar: ::grpcio::Marshaller {
            ser: ::grpcio::pr_ser,
            de: ::grpcio::pr_de,
        },
    };
const METHOD_ZETA_SQL_LOCAL_SERVICE_EXTRACT_TABLE_NAMES_FROM_STATEMENT: ::grpcio::Method<
    ExtractTableNamesFromStatementRequest,
    ExtractTableNamesFromStatementResponse,
> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/zetasql.local_service.ZetaSqlLocalService/ExtractTableNamesFromStatement",
    req_mar: ::grpcio::Marshaller {
        ser: ::grpcio::pr_ser,
        de: ::grpcio::pr_de,
    },
    resp_mar: ::grpcio::Marshaller {
        ser: ::grpcio::pr_ser,
        de: ::grpcio::pr_de,
    },
};
const METHOD_ZETA_SQL_LOCAL_SERVICE_EXTRACT_TABLE_NAMES_FROM_NEXT_STATEMENT: ::grpcio::Method<
    ExtractTableNamesFromNextStatementRequest,
    ExtractTableNamesFromNextStatementResponse,
> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/zetasql.local_service.ZetaSqlLocalService/ExtractTableNamesFromNextStatement",
    req_mar: ::grpcio::Marshaller {
        ser: ::grpcio::pr_ser,
        de: ::grpcio::pr_de,
    },
    resp_mar: ::grpcio::Marshaller {
        ser: ::grpcio::pr_ser,
        de: ::grpcio::pr_de,
    },
};
const METHOD_ZETA_SQL_LOCAL_SERVICE_FORMAT_SQL: ::grpcio::Method<
    FormatSqlRequest,
    FormatSqlResponse,
> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/zetasql.local_service.ZetaSqlLocalService/FormatSql",
    req_mar: ::grpcio::Marshaller {
        ser: ::grpcio::pr_ser,
        de: ::grpcio::pr_de,
    },
    resp_mar: ::grpcio::Marshaller {
        ser: ::grpcio::pr_ser,
        de: ::grpcio::pr_de,
    },
};
const METHOD_ZETA_SQL_LOCAL_SERVICE_LENIENT_FORMAT_SQL: ::grpcio::Method<
    FormatSqlRequest,
    FormatSqlResponse,
> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/zetasql.local_service.ZetaSqlLocalService/LenientFormatSql",
    req_mar: ::grpcio::Marshaller {
        ser: ::grpcio::pr_ser,
        de: ::grpcio::pr_de,
    },
    resp_mar: ::grpcio::Marshaller {
        ser: ::grpcio::pr_ser,
        de: ::grpcio::pr_de,
    },
};
const METHOD_ZETA_SQL_LOCAL_SERVICE_UNREGISTER_CATALOG: ::grpcio::Method<UnregisterRequest, ()> =
    ::grpcio::Method {
        ty: ::grpcio::MethodType::Unary,
        name: "/zetasql.local_service.ZetaSqlLocalService/UnregisterCatalog",
        req_mar: ::grpcio::Marshaller {
            ser: ::grpcio::pr_ser,
            de: ::grpcio::pr_de,
        },
        resp_mar: ::grpcio::Marshaller {
            ser: ::grpcio::pr_ser,
            de: ::grpcio::pr_de,
        },
    };
const METHOD_ZETA_SQL_LOCAL_SERVICE_UNREGISTER_PARSE_RESUME_LOCATION: ::grpcio::Method<
    UnregisterRequest,
    (),
> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/zetasql.local_service.ZetaSqlLocalService/UnregisterParseResumeLocation",
    req_mar: ::grpcio::Marshaller {
        ser: ::grpcio::pr_ser,
        de: ::grpcio::pr_de,
    },
    resp_mar: ::grpcio::Marshaller {
        ser: ::grpcio::pr_ser,
        de: ::grpcio::pr_de,
    },
};
const METHOD_ZETA_SQL_LOCAL_SERVICE_GET_BUILTIN_FUNCTIONS: ::grpcio::Method<
    super::ZetaSqlBuiltinFunctionOptionsProto,
    GetBuiltinFunctionsResponse,
> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/zetasql.local_service.ZetaSqlLocalService/GetBuiltinFunctions",
    req_mar: ::grpcio::Marshaller {
        ser: ::grpcio::pr_ser,
        de: ::grpcio::pr_de,
    },
    resp_mar: ::grpcio::Marshaller {
        ser: ::grpcio::pr_ser,
        de: ::grpcio::pr_de,
    },
};
const METHOD_ZETA_SQL_LOCAL_SERVICE_ADD_SIMPLE_TABLE: ::grpcio::Method<AddSimpleTableRequest, ()> =
    ::grpcio::Method {
        ty: ::grpcio::MethodType::Unary,
        name: "/zetasql.local_service.ZetaSqlLocalService/AddSimpleTable",
        req_mar: ::grpcio::Marshaller {
            ser: ::grpcio::pr_ser,
            de: ::grpcio::pr_de,
        },
        resp_mar: ::grpcio::Marshaller {
            ser: ::grpcio::pr_ser,
            de: ::grpcio::pr_de,
        },
    };
const METHOD_ZETA_SQL_LOCAL_SERVICE_GET_LANGUAGE_OPTIONS: ::grpcio::Method<
    LanguageOptionsRequest,
    super::LanguageOptionsProto,
> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/zetasql.local_service.ZetaSqlLocalService/GetLanguageOptions",
    req_mar: ::grpcio::Marshaller {
        ser: ::grpcio::pr_ser,
        de: ::grpcio::pr_de,
    },
    resp_mar: ::grpcio::Marshaller {
        ser: ::grpcio::pr_ser,
        de: ::grpcio::pr_de,
    },
};
const METHOD_ZETA_SQL_LOCAL_SERVICE_GET_ANALYZER_OPTIONS: ::grpcio::Method<
    AnalyzerOptionsRequest,
    super::AnalyzerOptionsProto,
> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/zetasql.local_service.ZetaSqlLocalService/GetAnalyzerOptions",
    req_mar: ::grpcio::Marshaller {
        ser: ::grpcio::pr_ser,
        de: ::grpcio::pr_de,
    },
    resp_mar: ::grpcio::Marshaller {
        ser: ::grpcio::pr_ser,
        de: ::grpcio::pr_de,
    },
};
#[derive(Clone)]
pub struct ZetaSqlLocalServiceClient {
    client: ::grpcio::Client,
}
impl ZetaSqlLocalServiceClient {
    pub fn new(channel: ::grpcio::Channel) -> Self {
        ZetaSqlLocalServiceClient {
            client: ::grpcio::Client::new(channel),
        }
    }
    pub fn prepare_opt(
        &self,
        req: &PrepareRequest,
        opt: ::grpcio::CallOption,
    ) -> ::grpcio::Result<PrepareResponse> {
        self.client
            .unary_call(&METHOD_ZETA_SQL_LOCAL_SERVICE_PREPARE, req, opt)
    }
    pub fn prepare(&self, req: &PrepareRequest) -> ::grpcio::Result<PrepareResponse> {
        self.prepare_opt(req, ::grpcio::CallOption::default())
    }
    pub fn prepare_async_opt(
        &self,
        req: &PrepareRequest,
        opt: ::grpcio::CallOption,
    ) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<PrepareResponse>> {
        self.client
            .unary_call_async(&METHOD_ZETA_SQL_LOCAL_SERVICE_PREPARE, req, opt)
    }
    pub fn prepare_async(
        &self,
        req: &PrepareRequest,
    ) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<PrepareResponse>> {
        self.prepare_async_opt(req, ::grpcio::CallOption::default())
    }
    pub fn evaluate_opt(
        &self,
        req: &EvaluateRequest,
        opt: ::grpcio::CallOption,
    ) -> ::grpcio::Result<EvaluateResponse> {
        self.client
            .unary_call(&METHOD_ZETA_SQL_LOCAL_SERVICE_EVALUATE, req, opt)
    }
    pub fn evaluate(&self, req: &EvaluateRequest) -> ::grpcio::Result<EvaluateResponse> {
        self.evaluate_opt(req, ::grpcio::CallOption::default())
    }
    pub fn evaluate_async_opt(
        &self,
        req: &EvaluateRequest,
        opt: ::grpcio::CallOption,
    ) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<EvaluateResponse>> {
        self.client
            .unary_call_async(&METHOD_ZETA_SQL_LOCAL_SERVICE_EVALUATE, req, opt)
    }
    pub fn evaluate_async(
        &self,
        req: &EvaluateRequest,
    ) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<EvaluateResponse>> {
        self.evaluate_async_opt(req, ::grpcio::CallOption::default())
    }
    pub fn evaluate_stream_opt(
        &self,
        opt: ::grpcio::CallOption,
    ) -> ::grpcio::Result<(
        ::grpcio::ClientDuplexSender<EvaluateRequestBatch>,
        ::grpcio::ClientDuplexReceiver<EvaluateResponseBatch>,
    )> {
        self.client
            .duplex_streaming(&METHOD_ZETA_SQL_LOCAL_SERVICE_EVALUATE_STREAM, opt)
    }
    pub fn evaluate_stream(
        &self,
    ) -> ::grpcio::Result<(
        ::grpcio::ClientDuplexSender<EvaluateRequestBatch>,
        ::grpcio::ClientDuplexReceiver<EvaluateResponseBatch>,
    )> {
        self.evaluate_stream_opt(::grpcio::CallOption::default())
    }
    pub fn unprepare_opt(
        &self,
        req: &UnprepareRequest,
        opt: ::grpcio::CallOption,
    ) -> ::grpcio::Result<()> {
        self.client
            .unary_call(&METHOD_ZETA_SQL_LOCAL_SERVICE_UNPREPARE, req, opt)
    }
    pub fn unprepare(&self, req: &UnprepareRequest) -> ::grpcio::Result<()> {
        self.unprepare_opt(req, ::grpcio::CallOption::default())
    }
    pub fn unprepare_async_opt(
        &self,
        req: &UnprepareRequest,
        opt: ::grpcio::CallOption,
    ) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<()>> {
        self.client
            .unary_call_async(&METHOD_ZETA_SQL_LOCAL_SERVICE_UNPREPARE, req, opt)
    }
    pub fn unprepare_async(
        &self,
        req: &UnprepareRequest,
    ) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<()>> {
        self.unprepare_async_opt(req, ::grpcio::CallOption::default())
    }
    pub fn get_table_from_proto_opt(
        &self,
        req: &TableFromProtoRequest,
        opt: ::grpcio::CallOption,
    ) -> ::grpcio::Result<super::SimpleTableProto> {
        self.client.unary_call(
            &METHOD_ZETA_SQL_LOCAL_SERVICE_GET_TABLE_FROM_PROTO,
            req,
            opt,
        )
    }
    pub fn get_table_from_proto(
        &self,
        req: &TableFromProtoRequest,
    ) -> ::grpcio::Result<super::SimpleTableProto> {
        self.get_table_from_proto_opt(req, ::grpcio::CallOption::default())
    }
    pub fn get_table_from_proto_async_opt(
        &self,
        req: &TableFromProtoRequest,
        opt: ::grpcio::CallOption,
    ) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::SimpleTableProto>> {
        self.client.unary_call_async(
            &METHOD_ZETA_SQL_LOCAL_SERVICE_GET_TABLE_FROM_PROTO,
            req,
            opt,
        )
    }
    pub fn get_table_from_proto_async(
        &self,
        req: &TableFromProtoRequest,
    ) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::SimpleTableProto>> {
        self.get_table_from_proto_async_opt(req, ::grpcio::CallOption::default())
    }
    pub fn register_catalog_opt(
        &self,
        req: &RegisterCatalogRequest,
        opt: ::grpcio::CallOption,
    ) -> ::grpcio::Result<RegisterResponse> {
        self.client
            .unary_call(&METHOD_ZETA_SQL_LOCAL_SERVICE_REGISTER_CATALOG, req, opt)
    }
    pub fn register_catalog(
        &self,
        req: &RegisterCatalogRequest,
    ) -> ::grpcio::Result<RegisterResponse> {
        self.register_catalog_opt(req, ::grpcio::CallOption::default())
    }
    pub fn register_catalog_async_opt(
        &self,
        req: &RegisterCatalogRequest,
        opt: ::grpcio::CallOption,
    ) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<RegisterResponse>> {
        self.client
            .unary_call_async(&METHOD_ZETA_SQL_LOCAL_SERVICE_REGISTER_CATALOG, req, opt)
    }
    pub fn register_catalog_async(
        &self,
        req: &RegisterCatalogRequest,
    ) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<RegisterResponse>> {
        self.register_catalog_async_opt(req, ::grpcio::CallOption::default())
    }
    pub fn register_parse_resume_location_opt(
        &self,
        req: &super::ParseResumeLocationProto,
        opt: ::grpcio::CallOption,
    ) -> ::grpcio::Result<RegisterResponse> {
        self.client.unary_call(
            &METHOD_ZETA_SQL_LOCAL_SERVICE_REGISTER_PARSE_RESUME_LOCATION,
            req,
            opt,
        )
    }
    pub fn register_parse_resume_location(
        &self,
        req: &super::ParseResumeLocationProto,
    ) -> ::grpcio::Result<RegisterResponse> {
        self.register_parse_resume_location_opt(req, ::grpcio::CallOption::default())
    }
    pub fn register_parse_resume_location_async_opt(
        &self,
        req: &super::ParseResumeLocationProto,
        opt: ::grpcio::CallOption,
    ) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<RegisterResponse>> {
        self.client.unary_call_async(
            &METHOD_ZETA_SQL_LOCAL_SERVICE_REGISTER_PARSE_RESUME_LOCATION,
            req,
            opt,
        )
    }
    pub fn register_parse_resume_location_async(
        &self,
        req: &super::ParseResumeLocationProto,
    ) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<RegisterResponse>> {
        self.register_parse_resume_location_async_opt(req, ::grpcio::CallOption::default())
    }
    pub fn analyze_opt(
        &self,
        req: &AnalyzeRequest,
        opt: ::grpcio::CallOption,
    ) -> ::grpcio::Result<AnalyzeResponse> {
        self.client
            .unary_call(&METHOD_ZETA_SQL_LOCAL_SERVICE_ANALYZE, req, opt)
    }
    pub fn analyze(&self, req: &AnalyzeRequest) -> ::grpcio::Result<AnalyzeResponse> {
        self.analyze_opt(req, ::grpcio::CallOption::default())
    }
    pub fn analyze_async_opt(
        &self,
        req: &AnalyzeRequest,
        opt: ::grpcio::CallOption,
    ) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<AnalyzeResponse>> {
        self.client
            .unary_call_async(&METHOD_ZETA_SQL_LOCAL_SERVICE_ANALYZE, req, opt)
    }
    pub fn analyze_async(
        &self,
        req: &AnalyzeRequest,
    ) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<AnalyzeResponse>> {
        self.analyze_async_opt(req, ::grpcio::CallOption::default())
    }
    pub fn build_sql_opt(
        &self,
        req: &BuildSqlRequest,
        opt: ::grpcio::CallOption,
    ) -> ::grpcio::Result<BuildSqlResponse> {
        self.client
            .unary_call(&METHOD_ZETA_SQL_LOCAL_SERVICE_BUILD_SQL, req, opt)
    }
    pub fn build_sql(&self, req: &BuildSqlRequest) -> ::grpcio::Result<BuildSqlResponse> {
        self.build_sql_opt(req, ::grpcio::CallOption::default())
    }
    pub fn build_sql_async_opt(
        &self,
        req: &BuildSqlRequest,
        opt: ::grpcio::CallOption,
    ) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<BuildSqlResponse>> {
        self.client
            .unary_call_async(&METHOD_ZETA_SQL_LOCAL_SERVICE_BUILD_SQL, req, opt)
    }
    pub fn build_sql_async(
        &self,
        req: &BuildSqlRequest,
    ) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<BuildSqlResponse>> {
        self.build_sql_async_opt(req, ::grpcio::CallOption::default())
    }
    pub fn extract_table_names_from_statement_opt(
        &self,
        req: &ExtractTableNamesFromStatementRequest,
        opt: ::grpcio::CallOption,
    ) -> ::grpcio::Result<ExtractTableNamesFromStatementResponse> {
        self.client.unary_call(
            &METHOD_ZETA_SQL_LOCAL_SERVICE_EXTRACT_TABLE_NAMES_FROM_STATEMENT,
            req,
            opt,
        )
    }
    pub fn extract_table_names_from_statement(
        &self,
        req: &ExtractTableNamesFromStatementRequest,
    ) -> ::grpcio::Result<ExtractTableNamesFromStatementResponse> {
        self.extract_table_names_from_statement_opt(req, ::grpcio::CallOption::default())
    }
    pub fn extract_table_names_from_statement_async_opt(
        &self,
        req: &ExtractTableNamesFromStatementRequest,
        opt: ::grpcio::CallOption,
    ) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<ExtractTableNamesFromStatementResponse>>
    {
        self.client.unary_call_async(
            &METHOD_ZETA_SQL_LOCAL_SERVICE_EXTRACT_TABLE_NAMES_FROM_STATEMENT,
            req,
            opt,
        )
    }
    pub fn extract_table_names_from_statement_async(
        &self,
        req: &ExtractTableNamesFromStatementRequest,
    ) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<ExtractTableNamesFromStatementResponse>>
    {
        self.extract_table_names_from_statement_async_opt(req, ::grpcio::CallOption::default())
    }
    pub fn extract_table_names_from_next_statement_opt(
        &self,
        req: &ExtractTableNamesFromNextStatementRequest,
        opt: ::grpcio::CallOption,
    ) -> ::grpcio::Result<ExtractTableNamesFromNextStatementResponse> {
        self.client.unary_call(
            &METHOD_ZETA_SQL_LOCAL_SERVICE_EXTRACT_TABLE_NAMES_FROM_NEXT_STATEMENT,
            req,
            opt,
        )
    }
    pub fn extract_table_names_from_next_statement(
        &self,
        req: &ExtractTableNamesFromNextStatementRequest,
    ) -> ::grpcio::Result<ExtractTableNamesFromNextStatementResponse> {
        self.extract_table_names_from_next_statement_opt(req, ::grpcio::CallOption::default())
    }
    pub fn extract_table_names_from_next_statement_async_opt(
        &self,
        req: &ExtractTableNamesFromNextStatementRequest,
        opt: ::grpcio::CallOption,
    ) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<ExtractTableNamesFromNextStatementResponse>>
    {
        self.client.unary_call_async(
            &METHOD_ZETA_SQL_LOCAL_SERVICE_EXTRACT_TABLE_NAMES_FROM_NEXT_STATEMENT,
            req,
            opt,
        )
    }
    pub fn extract_table_names_from_next_statement_async(
        &self,
        req: &ExtractTableNamesFromNextStatementRequest,
    ) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<ExtractTableNamesFromNextStatementResponse>>
    {
        self.extract_table_names_from_next_statement_async_opt(req, ::grpcio::CallOption::default())
    }
    pub fn format_sql_opt(
        &self,
        req: &FormatSqlRequest,
        opt: ::grpcio::CallOption,
    ) -> ::grpcio::Result<FormatSqlResponse> {
        self.client
            .unary_call(&METHOD_ZETA_SQL_LOCAL_SERVICE_FORMAT_SQL, req, opt)
    }
    pub fn format_sql(&self, req: &FormatSqlRequest) -> ::grpcio::Result<FormatSqlResponse> {
        self.format_sql_opt(req, ::grpcio::CallOption::default())
    }
    pub fn format_sql_async_opt(
        &self,
        req: &FormatSqlRequest,
        opt: ::grpcio::CallOption,
    ) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<FormatSqlResponse>> {
        self.client
            .unary_call_async(&METHOD_ZETA_SQL_LOCAL_SERVICE_FORMAT_SQL, req, opt)
    }
    pub fn format_sql_async(
        &self,
        req: &FormatSqlRequest,
    ) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<FormatSqlResponse>> {
        self.format_sql_async_opt(req, ::grpcio::CallOption::default())
    }
    pub fn lenient_format_sql_opt(
        &self,
        req: &FormatSqlRequest,
        opt: ::grpcio::CallOption,
    ) -> ::grpcio::Result<FormatSqlResponse> {
        self.client
            .unary_call(&METHOD_ZETA_SQL_LOCAL_SERVICE_LENIENT_FORMAT_SQL, req, opt)
    }
    pub fn lenient_format_sql(
        &self,
        req: &FormatSqlRequest,
    ) -> ::grpcio::Result<FormatSqlResponse> {
        self.lenient_format_sql_opt(req, ::grpcio::CallOption::default())
    }
    pub fn lenient_format_sql_async_opt(
        &self,
        req: &FormatSqlRequest,
        opt: ::grpcio::CallOption,
    ) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<FormatSqlResponse>> {
        self.client
            .unary_call_async(&METHOD_ZETA_SQL_LOCAL_SERVICE_LENIENT_FORMAT_SQL, req, opt)
    }
    pub fn lenient_format_sql_async(
        &self,
        req: &FormatSqlRequest,
    ) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<FormatSqlResponse>> {
        self.lenient_format_sql_async_opt(req, ::grpcio::CallOption::default())
    }
    pub fn unregister_catalog_opt(
        &self,
        req: &UnregisterRequest,
        opt: ::grpcio::CallOption,
    ) -> ::grpcio::Result<()> {
        self.client
            .unary_call(&METHOD_ZETA_SQL_LOCAL_SERVICE_UNREGISTER_CATALOG, req, opt)
    }
    pub fn unregister_catalog(&self, req: &UnregisterRequest) -> ::grpcio::Result<()> {
        self.unregister_catalog_opt(req, ::grpcio::CallOption::default())
    }
    pub fn unregister_catalog_async_opt(
        &self,
        req: &UnregisterRequest,
        opt: ::grpcio::CallOption,
    ) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<()>> {
        self.client
            .unary_call_async(&METHOD_ZETA_SQL_LOCAL_SERVICE_UNREGISTER_CATALOG, req, opt)
    }
    pub fn unregister_catalog_async(
        &self,
        req: &UnregisterRequest,
    ) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<()>> {
        self.unregister_catalog_async_opt(req, ::grpcio::CallOption::default())
    }
    pub fn unregister_parse_resume_location_opt(
        &self,
        req: &UnregisterRequest,
        opt: ::grpcio::CallOption,
    ) -> ::grpcio::Result<()> {
        self.client.unary_call(
            &METHOD_ZETA_SQL_LOCAL_SERVICE_UNREGISTER_PARSE_RESUME_LOCATION,
            req,
            opt,
        )
    }
    pub fn unregister_parse_resume_location(
        &self,
        req: &UnregisterRequest,
    ) -> ::grpcio::Result<()> {
        self.unregister_parse_resume_location_opt(req, ::grpcio::CallOption::default())
    }
    pub fn unregister_parse_resume_location_async_opt(
        &self,
        req: &UnregisterRequest,
        opt: ::grpcio::CallOption,
    ) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<()>> {
        self.client.unary_call_async(
            &METHOD_ZETA_SQL_LOCAL_SERVICE_UNREGISTER_PARSE_RESUME_LOCATION,
            req,
            opt,
        )
    }
    pub fn unregister_parse_resume_location_async(
        &self,
        req: &UnregisterRequest,
    ) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<()>> {
        self.unregister_parse_resume_location_async_opt(req, ::grpcio::CallOption::default())
    }
    pub fn get_builtin_functions_opt(
        &self,
        req: &super::ZetaSqlBuiltinFunctionOptionsProto,
        opt: ::grpcio::CallOption,
    ) -> ::grpcio::Result<GetBuiltinFunctionsResponse> {
        self.client.unary_call(
            &METHOD_ZETA_SQL_LOCAL_SERVICE_GET_BUILTIN_FUNCTIONS,
            req,
            opt,
        )
    }
    pub fn get_builtin_functions(
        &self,
        req: &super::ZetaSqlBuiltinFunctionOptionsProto,
    ) -> ::grpcio::Result<GetBuiltinFunctionsResponse> {
        self.get_builtin_functions_opt(req, ::grpcio::CallOption::default())
    }
    pub fn get_builtin_functions_async_opt(
        &self,
        req: &super::ZetaSqlBuiltinFunctionOptionsProto,
        opt: ::grpcio::CallOption,
    ) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<GetBuiltinFunctionsResponse>> {
        self.client.unary_call_async(
            &METHOD_ZETA_SQL_LOCAL_SERVICE_GET_BUILTIN_FUNCTIONS,
            req,
            opt,
        )
    }
    pub fn get_builtin_functions_async(
        &self,
        req: &super::ZetaSqlBuiltinFunctionOptionsProto,
    ) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<GetBuiltinFunctionsResponse>> {
        self.get_builtin_functions_async_opt(req, ::grpcio::CallOption::default())
    }
    pub fn add_simple_table_opt(
        &self,
        req: &AddSimpleTableRequest,
        opt: ::grpcio::CallOption,
    ) -> ::grpcio::Result<()> {
        self.client
            .unary_call(&METHOD_ZETA_SQL_LOCAL_SERVICE_ADD_SIMPLE_TABLE, req, opt)
    }
    pub fn add_simple_table(&self, req: &AddSimpleTableRequest) -> ::grpcio::Result<()> {
        self.add_simple_table_opt(req, ::grpcio::CallOption::default())
    }
    pub fn add_simple_table_async_opt(
        &self,
        req: &AddSimpleTableRequest,
        opt: ::grpcio::CallOption,
    ) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<()>> {
        self.client
            .unary_call_async(&METHOD_ZETA_SQL_LOCAL_SERVICE_ADD_SIMPLE_TABLE, req, opt)
    }
    pub fn add_simple_table_async(
        &self,
        req: &AddSimpleTableRequest,
    ) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<()>> {
        self.add_simple_table_async_opt(req, ::grpcio::CallOption::default())
    }
    pub fn get_language_options_opt(
        &self,
        req: &LanguageOptionsRequest,
        opt: ::grpcio::CallOption,
    ) -> ::grpcio::Result<super::LanguageOptionsProto> {
        self.client.unary_call(
            &METHOD_ZETA_SQL_LOCAL_SERVICE_GET_LANGUAGE_OPTIONS,
            req,
            opt,
        )
    }
    pub fn get_language_options(
        &self,
        req: &LanguageOptionsRequest,
    ) -> ::grpcio::Result<super::LanguageOptionsProto> {
        self.get_language_options_opt(req, ::grpcio::CallOption::default())
    }
    pub fn get_language_options_async_opt(
        &self,
        req: &LanguageOptionsRequest,
        opt: ::grpcio::CallOption,
    ) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::LanguageOptionsProto>> {
        self.client.unary_call_async(
            &METHOD_ZETA_SQL_LOCAL_SERVICE_GET_LANGUAGE_OPTIONS,
            req,
            opt,
        )
    }
    pub fn get_language_options_async(
        &self,
        req: &LanguageOptionsRequest,
    ) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::LanguageOptionsProto>> {
        self.get_language_options_async_opt(req, ::grpcio::CallOption::default())
    }
    pub fn get_analyzer_options_opt(
        &self,
        req: &AnalyzerOptionsRequest,
        opt: ::grpcio::CallOption,
    ) -> ::grpcio::Result<super::AnalyzerOptionsProto> {
        self.client.unary_call(
            &METHOD_ZETA_SQL_LOCAL_SERVICE_GET_ANALYZER_OPTIONS,
            req,
            opt,
        )
    }
    pub fn get_analyzer_options(
        &self,
        req: &AnalyzerOptionsRequest,
    ) -> ::grpcio::Result<super::AnalyzerOptionsProto> {
        self.get_analyzer_options_opt(req, ::grpcio::CallOption::default())
    }
    pub fn get_analyzer_options_async_opt(
        &self,
        req: &AnalyzerOptionsRequest,
        opt: ::grpcio::CallOption,
    ) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::AnalyzerOptionsProto>> {
        self.client.unary_call_async(
            &METHOD_ZETA_SQL_LOCAL_SERVICE_GET_ANALYZER_OPTIONS,
            req,
            opt,
        )
    }
    pub fn get_analyzer_options_async(
        &self,
        req: &AnalyzerOptionsRequest,
    ) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::AnalyzerOptionsProto>> {
        self.get_analyzer_options_async_opt(req, ::grpcio::CallOption::default())
    }
    pub fn spawn<F>(&self, f: F)
    where
        F: ::futures::Future<Output = ()> + Send + 'static,
    {
        self.client.spawn(f)
    }
}
pub trait ZetaSqlLocalService {
    fn prepare(
        &mut self,
        ctx: ::grpcio::RpcContext,
        req: PrepareRequest,
        sink: ::grpcio::UnarySink<PrepareResponse>,
    );
    fn evaluate(
        &mut self,
        ctx: ::grpcio::RpcContext,
        req: EvaluateRequest,
        sink: ::grpcio::UnarySink<EvaluateResponse>,
    );
    fn evaluate_stream(
        &mut self,
        ctx: ::grpcio::RpcContext,
        stream: ::grpcio::RequestStream<EvaluateRequestBatch>,
        sink: ::grpcio::DuplexSink<EvaluateResponseBatch>,
    );
    fn unprepare(
        &mut self,
        ctx: ::grpcio::RpcContext,
        req: UnprepareRequest,
        sink: ::grpcio::UnarySink<()>,
    );
    fn get_table_from_proto(
        &mut self,
        ctx: ::grpcio::RpcContext,
        req: TableFromProtoRequest,
        sink: ::grpcio::UnarySink<super::SimpleTableProto>,
    );
    fn register_catalog(
        &mut self,
        ctx: ::grpcio::RpcContext,
        req: RegisterCatalogRequest,
        sink: ::grpcio::UnarySink<RegisterResponse>,
    );
    fn register_parse_resume_location(
        &mut self,
        ctx: ::grpcio::RpcContext,
        req: super::ParseResumeLocationProto,
        sink: ::grpcio::UnarySink<RegisterResponse>,
    );
    fn analyze(
        &mut self,
        ctx: ::grpcio::RpcContext,
        req: AnalyzeRequest,
        sink: ::grpcio::UnarySink<AnalyzeResponse>,
    );
    fn build_sql(
        &mut self,
        ctx: ::grpcio::RpcContext,
        req: BuildSqlRequest,
        sink: ::grpcio::UnarySink<BuildSqlResponse>,
    );
    fn extract_table_names_from_statement(
        &mut self,
        ctx: ::grpcio::RpcContext,
        req: ExtractTableNamesFromStatementRequest,
        sink: ::grpcio::UnarySink<ExtractTableNamesFromStatementResponse>,
    );
    fn extract_table_names_from_next_statement(
        &mut self,
        ctx: ::grpcio::RpcContext,
        req: ExtractTableNamesFromNextStatementRequest,
        sink: ::grpcio::UnarySink<ExtractTableNamesFromNextStatementResponse>,
    );
    fn format_sql(
        &mut self,
        ctx: ::grpcio::RpcContext,
        req: FormatSqlRequest,
        sink: ::grpcio::UnarySink<FormatSqlResponse>,
    );
    fn lenient_format_sql(
        &mut self,
        ctx: ::grpcio::RpcContext,
        req: FormatSqlRequest,
        sink: ::grpcio::UnarySink<FormatSqlResponse>,
    );
    fn unregister_catalog(
        &mut self,
        ctx: ::grpcio::RpcContext,
        req: UnregisterRequest,
        sink: ::grpcio::UnarySink<()>,
    );
    fn unregister_parse_resume_location(
        &mut self,
        ctx: ::grpcio::RpcContext,
        req: UnregisterRequest,
        sink: ::grpcio::UnarySink<()>,
    );
    fn get_builtin_functions(
        &mut self,
        ctx: ::grpcio::RpcContext,
        req: super::ZetaSqlBuiltinFunctionOptionsProto,
        sink: ::grpcio::UnarySink<GetBuiltinFunctionsResponse>,
    );
    fn add_simple_table(
        &mut self,
        ctx: ::grpcio::RpcContext,
        req: AddSimpleTableRequest,
        sink: ::grpcio::UnarySink<()>,
    );
    fn get_language_options(
        &mut self,
        ctx: ::grpcio::RpcContext,
        req: LanguageOptionsRequest,
        sink: ::grpcio::UnarySink<super::LanguageOptionsProto>,
    );
    fn get_analyzer_options(
        &mut self,
        ctx: ::grpcio::RpcContext,
        req: AnalyzerOptionsRequest,
        sink: ::grpcio::UnarySink<super::AnalyzerOptionsProto>,
    );
}
pub fn create_zeta_sql_local_service<S: ZetaSqlLocalService + Send + Clone + 'static>(
    s: S,
) -> ::grpcio::Service {
    let mut builder = ::grpcio::ServiceBuilder::new();
    let mut instance = s.clone();
    builder = builder.add_unary_handler(
        &METHOD_ZETA_SQL_LOCAL_SERVICE_PREPARE,
        move |ctx, req, resp| instance.prepare(ctx, req, resp),
    );
    let mut instance = s.clone();
    builder = builder.add_unary_handler(
        &METHOD_ZETA_SQL_LOCAL_SERVICE_EVALUATE,
        move |ctx, req, resp| instance.evaluate(ctx, req, resp),
    );
    let mut instance = s.clone();
    builder = builder.add_duplex_streaming_handler(
        &METHOD_ZETA_SQL_LOCAL_SERVICE_EVALUATE_STREAM,
        move |ctx, req, resp| instance.evaluate_stream(ctx, req, resp),
    );
    let mut instance = s.clone();
    builder = builder.add_unary_handler(
        &METHOD_ZETA_SQL_LOCAL_SERVICE_UNPREPARE,
        move |ctx, req, resp| instance.unprepare(ctx, req, resp),
    );
    let mut instance = s.clone();
    builder = builder.add_unary_handler(
        &METHOD_ZETA_SQL_LOCAL_SERVICE_GET_TABLE_FROM_PROTO,
        move |ctx, req, resp| instance.get_table_from_proto(ctx, req, resp),
    );
    let mut instance = s.clone();
    builder = builder.add_unary_handler(
        &METHOD_ZETA_SQL_LOCAL_SERVICE_REGISTER_CATALOG,
        move |ctx, req, resp| instance.register_catalog(ctx, req, resp),
    );
    let mut instance = s.clone();
    builder = builder.add_unary_handler(
        &METHOD_ZETA_SQL_LOCAL_SERVICE_REGISTER_PARSE_RESUME_LOCATION,
        move |ctx, req, resp| instance.register_parse_resume_location(ctx, req, resp),
    );
    let mut instance = s.clone();
    builder = builder.add_unary_handler(
        &METHOD_ZETA_SQL_LOCAL_SERVICE_ANALYZE,
        move |ctx, req, resp| instance.analyze(ctx, req, resp),
    );
    let mut instance = s.clone();
    builder = builder.add_unary_handler(
        &METHOD_ZETA_SQL_LOCAL_SERVICE_BUILD_SQL,
        move |ctx, req, resp| instance.build_sql(ctx, req, resp),
    );
    let mut instance = s.clone();
    builder = builder.add_unary_handler(
        &METHOD_ZETA_SQL_LOCAL_SERVICE_EXTRACT_TABLE_NAMES_FROM_STATEMENT,
        move |ctx, req, resp| instance.extract_table_names_from_statement(ctx, req, resp),
    );
    let mut instance = s.clone();
    builder = builder.add_unary_handler(
        &METHOD_ZETA_SQL_LOCAL_SERVICE_EXTRACT_TABLE_NAMES_FROM_NEXT_STATEMENT,
        move |ctx, req, resp| instance.extract_table_names_from_next_statement(ctx, req, resp),
    );
    let mut instance = s.clone();
    builder = builder.add_unary_handler(
        &METHOD_ZETA_SQL_LOCAL_SERVICE_FORMAT_SQL,
        move |ctx, req, resp| instance.format_sql(ctx, req, resp),
    );
    let mut instance = s.clone();
    builder = builder.add_unary_handler(
        &METHOD_ZETA_SQL_LOCAL_SERVICE_LENIENT_FORMAT_SQL,
        move |ctx, req, resp| instance.lenient_format_sql(ctx, req, resp),
    );
    let mut instance = s.clone();
    builder = builder.add_unary_handler(
        &METHOD_ZETA_SQL_LOCAL_SERVICE_UNREGISTER_CATALOG,
        move |ctx, req, resp| instance.unregister_catalog(ctx, req, resp),
    );
    let mut instance = s.clone();
    builder = builder.add_unary_handler(
        &METHOD_ZETA_SQL_LOCAL_SERVICE_UNREGISTER_PARSE_RESUME_LOCATION,
        move |ctx, req, resp| instance.unregister_parse_resume_location(ctx, req, resp),
    );
    let mut instance = s.clone();
    builder = builder.add_unary_handler(
        &METHOD_ZETA_SQL_LOCAL_SERVICE_GET_BUILTIN_FUNCTIONS,
        move |ctx, req, resp| instance.get_builtin_functions(ctx, req, resp),
    );
    let mut instance = s.clone();
    builder = builder.add_unary_handler(
        &METHOD_ZETA_SQL_LOCAL_SERVICE_ADD_SIMPLE_TABLE,
        move |ctx, req, resp| instance.add_simple_table(ctx, req, resp),
    );
    let mut instance = s.clone();
    builder = builder.add_unary_handler(
        &METHOD_ZETA_SQL_LOCAL_SERVICE_GET_LANGUAGE_OPTIONS,
        move |ctx, req, resp| instance.get_language_options(ctx, req, resp),
    );
    let mut instance = s;
    builder = builder.add_unary_handler(
        &METHOD_ZETA_SQL_LOCAL_SERVICE_GET_ANALYZER_OPTIONS,
        move |ctx, req, resp| instance.get_analyzer_options(ctx, req, resp),
    );
    builder.build()
}
