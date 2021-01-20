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
#[doc = r" Generated client implementations."]
pub mod zeta_sql_local_service_client {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    #[doc = " Note that *all* service RPCs are locked down to only be authorized by"]
    #[doc = " the user that started the ZetaSql service.  This disallows random users"]
    #[doc = " from sending requests to this service to be executed on behalf of the"]
    #[doc = " user that started the service.  Given that this service does not currently"]
    #[doc = " provide access to external data, this lockdown is precautionary and"]
    #[doc = " conservative.  But the lockdown will be necessary once the reference"]
    #[doc = " implementation is extended to execute full queries over external data,"]
    #[doc = " since we cannot allow random users to run queries over data accessible by"]
    #[doc = " the (different) user that started the ZetaSql service."]
    pub struct ZetaSqlLocalServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl ZetaSqlLocalServiceClient<tonic::transport::Channel> {
        #[doc = r" Attempt to create a new client by connecting to a given endpoint."]
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> ZetaSqlLocalServiceClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::ResponseBody: Body + HttpBody + Send + 'static,
        T::Error: Into<StdError>,
        <T::ResponseBody as HttpBody>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor(inner: T, interceptor: impl Into<tonic::Interceptor>) -> Self {
            let inner = tonic::client::Grpc::with_interceptor(inner, interceptor);
            Self { inner }
        }
        #[doc = " Prepare the sql expression in PrepareRequest with given columns and"]
        #[doc = " parameters with zetasql::PreparedExpression and return the result"]
        #[doc = " type as PrepareResponse. The prepared expression will be kept at server"]
        #[doc = " side which can be referred to with the returned id."]
        pub async fn prepare(
            &mut self,
            request: impl tonic::IntoRequest<super::PrepareRequest>,
        ) -> Result<tonic::Response<super::PrepareResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/zetasql.local_service.ZetaSqlLocalService/Prepare",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Evaluate the prepared expression in EvaluateRequest with given columns and"]
        #[doc = " parameters with zetasql::PreparedExpression and return the result"]
        #[doc = " and value as EvaluateResponse."]
        pub async fn evaluate(
            &mut self,
            request: impl tonic::IntoRequest<super::EvaluateRequest>,
        ) -> Result<tonic::Response<super::EvaluateResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/zetasql.local_service.ZetaSqlLocalService/Evaluate",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Evaluate a Stream of prepared expression batches and return a stream of"]
        #[doc = " result batches. Requests will be evaluated and returned in the same order"]
        #[doc = " as received, but batches may be packed differently."]
        pub async fn evaluate_stream(
            &mut self,
            request: impl tonic::IntoStreamingRequest<Message = super::EvaluateRequestBatch>,
        ) -> Result<
            tonic::Response<tonic::codec::Streaming<super::EvaluateResponseBatch>>,
            tonic::Status,
        > {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/zetasql.local_service.ZetaSqlLocalService/EvaluateStream",
            );
            self.inner
                .streaming(request.into_streaming_request(), path, codec)
                .await
        }
        #[doc = " Cleanup the prepared expression kept at server side with given id."]
        pub async fn unprepare(
            &mut self,
            request: impl tonic::IntoRequest<super::UnprepareRequest>,
        ) -> Result<tonic::Response<()>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/zetasql.local_service.ZetaSqlLocalService/Unprepare",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Get a table schema from a proto."]
        pub async fn get_table_from_proto(
            &mut self,
            request: impl tonic::IntoRequest<super::TableFromProtoRequest>,
        ) -> Result<tonic::Response<super::super::SimpleTableProto>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/zetasql.local_service.ZetaSqlLocalService/GetTableFromProto",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Register a catalog at server side so that it can be reused."]
        pub async fn register_catalog(
            &mut self,
            request: impl tonic::IntoRequest<super::RegisterCatalogRequest>,
        ) -> Result<tonic::Response<super::RegisterResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/zetasql.local_service.ZetaSqlLocalService/RegisterCatalog",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Register a parse resume location"]
        pub async fn register_parse_resume_location(
            &mut self,
            request: impl tonic::IntoRequest<super::super::ParseResumeLocationProto>,
        ) -> Result<tonic::Response<super::RegisterResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/zetasql.local_service.ZetaSqlLocalService/RegisterParseResumeLocation",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Analyze a SQL statement, return the resolved AST and an optional byte"]
        #[doc = " position if end of input is not yet reached."]
        pub async fn analyze(
            &mut self,
            request: impl tonic::IntoRequest<super::AnalyzeRequest>,
        ) -> Result<tonic::Response<super::AnalyzeResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/zetasql.local_service.ZetaSqlLocalService/Analyze",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Build a SQL statement or expression from a resolved AST."]
        pub async fn build_sql(
            &mut self,
            request: impl tonic::IntoRequest<super::BuildSqlRequest>,
        ) -> Result<tonic::Response<super::BuildSqlResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/zetasql.local_service.ZetaSqlLocalService/BuildSql",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Validate statement and extract table names."]
        pub async fn extract_table_names_from_statement(
            &mut self,
            request: impl tonic::IntoRequest<super::ExtractTableNamesFromStatementRequest>,
        ) -> Result<tonic::Response<super::ExtractTableNamesFromStatementResponse>, tonic::Status>
        {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/zetasql.local_service.ZetaSqlLocalService/ExtractTableNamesFromStatement",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Validate statement, return table names and a byte offset of the next"]
        #[doc = " statement."]
        #[doc = ""]
        #[doc = " Statements are separated by semicolons. A final semicolon is not required"]
        #[doc = " on the last statement. Whitespace between statements is ignored. Whitespace"]
        #[doc = " after that last semicolon is treated as a separate empty statement and"]
        #[doc = " would cause a parse error."]
        #[doc = ""]
        #[doc = " Full text passed to this method does not need to be syntactically valid;"]
        #[doc = " only the current statement pointed by parse_resume_location should be"]
        #[doc = " parseable."]
        #[doc = ""]
        #[doc = " Passing incorrect parse_resume_position (negative or pointing outside"]
        #[doc = " input data) would result in a generic::internal error."]
        #[doc = ""]
        #[doc = " If language options are not provided, the parser would use a default"]
        #[doc = " LanguageOptions object. Refer to the LanguageOptions class definition"]
        #[doc = " for the exact implementation details."]
        #[doc = ""]
        #[doc = " Client can detect that there's no more SQL statements to parse"]
        #[doc = " by comparing this byte offset to the overall input length"]
        #[doc = " (similar to Analyze method)"]
        #[doc = ""]
        #[doc = " After a parse error, parse_resume_location is not available; it's up"]
        #[doc = " to client to try and recover from parse errors."]
        #[doc = ""]
        #[doc = " Unsupported statements (e.g. SET statements from F1 dialect) are treated"]
        #[doc = " as parse errors."]
        #[doc = ""]
        #[doc = " Note: statements are handled by ParseNextStatement function."]
        #[doc = " Documentation on that function is the source of truth on the behavior."]
        #[doc = ""]
        pub async fn extract_table_names_from_next_statement(
            &mut self,
            request: impl tonic::IntoRequest<super::ExtractTableNamesFromNextStatementRequest>,
        ) -> Result<tonic::Response<super::ExtractTableNamesFromNextStatementResponse>, tonic::Status>
        {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/zetasql.local_service.ZetaSqlLocalService/ExtractTableNamesFromNextStatement",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Format a SQL statement (see also (broken link))"]
        pub async fn format_sql(
            &mut self,
            request: impl tonic::IntoRequest<super::FormatSqlRequest>,
        ) -> Result<tonic::Response<super::FormatSqlResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/zetasql.local_service.ZetaSqlLocalService/FormatSql",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Format a SQL statement using the new lenient_formatter.h"]
        pub async fn lenient_format_sql(
            &mut self,
            request: impl tonic::IntoRequest<super::FormatSqlRequest>,
        ) -> Result<tonic::Response<super::FormatSqlResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/zetasql.local_service.ZetaSqlLocalService/LenientFormatSql",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Cleanup a registered catalog."]
        pub async fn unregister_catalog(
            &mut self,
            request: impl tonic::IntoRequest<super::UnregisterRequest>,
        ) -> Result<tonic::Response<()>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/zetasql.local_service.ZetaSqlLocalService/UnregisterCatalog",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Cleanup a registered parse resume location."]
        pub async fn unregister_parse_resume_location(
            &mut self,
            request: impl tonic::IntoRequest<super::UnregisterRequest>,
        ) -> Result<tonic::Response<()>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/zetasql.local_service.ZetaSqlLocalService/UnregisterParseResumeLocation",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Get ZetaSQL builtin functions specified by given options."]
        pub async fn get_builtin_functions(
            &mut self,
            request: impl tonic::IntoRequest<super::super::ZetaSqlBuiltinFunctionOptionsProto>,
        ) -> Result<tonic::Response<super::GetBuiltinFunctionsResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/zetasql.local_service.ZetaSqlLocalService/GetBuiltinFunctions",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Add SimpleTable"]
        pub async fn add_simple_table(
            &mut self,
            request: impl tonic::IntoRequest<super::AddSimpleTableRequest>,
        ) -> Result<tonic::Response<()>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/zetasql.local_service.ZetaSqlLocalService/AddSimpleTable",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Gets ZetaSQL lanauge options."]
        pub async fn get_language_options(
            &mut self,
            request: impl tonic::IntoRequest<super::LanguageOptionsRequest>,
        ) -> Result<tonic::Response<super::super::LanguageOptionsProto>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/zetasql.local_service.ZetaSqlLocalService/GetLanguageOptions",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " Gets ZetaSQL analyzer options."]
        pub async fn get_analyzer_options(
            &mut self,
            request: impl tonic::IntoRequest<super::AnalyzerOptionsRequest>,
        ) -> Result<tonic::Response<super::super::AnalyzerOptionsProto>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/zetasql.local_service.ZetaSqlLocalService/GetAnalyzerOptions",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
    impl<T: Clone> Clone for ZetaSqlLocalServiceClient<T> {
        fn clone(&self) -> Self {
            Self {
                inner: self.inner.clone(),
            }
        }
    }
    impl<T> std::fmt::Debug for ZetaSqlLocalServiceClient<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "ZetaSqlLocalServiceClient {{ ... }}")
        }
    }
}
