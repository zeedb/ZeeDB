#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PrepareRequest {
    #[prost(string, optional, tag = "1")]
    pub sql: ::std::option::Option<std::string::String>,
    #[prost(message, optional, tag = "2")]
    pub options: ::std::option::Option<super::AnalyzerOptionsProto>,
    /// Serialized descriptor pools of all types in the request.
    #[prost(message, repeated, tag = "3")]
    pub file_descriptor_set: ::std::vec::Vec<::prost_types::FileDescriptorSet>,
    #[prost(message, optional, tag = "4")]
    pub simple_catalog: ::std::option::Option<super::SimpleCatalogProto>,
    #[prost(int64, optional, tag = "5")]
    pub registered_catalog_id: ::std::option::Option<i64>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PrepareResponse {
    #[prost(int64, optional, tag = "1")]
    pub prepared_expression_id: ::std::option::Option<i64>,
    /// No file_descriptor_set returned. Use the same descriptor pools as sent in
    /// the request deserialize the type.
    #[prost(message, optional, tag = "2")]
    pub output_type: ::std::option::Option<super::TypeProto>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EvaluateRequest {
    #[prost(string, optional, tag = "1")]
    pub sql: ::std::option::Option<std::string::String>,
    #[prost(message, repeated, tag = "2")]
    pub columns: ::std::vec::Vec<evaluate_request::Parameter>,
    #[prost(message, repeated, tag = "3")]
    pub params: ::std::vec::Vec<evaluate_request::Parameter>,
    /// Serialized descriptor pools of all types in the request.
    #[prost(message, repeated, tag = "4")]
    pub file_descriptor_set: ::std::vec::Vec<::prost_types::FileDescriptorSet>,
    /// Set if the expression is already prepared, in which case sql and
    /// file_descriptor_set will be ignored.
    #[prost(int64, optional, tag = "5")]
    pub prepared_expression_id: ::std::option::Option<i64>,
}
pub mod evaluate_request {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Parameter {
        #[prost(string, optional, tag = "1")]
        pub name: ::std::option::Option<std::string::String>,
        #[prost(message, optional, tag = "2")]
        pub value: ::std::option::Option<super::super::ValueProto>,
        #[prost(message, optional, tag = "3")]
        pub r#type: ::std::option::Option<super::super::TypeProto>,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EvaluateResponse {
    #[prost(message, optional, tag = "1")]
    pub value: ::std::option::Option<super::ValueProto>,
    #[prost(message, optional, tag = "2")]
    pub r#type: ::std::option::Option<super::TypeProto>,
    #[prost(int64, optional, tag = "3")]
    pub prepared_expression_id: ::std::option::Option<i64>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UnprepareRequest {
    #[prost(int64, optional, tag = "1")]
    pub prepared_expression_id: ::std::option::Option<i64>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableFromProtoRequest {
    #[prost(message, optional, tag = "1")]
    pub proto: ::std::option::Option<super::ProtoTypeProto>,
    #[prost(message, optional, tag = "2")]
    pub file_descriptor_set: ::std::option::Option<::prost_types::FileDescriptorSet>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RegisteredParseResumeLocationProto {
    #[prost(int64, optional, tag = "1")]
    pub registered_id: ::std::option::Option<i64>,
    #[prost(int32, optional, tag = "2")]
    pub byte_position: ::std::option::Option<i32>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AnalyzeRequest {
    #[prost(message, optional, tag = "1")]
    pub options: ::std::option::Option<super::AnalyzerOptionsProto>,
    #[prost(message, optional, tag = "2")]
    pub simple_catalog: ::std::option::Option<super::SimpleCatalogProto>,
    #[prost(message, repeated, tag = "3")]
    pub file_descriptor_set: ::std::vec::Vec<::prost_types::FileDescriptorSet>,
    /// Set if using a registered catalog, in which case simple_catalog and
    /// file_descriptor_set will be ignored.
    #[prost(int64, optional, tag = "4")]
    pub registered_catalog_id: ::std::option::Option<i64>,
    #[prost(oneof = "analyze_request::Target", tags = "5, 6, 7, 8")]
    pub target: ::std::option::Option<analyze_request::Target>,
}
pub mod analyze_request {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Target {
        /// Single statement.
        #[prost(string, tag = "5")]
        SqlStatement(std::string::String),
        /// Multiple statement.
        #[prost(message, tag = "6")]
        ParseResumeLocation(super::super::ParseResumeLocationProto),
        /// Set if using a registered parse resume location.
        #[prost(message, tag = "7")]
        RegisteredParseResumeLocation(super::RegisteredParseResumeLocationProto),
        /// Expression.
        #[prost(string, tag = "8")]
        SqlExpression(std::string::String),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AnalyzeResponse {
    /// Set only if the request had parse_resume_location.
    #[prost(int32, optional, tag = "2")]
    pub resume_byte_position: ::std::option::Option<i32>,
    #[prost(oneof = "analyze_response::Result", tags = "1, 3")]
    pub result: ::std::option::Option<analyze_response::Result>,
}
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
    pub simple_catalog: ::std::option::Option<super::SimpleCatalogProto>,
    #[prost(message, repeated, tag = "2")]
    pub file_descriptor_set: ::std::vec::Vec<::prost_types::FileDescriptorSet>,
    /// Set if using a registered catalog, in which case simple_catalog and
    /// file_descriptor_set will be ignored.
    #[prost(int64, optional, tag = "3")]
    pub registered_catalog_id: ::std::option::Option<i64>,
    #[prost(oneof = "build_sql_request::Target", tags = "4, 5")]
    pub target: ::std::option::Option<build_sql_request::Target>,
}
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
    pub sql: ::std::option::Option<std::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExtractTableNamesFromStatementRequest {
    #[prost(string, optional, tag = "1")]
    pub sql_statement: ::std::option::Option<std::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExtractTableNamesFromStatementResponse {
    #[prost(message, repeated, tag = "1")]
    pub table_name: ::std::vec::Vec<extract_table_names_from_statement_response::TableName>,
}
pub mod extract_table_names_from_statement_response {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct TableName {
        #[prost(string, repeated, tag = "1")]
        pub table_name_segment: ::std::vec::Vec<std::string::String>,
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
    pub options: ::std::option::Option<super::LanguageOptionsProto>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExtractTableNamesFromNextStatementResponse {
    #[prost(message, repeated, tag = "1")]
    pub table_name: ::std::vec::Vec<extract_table_names_from_next_statement_response::TableName>,
    #[prost(int32, optional, tag = "2")]
    pub resume_byte_position: ::std::option::Option<i32>,
}
pub mod extract_table_names_from_next_statement_response {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct TableName {
        #[prost(string, repeated, tag = "1")]
        pub table_name_segment: ::std::vec::Vec<std::string::String>,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FormatSqlRequest {
    #[prost(string, optional, tag = "1")]
    pub sql: ::std::option::Option<std::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FormatSqlResponse {
    #[prost(string, optional, tag = "1")]
    pub sql: ::std::option::Option<std::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RegisterCatalogRequest {
    #[prost(message, optional, tag = "1")]
    pub simple_catalog: ::std::option::Option<super::SimpleCatalogProto>,
    #[prost(message, repeated, tag = "2")]
    pub file_descriptor_set: ::std::vec::Vec<::prost_types::FileDescriptorSet>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RegisterResponse {
    #[prost(int64, optional, tag = "1")]
    pub registered_id: ::std::option::Option<i64>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UnregisterRequest {
    #[prost(int64, optional, tag = "1")]
    pub registered_id: ::std::option::Option<i64>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetBuiltinFunctionsResponse {
    /// No file_descriptor_set returned. For now, only Datetime functions
    /// have arguments of enum type which need to be added manually when
    /// deserializing.
    #[prost(message, repeated, tag = "1")]
    pub function: ::std::vec::Vec<super::FunctionProto>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AddSimpleTableRequest {
    #[prost(int64, optional, tag = "1")]
    pub registered_catalog_id: ::std::option::Option<i64>,
    #[prost(message, optional, tag = "2")]
    pub table: ::std::option::Option<super::SimpleTableProto>,
    #[prost(message, repeated, tag = "3")]
    pub file_descriptor_set: ::std::vec::Vec<::prost_types::FileDescriptorSet>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LanguageOptionsRequest {
    #[prost(bool, optional, tag = "1")]
    pub maximum_features: ::std::option::Option<bool>,
    #[prost(enumeration = "super::LanguageVersion", optional, tag = "2")]
    pub language_version: ::std::option::Option<i32>,
}
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
    }
    impl<T: Clone> Clone for ZetaSqlLocalServiceClient<T> {
        fn clone(&self) -> Self {
            Self {
                inner: self.inner.clone(),
            }
        }
    }
}
#[doc = r" Generated server implementations."]
pub mod zeta_sql_local_service_server {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with ZetaSqlLocalServiceServer."]
    #[async_trait]
    pub trait ZetaSqlLocalService: Send + Sync + 'static {
        #[doc = " Prepare the sql expression in PrepareRequest with given columns and"]
        #[doc = " parameters with zetasql::PreparedExpression and return the result"]
        #[doc = " type as PrepareResponse. The prepared expression will be kept at server"]
        #[doc = " side which can be referred to with the returned id."]
        async fn prepare(
            &self,
            request: tonic::Request<super::PrepareRequest>,
        ) -> Result<tonic::Response<super::PrepareResponse>, tonic::Status>;
        #[doc = " Evaluate the prepared expression in EvaluateRequest with given columns and"]
        #[doc = " parameters with zetasql::PreparedExpression and return the result"]
        #[doc = " and value as EvaluateResponse."]
        async fn evaluate(
            &self,
            request: tonic::Request<super::EvaluateRequest>,
        ) -> Result<tonic::Response<super::EvaluateResponse>, tonic::Status>;
        #[doc = " Cleanup the prepared expression kept at server side with given id."]
        async fn unprepare(
            &self,
            request: tonic::Request<super::UnprepareRequest>,
        ) -> Result<tonic::Response<()>, tonic::Status>;
        #[doc = " Get a table schema from a proto."]
        async fn get_table_from_proto(
            &self,
            request: tonic::Request<super::TableFromProtoRequest>,
        ) -> Result<tonic::Response<super::super::SimpleTableProto>, tonic::Status>;
        #[doc = " Register a catalog at server side so that it can be reused."]
        async fn register_catalog(
            &self,
            request: tonic::Request<super::RegisterCatalogRequest>,
        ) -> Result<tonic::Response<super::RegisterResponse>, tonic::Status>;
        #[doc = " Register a parse resume location"]
        async fn register_parse_resume_location(
            &self,
            request: tonic::Request<super::super::ParseResumeLocationProto>,
        ) -> Result<tonic::Response<super::RegisterResponse>, tonic::Status>;
        #[doc = " Analyze a SQL statement, return the resolved AST and an optional byte"]
        #[doc = " position if end of input is not yet reached."]
        async fn analyze(
            &self,
            request: tonic::Request<super::AnalyzeRequest>,
        ) -> Result<tonic::Response<super::AnalyzeResponse>, tonic::Status>;
        #[doc = " Build a SQL statement or expression from a resolved AST."]
        async fn build_sql(
            &self,
            request: tonic::Request<super::BuildSqlRequest>,
        ) -> Result<tonic::Response<super::BuildSqlResponse>, tonic::Status>;
        #[doc = " Validate statement and extract table names."]
        async fn extract_table_names_from_statement(
            &self,
            request: tonic::Request<super::ExtractTableNamesFromStatementRequest>,
        ) -> Result<tonic::Response<super::ExtractTableNamesFromStatementResponse>, tonic::Status>;
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
        async fn extract_table_names_from_next_statement(
            &self,
            request: tonic::Request<super::ExtractTableNamesFromNextStatementRequest>,
        ) -> Result<tonic::Response<super::ExtractTableNamesFromNextStatementResponse>, tonic::Status>;
        #[doc = " Format a SQL statement (see also (broken link))"]
        async fn format_sql(
            &self,
            request: tonic::Request<super::FormatSqlRequest>,
        ) -> Result<tonic::Response<super::FormatSqlResponse>, tonic::Status>;
        #[doc = " Format a SQL statement using the new lenient_formatter.h"]
        async fn lenient_format_sql(
            &self,
            request: tonic::Request<super::FormatSqlRequest>,
        ) -> Result<tonic::Response<super::FormatSqlResponse>, tonic::Status>;
        #[doc = " Cleanup a registered catalog."]
        async fn unregister_catalog(
            &self,
            request: tonic::Request<super::UnregisterRequest>,
        ) -> Result<tonic::Response<()>, tonic::Status>;
        #[doc = " Cleanup a registered parse resume location."]
        async fn unregister_parse_resume_location(
            &self,
            request: tonic::Request<super::UnregisterRequest>,
        ) -> Result<tonic::Response<()>, tonic::Status>;
        #[doc = " Get ZetaSQL builtin functions specified by given options."]
        async fn get_builtin_functions(
            &self,
            request: tonic::Request<super::super::ZetaSqlBuiltinFunctionOptionsProto>,
        ) -> Result<tonic::Response<super::GetBuiltinFunctionsResponse>, tonic::Status>;
        #[doc = " Add SimpleTable"]
        async fn add_simple_table(
            &self,
            request: tonic::Request<super::AddSimpleTableRequest>,
        ) -> Result<tonic::Response<()>, tonic::Status>;
        #[doc = " Gets ZetaSQL lanauge options."]
        async fn get_language_options(
            &self,
            request: tonic::Request<super::LanguageOptionsRequest>,
        ) -> Result<tonic::Response<super::super::LanguageOptionsProto>, tonic::Status>;
    }
    #[doc = " Note that *all* service RPCs are locked down to only be authorized by"]
    #[doc = " the user that started the ZetaSql service.  This disallows random users"]
    #[doc = " from sending requests to this service to be executed on behalf of the"]
    #[doc = " user that started the service.  Given that this service does not currently"]
    #[doc = " provide access to external data, this lockdown is precautionary and"]
    #[doc = " conservative.  But the lockdown will be necessary once the reference"]
    #[doc = " implementation is extended to execute full queries over external data,"]
    #[doc = " since we cannot allow random users to run queries over data accessible by"]
    #[doc = " the (different) user that started the ZetaSql service."]
    #[derive(Debug)]
    #[doc(hidden)]
    pub struct ZetaSqlLocalServiceServer<T: ZetaSqlLocalService> {
        inner: _Inner<T>,
    }
    struct _Inner<T>(Arc<T>, Option<tonic::Interceptor>);
    impl<T: ZetaSqlLocalService> ZetaSqlLocalServiceServer<T> {
        pub fn new(inner: T) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner, None);
            Self { inner }
        }
        pub fn with_interceptor(inner: T, interceptor: impl Into<tonic::Interceptor>) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner, Some(interceptor.into()));
            Self { inner }
        }
    }
    impl<T: ZetaSqlLocalService> Service<http::Request<HyperBody>> for ZetaSqlLocalServiceServer<T> {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = Never;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<HyperBody>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/zetasql.local_service.ZetaSqlLocalService/Prepare" => {
                    struct PrepareSvc<T: ZetaSqlLocalService>(pub Arc<T>);
                    impl<T: ZetaSqlLocalService> tonic::server::UnaryService<super::PrepareRequest> for PrepareSvc<T> {
                        type Response = super::PrepareResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::PrepareRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { inner.prepare(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = PrepareSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/zetasql.local_service.ZetaSqlLocalService/Evaluate" => {
                    struct EvaluateSvc<T: ZetaSqlLocalService>(pub Arc<T>);
                    impl<T: ZetaSqlLocalService> tonic::server::UnaryService<super::EvaluateRequest>
                        for EvaluateSvc<T>
                    {
                        type Response = super::EvaluateResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::EvaluateRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { inner.evaluate(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = EvaluateSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/zetasql.local_service.ZetaSqlLocalService/Unprepare" => {
                    struct UnprepareSvc<T: ZetaSqlLocalService>(pub Arc<T>);
                    impl<T: ZetaSqlLocalService>
                        tonic::server::UnaryService<super::UnprepareRequest> for UnprepareSvc<T>
                    {
                        type Response = ();
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::UnprepareRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { inner.unprepare(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = UnprepareSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/zetasql.local_service.ZetaSqlLocalService/GetTableFromProto" => {
                    struct GetTableFromProtoSvc<T: ZetaSqlLocalService>(pub Arc<T>);
                    impl<T: ZetaSqlLocalService>
                        tonic::server::UnaryService<super::TableFromProtoRequest>
                        for GetTableFromProtoSvc<T>
                    {
                        type Response = super::super::SimpleTableProto;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::TableFromProtoRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { inner.get_table_from_proto(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = GetTableFromProtoSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/zetasql.local_service.ZetaSqlLocalService/RegisterCatalog" => {
                    struct RegisterCatalogSvc<T: ZetaSqlLocalService>(pub Arc<T>);
                    impl<T: ZetaSqlLocalService>
                        tonic::server::UnaryService<super::RegisterCatalogRequest>
                        for RegisterCatalogSvc<T>
                    {
                        type Response = super::RegisterResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::RegisterCatalogRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { inner.register_catalog(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = RegisterCatalogSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/zetasql.local_service.ZetaSqlLocalService/RegisterParseResumeLocation" => {
                    struct RegisterParseResumeLocationSvc<T: ZetaSqlLocalService>(pub Arc<T>);
                    impl<T: ZetaSqlLocalService>
                        tonic::server::UnaryService<super::super::ParseResumeLocationProto>
                        for RegisterParseResumeLocationSvc<T>
                    {
                        type Response = super::RegisterResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::super::ParseResumeLocationProto>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut =
                                async move { inner.register_parse_resume_location(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = RegisterParseResumeLocationSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/zetasql.local_service.ZetaSqlLocalService/Analyze" => {
                    struct AnalyzeSvc<T: ZetaSqlLocalService>(pub Arc<T>);
                    impl<T: ZetaSqlLocalService> tonic::server::UnaryService<super::AnalyzeRequest> for AnalyzeSvc<T> {
                        type Response = super::AnalyzeResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::AnalyzeRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { inner.analyze(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = AnalyzeSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/zetasql.local_service.ZetaSqlLocalService/BuildSql" => {
                    struct BuildSqlSvc<T: ZetaSqlLocalService>(pub Arc<T>);
                    impl<T: ZetaSqlLocalService> tonic::server::UnaryService<super::BuildSqlRequest>
                        for BuildSqlSvc<T>
                    {
                        type Response = super::BuildSqlResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::BuildSqlRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { inner.build_sql(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = BuildSqlSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/zetasql.local_service.ZetaSqlLocalService/ExtractTableNamesFromStatement" => {
                    struct ExtractTableNamesFromStatementSvc<T: ZetaSqlLocalService>(pub Arc<T>);
                    impl<T: ZetaSqlLocalService>
                        tonic::server::UnaryService<super::ExtractTableNamesFromStatementRequest>
                        for ExtractTableNamesFromStatementSvc<T>
                    {
                        type Response = super::ExtractTableNamesFromStatementResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ExtractTableNamesFromStatementRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                inner.extract_table_names_from_statement(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = ExtractTableNamesFromStatementSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/zetasql.local_service.ZetaSqlLocalService/ExtractTableNamesFromNextStatement" => {
                    struct ExtractTableNamesFromNextStatementSvc<T: ZetaSqlLocalService>(
                        pub Arc<T>,
                    );
                    impl<T: ZetaSqlLocalService>
                        tonic::server::UnaryService<
                            super::ExtractTableNamesFromNextStatementRequest,
                        > for ExtractTableNamesFromNextStatementSvc<T>
                    {
                        type Response = super::ExtractTableNamesFromNextStatementResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                super::ExtractTableNamesFromNextStatementRequest,
                            >,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                inner.extract_table_names_from_next_statement(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = ExtractTableNamesFromNextStatementSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/zetasql.local_service.ZetaSqlLocalService/FormatSql" => {
                    struct FormatSqlSvc<T: ZetaSqlLocalService>(pub Arc<T>);
                    impl<T: ZetaSqlLocalService>
                        tonic::server::UnaryService<super::FormatSqlRequest> for FormatSqlSvc<T>
                    {
                        type Response = super::FormatSqlResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::FormatSqlRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { inner.format_sql(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = FormatSqlSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/zetasql.local_service.ZetaSqlLocalService/LenientFormatSql" => {
                    struct LenientFormatSqlSvc<T: ZetaSqlLocalService>(pub Arc<T>);
                    impl<T: ZetaSqlLocalService>
                        tonic::server::UnaryService<super::FormatSqlRequest>
                        for LenientFormatSqlSvc<T>
                    {
                        type Response = super::FormatSqlResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::FormatSqlRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { inner.lenient_format_sql(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = LenientFormatSqlSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/zetasql.local_service.ZetaSqlLocalService/UnregisterCatalog" => {
                    struct UnregisterCatalogSvc<T: ZetaSqlLocalService>(pub Arc<T>);
                    impl<T: ZetaSqlLocalService>
                        tonic::server::UnaryService<super::UnregisterRequest>
                        for UnregisterCatalogSvc<T>
                    {
                        type Response = ();
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::UnregisterRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { inner.unregister_catalog(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = UnregisterCatalogSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/zetasql.local_service.ZetaSqlLocalService/UnregisterParseResumeLocation" => {
                    struct UnregisterParseResumeLocationSvc<T: ZetaSqlLocalService>(pub Arc<T>);
                    impl<T: ZetaSqlLocalService>
                        tonic::server::UnaryService<super::UnregisterRequest>
                        for UnregisterParseResumeLocationSvc<T>
                    {
                        type Response = ();
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::UnregisterRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                inner.unregister_parse_resume_location(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = UnregisterParseResumeLocationSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/zetasql.local_service.ZetaSqlLocalService/GetBuiltinFunctions" => {
                    struct GetBuiltinFunctionsSvc<T: ZetaSqlLocalService>(pub Arc<T>);
                    impl<T: ZetaSqlLocalService>
                        tonic::server::UnaryService<
                            super::super::ZetaSqlBuiltinFunctionOptionsProto,
                        > for GetBuiltinFunctionsSvc<T>
                    {
                        type Response = super::GetBuiltinFunctionsResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                super::super::ZetaSqlBuiltinFunctionOptionsProto,
                            >,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { inner.get_builtin_functions(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = GetBuiltinFunctionsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/zetasql.local_service.ZetaSqlLocalService/AddSimpleTable" => {
                    struct AddSimpleTableSvc<T: ZetaSqlLocalService>(pub Arc<T>);
                    impl<T: ZetaSqlLocalService>
                        tonic::server::UnaryService<super::AddSimpleTableRequest>
                        for AddSimpleTableSvc<T>
                    {
                        type Response = ();
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::AddSimpleTableRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { inner.add_simple_table(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = AddSimpleTableSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/zetasql.local_service.ZetaSqlLocalService/GetLanguageOptions" => {
                    struct GetLanguageOptionsSvc<T: ZetaSqlLocalService>(pub Arc<T>);
                    impl<T: ZetaSqlLocalService>
                        tonic::server::UnaryService<super::LanguageOptionsRequest>
                        for GetLanguageOptionsSvc<T>
                    {
                        type Response = super::super::LanguageOptionsProto;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::LanguageOptionsRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { inner.get_language_options(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = GetLanguageOptionsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => Box::pin(async move {
                    Ok(http::Response::builder()
                        .status(200)
                        .header("grpc-status", "12")
                        .body(tonic::body::BoxBody::empty())
                        .unwrap())
                }),
            }
        }
    }
    impl<T: ZetaSqlLocalService> Clone for ZetaSqlLocalServiceServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self { inner }
        }
    }
    impl<T: ZetaSqlLocalService> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone(), self.1.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: ZetaSqlLocalService> tonic::transport::NamedService for ZetaSqlLocalServiceServer<T> {
        const NAME: &'static str = "zetasql.local_service.ZetaSqlLocalService";
    }
}
