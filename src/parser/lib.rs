use std::cell::Cell;
use tokio::runtime::Runtime;
use tonic::Status;
use zetasql::local_service::zeta_sql_local_service_client::ZetaSqlLocalServiceClient;
use zetasql::local_service::{analyze_request, analyze_response, AnalyzeRequest};
use zetasql::{
    AnyResolvedStatementProto, FunctionSignatureId, LanguageFeature, LanguageOptionsProto,
    ParseResumeLocationProto, SimpleCatalogProto, ZetaSqlBuiltinFunctionOptionsProto,
};

mod convert;
mod int128;
mod server;

mod lib_tests;

pub struct ParseProvider {
    runtime: Runtime,
}

impl ParseProvider {
    pub fn new() -> ParseProvider {
        server::create_zetasql_server();
        let mut runtime = Runtime::new().expect("runtime failed to start");
        ParseProvider { runtime }
    }

    pub fn parse(
        &mut self,
        sql: &str,
        offset: i32,
        catalog: SimpleCatalogProto, // TODO eliminate catalog in favor of rocksdb reference
    ) -> Result<(i32, node::Plan), String> {
        let future = ParseProvider::analyze(sql, offset, catalog);
        let result = self.runtime.block_on(future);
        match result {
            Ok((offset, stmt)) => Ok((offset, convert::convert(stmt))),
            Err(status) => Err(String::from(status.message())),
        }
    }

    async fn analyze(
        sql: &str,
        offset: i32,
        catalog: SimpleCatalogProto,
    ) -> Result<(i32, AnyResolvedStatementProto), Status> {
        let mut client = ZetaSqlLocalServiceClient::connect("http://127.0.0.1:50051")
            .await
            .expect("client failed to connect");
        let request = tonic::Request::new(AnalyzeRequest {
            simple_catalog: Some(catalog),
            target: Some(analyze_request::Target::ParseResumeLocation(
                ParseResumeLocationProto {
                    input: Some(String::from(sql)),
                    byte_position: Some(offset),
                    ..Default::default()
                },
            )),
            ..Default::default()
        });
        let response = client.analyze(request).await?.into_inner();
        let offset = response.resume_byte_position.unwrap_or(-1);
        let statement = match response.result.unwrap() {
            analyze_response::Result::ResolvedStatement(statement) => statement,
            _ => panic!("expression"),
        };
        Ok((offset, statement))
    }
}

pub fn catalog() -> SimpleCatalogProto {
    SimpleCatalogProto {
        builtin_function_options: Some(ZetaSqlBuiltinFunctionOptionsProto {
            language_options: Some(LanguageOptionsProto {
                enabled_language_features: enabled_features(),
                ..Default::default()
            }),
            include_function_ids: enabled_functions(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn enabled_features() -> Vec<i32> {
    vec![
        LanguageFeature::FeatureTimestampNanos as i32,
        LanguageFeature::FeatureDmlUpdateWithJoin as i32,
        LanguageFeature::FeatureCreateTablePartitionBy as i32,
        LanguageFeature::FeatureCreateTableClusterBy as i32,
        LanguageFeature::FeatureNumericType as i32,
        LanguageFeature::FeatureCreateTableFieldAnnotations as i32,
        LanguageFeature::FeatureCreateTableAsSelectColumnList as i32,
        LanguageFeature::FeatureDisallowNullPrimaryKeys as i32,
        LanguageFeature::FeatureDisallowPrimaryKeyUpdates as i32,
        LanguageFeature::FeatureParametersInGranteeList as i32,
        LanguageFeature::FeatureNamedArguments as i32,
        LanguageFeature::FeatureV11SelectStarExceptReplace as i32,
        LanguageFeature::FeatureV12CorrelatedRefsInNestedDml as i32,
        LanguageFeature::FeatureV12WeekWithWeekday as i32,
        LanguageFeature::FeatureV13OmitInsertColumnList as i32,
        LanguageFeature::FeatureV13NullsFirstLastInOrderBy as i32,
        LanguageFeature::FeatureV13ConcatMixedTypes as i32,
    ]
}

fn enabled_functions() -> Vec<i32> {
    vec![
        FunctionSignatureId::FnAddDouble as i32,
        FunctionSignatureId::FnAddInt64 as i32,
        FunctionSignatureId::FnAddNumeric as i32,
        FunctionSignatureId::FnAnd as i32,
        FunctionSignatureId::FnCaseNoValue as i32,
        FunctionSignatureId::FnCaseWithValue as i32,
        FunctionSignatureId::FnDivideDouble as i32,
        FunctionSignatureId::FnDivideNumeric as i32,
        FunctionSignatureId::FnGreater as i32,
        FunctionSignatureId::FnGreaterOrEqual as i32,
        FunctionSignatureId::FnLess as i32,
        FunctionSignatureId::FnLessOrEqual as i32,
        FunctionSignatureId::FnEqual as i32,
        FunctionSignatureId::FnStringLike as i32,
        FunctionSignatureId::FnIn as i32,
        FunctionSignatureId::FnBetween as i32,
        FunctionSignatureId::FnIsNull as i32,
        FunctionSignatureId::FnMultiplyDouble as i32,
        FunctionSignatureId::FnMultiplyInt64 as i32,
        FunctionSignatureId::FnMultiplyNumeric as i32,
        FunctionSignatureId::FnNot as i32,
        FunctionSignatureId::FnNotEqual as i32,
        FunctionSignatureId::FnOr as i32,
        FunctionSignatureId::FnSubtractDouble as i32,
        FunctionSignatureId::FnSubtractInt64 as i32,
        FunctionSignatureId::FnSubtractNumeric as i32,
        FunctionSignatureId::FnUnaryMinusInt64 as i32,
        FunctionSignatureId::FnUnaryMinusDouble as i32,
        FunctionSignatureId::FnUnaryMinusNumeric as i32,
        // Bitwise unary operators.
        FunctionSignatureId::FnBitwiseNotInt64 as i32,
        // Bitwise binary operators.
        FunctionSignatureId::FnBitwiseOrInt64 as i32,
        FunctionSignatureId::FnBitwiseXorInt64 as i32,
        FunctionSignatureId::FnBitwiseAndInt64 as i32,
        // For all bitwise shift operators, the second argument has int64 type as i32.
        // Expected behavior of bitwise shift operations:
        // * Shifting by a negative offset is an error.
        // * Shifting right on signed values does not do sign extension.
        FunctionSignatureId::FnBitwiseLeftShiftInt64 as i32,
        FunctionSignatureId::FnBitwiseRightShiftInt64 as i32,
        // BITCOUNT functions.
        FunctionSignatureId::FnBitCountInt64 as i32,
        // String functions
        FunctionSignatureId::FnConcatString as i32,
        FunctionSignatureId::FnStrposString as i32,
        FunctionSignatureId::FnLowerString as i32,
        FunctionSignatureId::FnUpperString as i32,
        FunctionSignatureId::FnLengthString as i32,
        FunctionSignatureId::FnStartsWithString as i32,
        FunctionSignatureId::FnEndsWithString as i32,
        FunctionSignatureId::FnSubstrString as i32,
        FunctionSignatureId::FnTrimString as i32,
        FunctionSignatureId::FnLtrimString as i32,
        FunctionSignatureId::FnRtrimString as i32,
        FunctionSignatureId::FnReplaceString as i32,
        FunctionSignatureId::FnRegexpExtractString as i32,
        FunctionSignatureId::FnRegexpReplaceString as i32,
        FunctionSignatureId::FnRegexpExtractAllString as i32,
        FunctionSignatureId::FnByteLengthString as i32,
        FunctionSignatureId::FnCharLengthString as i32,
        FunctionSignatureId::FnSplitString as i32,
        FunctionSignatureId::FnRegexpContainsString as i32,
        FunctionSignatureId::FnToBase64 as i32,
        FunctionSignatureId::FnFromBase64 as i32,
        FunctionSignatureId::FnToHex as i32,
        FunctionSignatureId::FnFromHex as i32,
        FunctionSignatureId::FnLpadString as i32,
        FunctionSignatureId::FnRpadString as i32,
        FunctionSignatureId::FnRepeatString as i32,
        FunctionSignatureId::FnReverseString as i32,
        // Control flow functions
        FunctionSignatureId::FnIf as i32,
        // Coalesce is used to express the output join column in FULL JOIN.
        FunctionSignatureId::FnCoalesce as i32,
        FunctionSignatureId::FnIfnull as i32,
        // Time functions
        FunctionSignatureId::FnDateAddDate as i32,
        FunctionSignatureId::FnTimestampAdd as i32,
        FunctionSignatureId::FnDateDiffDate as i32,
        FunctionSignatureId::FnTimestampDiff as i32,
        FunctionSignatureId::FnDateSubDate as i32,
        FunctionSignatureId::FnTimestampSub as i32,
        FunctionSignatureId::FnDateTruncDate as i32,
        FunctionSignatureId::FnTimestampTrunc as i32,
        FunctionSignatureId::FnDateFromUnixDate as i32,
        FunctionSignatureId::FnTimestampFromInt64Seconds as i32,
        FunctionSignatureId::FnTimestampFromInt64Millis as i32,
        FunctionSignatureId::FnTimestampFromInt64Micros as i32,
        FunctionSignatureId::FnTimestampFromUnixSecondsInt64 as i32,
        FunctionSignatureId::FnTimestampFromUnixSecondsTimestamp as i32,
        FunctionSignatureId::FnTimestampFromUnixMillisInt64 as i32,
        FunctionSignatureId::FnTimestampFromUnixMillisTimestamp as i32,
        FunctionSignatureId::FnTimestampFromUnixMicrosInt64 as i32,
        FunctionSignatureId::FnTimestampFromUnixMicrosTimestamp as i32,
        FunctionSignatureId::FnUnixDate as i32,
        FunctionSignatureId::FnUnixSecondsFromTimestamp as i32,
        FunctionSignatureId::FnUnixMillisFromTimestamp as i32,
        FunctionSignatureId::FnUnixMicrosFromTimestamp as i32,
        FunctionSignatureId::FnDateFromYearMonthDay as i32,
        FunctionSignatureId::FnTimestampFromString as i32,
        // Signatures for extracting date parts, taking a date/timestam as i32p
        // and the target date part as arguments.
        FunctionSignatureId::FnExtractFromDate as i32,
        FunctionSignatureId::FnExtractFromTimestamp as i32,
        // TIMESTAMP.
        FunctionSignatureId::FnExtractDateFromTimestamp as i32,
        // Math functions
        FunctionSignatureId::FnAbsInt64 as i32,
        FunctionSignatureId::FnAbsDouble as i32,
        FunctionSignatureId::FnAbsNumeric as i32,
        FunctionSignatureId::FnSignInt64 as i32,
        FunctionSignatureId::FnSignDouble as i32,
        FunctionSignatureId::FnSignNumeric as i32,
        FunctionSignatureId::FnRoundDouble as i32,
        FunctionSignatureId::FnRoundNumeric as i32,
        FunctionSignatureId::FnRoundWithDigitsDouble as i32,
        FunctionSignatureId::FnRoundWithDigitsNumeric as i32,
        FunctionSignatureId::FnTruncDouble as i32,
        FunctionSignatureId::FnTruncNumeric as i32,
        FunctionSignatureId::FnTruncWithDigitsDouble as i32,
        FunctionSignatureId::FnTruncWithDigitsNumeric as i32,
        FunctionSignatureId::FnCeilDouble as i32,
        FunctionSignatureId::FnCeilNumeric as i32,
        FunctionSignatureId::FnFloorDouble as i32,
        FunctionSignatureId::FnFloorNumeric as i32,
        FunctionSignatureId::FnModInt64 as i32,
        FunctionSignatureId::FnModNumeric as i32,
        FunctionSignatureId::FnDivInt64 as i32,
        FunctionSignatureId::FnDivNumeric as i32,
        FunctionSignatureId::FnIsInf as i32,
        FunctionSignatureId::FnIsNan as i32,
        FunctionSignatureId::FnGreatest as i32,
        FunctionSignatureId::FnLeast as i32,
        FunctionSignatureId::FnSqrtDouble as i32,
        FunctionSignatureId::FnPowDouble as i32,
        FunctionSignatureId::FnPowNumeric as i32,
        FunctionSignatureId::FnExpDouble as i32,
        FunctionSignatureId::FnNaturalLogarithmDouble as i32,
        FunctionSignatureId::FnDecimalLogarithmDouble as i32,
        FunctionSignatureId::FnLogarithmDouble as i32,
        FunctionSignatureId::FnCosDouble as i32,
        FunctionSignatureId::FnCoshDouble as i32,
        FunctionSignatureId::FnAcosDouble as i32,
        FunctionSignatureId::FnAcoshDouble as i32,
        FunctionSignatureId::FnSinDouble as i32,
        FunctionSignatureId::FnSinhDouble as i32,
        FunctionSignatureId::FnAsinDouble as i32,
        FunctionSignatureId::FnAsinhDouble as i32,
        FunctionSignatureId::FnTanDouble as i32,
        FunctionSignatureId::FnTanhDouble as i32,
        FunctionSignatureId::FnAtanDouble as i32,
        FunctionSignatureId::FnAtanhDouble as i32,
        FunctionSignatureId::FnAtan2Double as i32,
        // Non-deterministic functions
        FunctionSignatureId::FnCurrentDate as i32,
        FunctionSignatureId::FnCurrentTimestamp as i32,
        FunctionSignatureId::FnRand as i32,
        // Aggregate functions
        // TODO these are still not implemented in compiler
        FunctionSignatureId::FnCountStar as i32,
        FunctionSignatureId::FnAnyValue as i32,
        FunctionSignatureId::FnArrayAgg as i32,
        FunctionSignatureId::FnArrayConcatAgg as i32,
        FunctionSignatureId::FnAvgInt64 as i32,
        FunctionSignatureId::FnAvgDouble as i32,
        FunctionSignatureId::FnAvgNumeric as i32,
        FunctionSignatureId::FnCount as i32,
        FunctionSignatureId::FnMax as i32,
        FunctionSignatureId::FnMin as i32,
        FunctionSignatureId::FnStringAggString as i32,
        FunctionSignatureId::FnStringAggDelimString as i32,
        FunctionSignatureId::FnStringAggBytes as i32,
        FunctionSignatureId::FnStringAggDelimBytes as i32,
        FunctionSignatureId::FnSumInt64 as i32,
        FunctionSignatureId::FnSumDouble as i32,
        FunctionSignatureId::FnSumNumeric as i32,
        FunctionSignatureId::FnBitAndInt64 as i32,
        FunctionSignatureId::FnBitOrInt64 as i32,
        FunctionSignatureId::FnBitXorInt64 as i32,
        FunctionSignatureId::FnLogicalAnd as i32,
        FunctionSignatureId::FnLogicalOr as i32,
    ]
}
