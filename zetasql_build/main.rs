fn main() {
    tonic_build::configure()
        .build_client(true)
        .build_server(false)
        .out_dir("./zetasql_build")
        .compile(
            &["zetasql_build/protos/zetasql/local_service/local_service.proto"],
            &["zetasql_build/protos"],
        )
        .unwrap();
    std::fs::rename(
        "./zetasql_build/zetasql.local_service.rs",
        "./zetasql/local_service.rs",
    )
    .unwrap();
    std::fs::rename("./zetasql_build/zetasql.rs", "./zetasql/zetasql.rs").unwrap();
    std::fs::remove_file("./zetasql_build/google.protobuf.rs").unwrap();
}
