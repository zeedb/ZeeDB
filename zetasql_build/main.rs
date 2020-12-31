fn main() {
    tonic_build::configure()
        .build_client(true)
        .build_server(false)
        .out_dir("./src/zetasql_build")
        .compile(
            &["src/zetasql_build/protos/zetasql/local_service/local_service.proto"],
            &["src/zetasql_build/protos"],
        )
        .unwrap();
    std::fs::rename(
        "./src/zetasql_build/zetasql.local_service.rs",
        "./src/zetasql/local_service.rs",
    )
    .unwrap();
    std::fs::rename("./src/zetasql_build/zetasql.rs", "./src/zetasql/zetasql.rs").unwrap();
    std::fs::remove_file("./src/zetasql_build/google.protobuf.rs").unwrap();
}
