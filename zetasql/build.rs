fn main() {
    println!("cargo:rerun-if-changed={}", "./protos");
    grpcio_compiler::prost_codegen::compile_protos(
        &["./protos/zetasql/local_service/local_service.proto"],
        &["./protos"],
        ".",
    )
    .unwrap();
    std::fs::rename("./zetasql.local_service.rs", "./local_service.rs").unwrap();
    std::fs::remove_file("./google.protobuf.rs").unwrap();
}
