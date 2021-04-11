fn main() {
    println!("cargo:rerun-if-changed={}", "../protos/rpc.proto");
    grpcio_compiler::prost_codegen::compile_protos(&["../protos/rpc.proto"], &["../protos"], ".")
        .unwrap();
}
