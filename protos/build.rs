fn main() {
    println!("cargo:rerun-if-changed={}", "./rpc.proto");
    grpcio_compiler::prost_codegen::compile_protos(&["./rpc.proto"], &["."], ".").unwrap();
}
