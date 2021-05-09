fn main() {
    println!("cargo:rerun-if-changed={}", "./rpc.proto");
    tonic_build::configure()
        .out_dir(".")
        .compile(&["./rpc.proto"], &["."])
        .unwrap();
}
