fn main() {
    println!("cargo:rerun-if-changed={}", "./rpc.proto");
    tonic_build::configure()
        .format(option_env!("NO_RUSTFMT") != Some("1"))
        .out_dir(".")
        .compile(&["./rpc.proto"], &["."])
        .unwrap();
}
