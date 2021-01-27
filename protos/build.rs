fn main() {
    let mut config = prost_build::Config::new();
    config.protoc_arg("--experimental_allow_proto3_optional");
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .out_dir(".")
        .compile_with_config(config, &["../protos/worker.proto"], &["../protos"])
        .unwrap();
}
