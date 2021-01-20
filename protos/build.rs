fn main() {
    tonic_build::compile_protos("../protos/helloworld.proto").unwrap();
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .out_dir(".")
        .compile(&["../protos/helloworld.proto"], &["../protos"])
        .unwrap();
}
