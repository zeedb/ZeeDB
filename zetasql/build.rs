fn main() {
    println!("cargo:rerun-if-changed={}", "./protos");
    // Create these files if they don't already exist.
    touch("./local_service.rs").unwrap();
    touch("./zetasql.rs").unwrap();
    // Generate protobuf rust implementations from definitions.
    tonic_build::configure()
        .build_server(false)
        .format(option_env!("NO_RUSTFMT") != Some("1"))
        .out_dir(".")
        .compile(
            &["./protos/zetasql/local_service/local_service.proto"],
            &["./protos"],
        )
        .unwrap();
    // Rename bad file names.
    std::fs::rename("./zetasql.local_service.rs", "./local_service.rs").unwrap();
    // This file is empty.
    std::fs::remove_file("./google.protobuf.rs").unwrap();
}

fn touch(path: impl AsRef<std::path::Path>) -> std::io::Result<()> {
    std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(path)
        .map(|_| ())
}
