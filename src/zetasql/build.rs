fn main() -> Result<(), std::io::Error> {
    println!("cargo:rerun-if-changed=protos");
    tonic_build::configure()
        .build_client(true)
        .build_server(false)
        .compile(
            &["protos/zetasql/local_service/local_service.proto"],
            &["protos"],
        )?;
    std::fs::rename("zetasql.local_service.rs", "local_service.rs")?;
    std::fs::remove_file("google.protobuf.rs")?;
    Ok(())
}
