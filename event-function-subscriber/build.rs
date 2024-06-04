fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = prost_build::Config::new();
    config.protoc_arg("--experimental_allow_proto3_optional");
    tonic_build::configure().compile_with_config(config, &["../proto/actor_service.proto",
        "../proto/event_reception_service.proto", "../proto/event_subscriber.proto"], &["../proto"])?;
    Ok(())
}
