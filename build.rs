fn main() -> Result<(), Box<dyn std::error::Error>> {
    // tonic_build::compile_protos("proto/peering.proto")?;
    // tonic_build::compile_protos("proto/chord.proto")?;
    tonic_build::configure()
        .format(true)
        .compile(&["proto/chord/chord.proto"], &["proto/chord"])?;

    tonic_build::configure().compile(&["proto/peering/peering.proto"], &["proto/peering"])?;
    Ok(())
}
