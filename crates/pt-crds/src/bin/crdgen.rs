//! Write every CRD to `deploy/crds/<plural>.yaml`, or print them with `--stdout`.
//!
//! `cargo run -p pt-crds --bin crdgen` from the repo root. A test fails if the committed
//! files drift from the Rust types.

use std::path::PathBuf;

fn main() -> anyhow::Result<()> {
    let crds = pt_crds::all_crds();
    if std::env::args().any(|a| a == "--stdout") {
        for crd in &crds {
            print!("---\n{}", pt_crds::crd_yaml(crd)?);
        }
        return Ok(());
    }
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../deploy/crds");
    std::fs::create_dir_all(&dir)?;
    for crd in &crds {
        let path = dir.join(pt_crds::crd_file_name(crd));
        std::fs::write(&path, pt_crds::crd_yaml(crd)?)?;
        println!("wrote {}", path.canonicalize()?.display());
    }
    Ok(())
}
