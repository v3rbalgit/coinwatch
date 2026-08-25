//! Rebuild the crate whenever embedded SQL migrations change.

fn main() {
    println!("cargo:rerun-if-changed=migrations");
}
