//! Shared normalization for user-supplied entity names.

/// The trimmed name, or `None` when blank.
pub(crate) fn normalized_name(name: &str) -> Option<String> {
    let name = name.trim();
    (!name.is_empty()).then(|| name.to_owned())
}
