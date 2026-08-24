//! Swing-pivot detection: local extremes that anchor divergence lines.

/// Whether a pivot is a local high or a local low.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PivotKind {
    /// A local maximum.
    High,
    /// A local minimum.
    Low,
}

/// A confirmed swing pivot.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Pivot {
    /// Position of the pivot in the input series.
    pub index: usize,
    /// Value at the pivot.
    pub value: f64,
    /// Whether it is a high or a low.
    pub kind: PivotKind,
}

/// Find swing pivots in `series`: a bar that is the strict extreme over `left`
/// bars before and `right` bars after. Pivots come back in ascending index order.
/// Because a pivot needs `right` bars of confirmation, the final `right` bars can
/// never produce one (no repainting).
#[must_use]
pub fn pivots(series: &[f64], left: usize, right: usize) -> Vec<Pivot> {
    let width = left + right + 1;
    if series.len() < width {
        return Vec::new();
    }

    let mut out = Vec::new();
    for (start, window) in series.windows(width).enumerate() {
        let Some(&center) = window.get(left) else { continue };
        let is_high = window
            .iter()
            .enumerate()
            .all(|(offset, &value)| offset == left || value < center);
        let is_low = window
            .iter()
            .enumerate()
            .all(|(offset, &value)| offset == left || value > center);
        if is_high {
            out.push(Pivot {
                index: start + left,
                value: center,
                kind: PivotKind::High,
            });
        } else if is_low {
            out.push(Pivot {
                index: start + left,
                value: center,
                kind: PivotKind::Low,
            });
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{PivotKind, pivots};

    #[test]
    fn finds_a_strict_high() {
        let ps = pivots(&[1.0, 2.0, 5.0, 2.0, 1.0], 2, 2);
        assert_eq!(ps.len(), 1);
        assert!(ps.iter().any(|p| p.index == 2 && p.kind == PivotKind::High));
    }

    #[test]
    fn finds_a_strict_low() {
        let ps = pivots(&[5.0, 4.0, 1.0, 4.0, 5.0], 2, 2);
        assert!(ps.iter().any(|p| p.index == 2 && p.kind == PivotKind::Low));
    }

    #[test]
    fn a_tie_is_not_a_pivot() {
        // The centre equals a neighbour, so it is neither a strict high nor low.
        assert!(pivots(&[1.0, 5.0, 5.0, 4.0, 1.0], 2, 2).is_empty());
    }

    #[test]
    fn too_short_yields_nothing() {
        assert!(pivots(&[1.0, 2.0], 2, 2).is_empty());
    }
}
