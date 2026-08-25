//! The persisted signal model: configured indicator conditions composed into a
//! boolean logic tree.
//!
//! Definitions use an explicitly tagged, camel-case JSON contract because the
//! same representation is stored in SQLite and edited by the frontend. Validation
//! resolves every tree leaf and checks indicator capabilities before evaluation.

use std::collections::{BTreeMap, BTreeSet};

use domain::Timeframe;
use indicators::{Descriptor, DescriptorKind, DivergenceDirection, DivergenceKind, Instance, descriptors};
use serde::{Deserialize, Serialize};

/// Current schema version of serialized signal definitions.
pub const DEFINITION_VERSION: u32 = 1;

/// Sanity bound for user-entered freshness windows. Evaluation remains bounded by
/// the hot cache, but rejecting absurd values catches corrupted definitions early.
const MAX_WITHIN_BARS: u32 = 10_000;

/// A definition validation failure.
#[derive(Debug, thiserror::Error)]
pub enum DefinitionError {
    /// The definition uses a schema version this build does not understand.
    #[error("unsupported definition version {0}")]
    UnsupportedVersion(u32),
    /// No conditions were defined.
    #[error("a signal needs at least one condition")]
    NoConditions,
    /// An `all` or `any` node has no children.
    #[error("{0} groups need at least one child")]
    EmptyGroup(&'static str),
    /// An XOR node cannot express exclusivity with fewer than two children.
    #[error("xor groups need at least two children")]
    XorNeedsTwoChildren,
    /// A tree leaf refers to a missing condition.
    #[error("tree references unknown condition '{0}'")]
    UnknownCondition(String),
    /// A stored condition is not reachable from the tree.
    #[error("condition '{0}' is not referenced by the tree")]
    UnreferencedCondition(String),
    /// A condition exceeds the supported freshness bound.
    #[error("condition '{condition}' exceeds the {maximum}-bar freshness window")]
    FreshnessTooLarge {
        /// Condition id.
        condition: String,
        /// Maximum accepted `withinBars` value.
        maximum: u32,
    },
    /// An indicator parameter is NaN or infinite.
    #[error("condition '{condition}' has a non-finite '{parameter}' parameter")]
    NonFiniteParameter {
        /// Condition id.
        condition: String,
        /// Parameter key.
        parameter: String,
    },
    /// A predicate constant is NaN or infinite.
    #[error("condition '{0}' has a non-finite constant")]
    NonFiniteConstant(String),
    /// The indicator id does not exist in the registry.
    #[error("condition '{condition}' references unknown indicator '{indicator}'")]
    UnknownIndicator {
        /// Condition id.
        condition: String,
        /// Indicator id.
        indicator: String,
    },
    /// A divergence predicate was assigned to a numeric-series indicator.
    #[error("condition '{condition}' uses a divergence predicate on series indicator '{indicator}'")]
    DivergenceRequiresDivergenceIndicator {
        /// Condition id.
        condition: String,
        /// Indicator id.
        indicator: String,
    },
    /// A series predicate was assigned to a divergence-only indicator.
    #[error("condition '{condition}' uses a series predicate on divergence indicator '{indicator}'")]
    SeriesRequiresSeriesIndicator {
        /// Condition id.
        condition: String,
        /// Indicator id.
        indicator: String,
    },
    /// An output operand does not exist on the selected indicator.
    #[error("condition '{condition}' references unknown output '{output}' of '{indicator}'")]
    UnknownOutput {
        /// Condition id.
        condition: String,
        /// Indicator id.
        indicator: String,
        /// Output key.
        output: String,
    },
    /// A series predicate never reads its selected indicator.
    #[error("condition '{0}' must reference at least one indicator output")]
    PredicateIgnoresIndicator(String),
}

/// Trading bias of a saved signal — user intent, shown in catalogs and results.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Direction {
    /// Setup to buy.
    Bullish,
    /// Setup to sell.
    Bearish,
}

/// Which series an operand reads from.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
pub enum Operand {
    /// One output of this condition's indicator instance.
    Output {
        /// Descriptor output key.
        key: String,
    },
    /// A raw OHLCV candle field.
    Field {
        /// Field to read.
        field: PriceField,
    },
    /// A literal number broadcast over the series.
    Constant {
        /// Literal value.
        value: f64,
    },
}

/// Raw OHLCV fields usable as operands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PriceField {
    /// Bar open price.
    Open,
    /// Bar high price.
    High,
    /// Bar low price.
    Low,
    /// Bar close price.
    Close,
    /// Bar volume.
    Volume,
}

/// Ordering comparisons for threshold predicates. Equality is intentionally
/// absent: exact float equality is not a meaningful market condition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ComparisonOp {
    /// Left strictly below right.
    LessThan,
    /// Left at or below right.
    LessOrEqual,
    /// Left strictly above right.
    GreaterThan,
    /// Left at or above right.
    GreaterOrEqual,
}

impl ComparisonOp {
    pub(crate) fn holds(self, left: f64, right: f64) -> bool {
        match self {
            Self::LessThan => left < right,
            Self::LessOrEqual => left <= right,
            Self::GreaterThan => left > right,
            Self::GreaterOrEqual => left >= right,
        }
    }
}

/// Direction of a cross event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CrossDirection {
    /// Left crosses from at-or-below to above right.
    Over,
    /// Left crosses from at-or-above to below right.
    Under,
}

/// Filter over divergence kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum KindFilter {
    /// Match regular and hidden divergences.
    Any,
    /// Match regular (reversal) divergences only.
    Regular,
    /// Match hidden (continuation) divergences only.
    Hidden,
}

impl KindFilter {
    pub(crate) const fn matches(self, kind: DivergenceKind) -> bool {
        matches!(self, Self::Any)
            || matches!(
                (self, kind),
                (Self::Regular, DivergenceKind::Regular) | (Self::Hidden, DivergenceKind::Hidden)
            )
    }
}

/// Filter over divergence directions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum DirectionFilter {
    /// Match bullish and bearish divergences.
    Any,
    /// Match bullish divergences only.
    Bullish,
    /// Match bearish divergences only.
    Bearish,
}

impl DirectionFilter {
    pub(crate) const fn matches(self, direction: DivergenceDirection) -> bool {
        matches!(self, Self::Any)
            || matches!(
                (self, direction),
                (Self::Bullish, DivergenceDirection::Bullish) | (Self::Bearish, DivergenceDirection::Bearish)
            )
    }
}

/// How a condition judges its indicator instance.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
pub enum Predicate {
    /// `left op right`, true wherever the comparison holds.
    Threshold {
        /// Left-hand operand.
        left: Operand,
        /// Comparison operator.
        op: ComparisonOp,
        /// Right-hand operand.
        right: Operand,
    },
    /// True exactly at bars where left crosses over/under right.
    Cross {
        /// Fast-side operand.
        left: Operand,
        /// Slow-side operand.
        right: Operand,
        /// Crossing direction.
        direction: CrossDirection,
    },
    /// True at confirming-pivot bars of matching divergences.
    Divergence {
        /// Which divergence kinds count.
        kind: KindFilter,
        /// Which divergence directions count.
        direction: DirectionFilter,
    },
}

/// One testable market condition. Its stable id is the key under which it appears
/// in [`SignalDefinition::conditions`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Condition {
    /// Timeframe whose candles the condition evaluates on.
    pub timeframe: Timeframe,
    /// Indicator instance (registry id + params).
    pub indicator: Instance,
    /// What the condition tests.
    pub predicate: Predicate,
    /// Bars of tolerance back from the latest completed bar (`0` = latest only).
    #[serde(default)]
    pub within_bars: u32,
}

/// Boolean composition tree over condition ids.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
pub enum Node {
    /// Every child must hold.
    All {
        /// Child expressions.
        children: Vec<Node>,
    },
    /// At least one child must hold.
    Any {
        /// Child expressions.
        children: Vec<Node>,
    },
    /// Negation of one child expression.
    Not {
        /// Child expression.
        child: Box<Node>,
    },
    /// An odd number of children must hold.
    Xor {
        /// Child expressions (at least two).
        children: Vec<Node>,
    },
    /// Leaf referencing a condition id.
    Condition {
        /// Key in [`SignalDefinition::conditions`].
        id: String,
    },
}

/// A persisted, composable signal definition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SignalDefinition {
    /// Schema version, see [`DEFINITION_VERSION`].
    pub version: u32,
    /// User-facing trading intent.
    pub direction: Direction,
    /// Composition root.
    pub root: Node,
    /// Conditions keyed by stable id.
    pub conditions: BTreeMap<String, Condition>,
}

impl SignalDefinition {
    /// Validate structure, registry membership, params, and predicate/indicator
    /// pairing before persistence or evaluation.
    pub fn validate(&self) -> Result<(), DefinitionError> {
        if self.version != DEFINITION_VERSION {
            return Err(DefinitionError::UnsupportedVersion(self.version));
        }
        if self.conditions.is_empty() {
            return Err(DefinitionError::NoConditions);
        }

        let registry = descriptors();
        for (id, condition) in &self.conditions {
            validate_condition(id, condition, registry)?;
        }

        let mut referenced = BTreeSet::new();
        validate_node(&self.root, &self.conditions, &mut referenced)?;
        if let Some(id) = self.conditions.keys().find(|id| !referenced.contains(*id)) {
            return Err(DefinitionError::UnreferencedCondition(id.clone()));
        }
        Ok(())
    }
}

fn validate_node(
    node: &Node,
    conditions: &BTreeMap<String, Condition>,
    referenced: &mut BTreeSet<String>,
) -> Result<(), DefinitionError> {
    match node {
        Node::All { children } | Node::Any { children } => {
            if children.is_empty() {
                let kind = if matches!(node, Node::All { .. }) { "all" } else { "any" };
                return Err(DefinitionError::EmptyGroup(kind));
            }
            for child in children {
                validate_node(child, conditions, referenced)?;
            }
        }
        Node::Xor { children } => {
            if children.len() < 2 {
                return Err(DefinitionError::XorNeedsTwoChildren);
            }
            for child in children {
                validate_node(child, conditions, referenced)?;
            }
        }
        Node::Not { child } => validate_node(child, conditions, referenced)?,
        Node::Condition { id } => {
            if !conditions.contains_key(id) {
                return Err(DefinitionError::UnknownCondition(id.clone()));
            }
            referenced.insert(id.clone());
        }
    }
    Ok(())
}

fn validate_condition(id: &str, condition: &Condition, registry: &[Descriptor]) -> Result<(), DefinitionError> {
    if condition.within_bars > MAX_WITHIN_BARS {
        return Err(DefinitionError::FreshnessTooLarge {
            condition: id.to_owned(),
            maximum: MAX_WITHIN_BARS,
        });
    }
    for (parameter, value) in &condition.indicator.params {
        if !value.is_finite() {
            return Err(DefinitionError::NonFiniteParameter {
                condition: id.to_owned(),
                parameter: parameter.clone(),
            });
        }
    }
    for operand in predicate_operands(&condition.predicate).into_iter().flatten() {
        if let Operand::Constant { value } = operand
            && !value.is_finite()
        {
            return Err(DefinitionError::NonFiniteConstant(id.to_owned()));
        }
    }

    let Some(descriptor) = registry.iter().find(|candidate| candidate.id == condition.indicator.id) else {
        return Err(DefinitionError::UnknownIndicator {
            condition: id.to_owned(),
            indicator: condition.indicator.id.clone(),
        });
    };

    match &condition.predicate {
        Predicate::Divergence { .. } => {
            if descriptor.kind != DescriptorKind::Divergence {
                return Err(DefinitionError::DivergenceRequiresDivergenceIndicator {
                    condition: id.to_owned(),
                    indicator: descriptor.id.clone(),
                });
            }
        }
        Predicate::Threshold { left, right, .. } | Predicate::Cross { left, right, .. } => {
            if descriptor.kind != DescriptorKind::Series {
                return Err(DefinitionError::SeriesRequiresSeriesIndicator {
                    condition: id.to_owned(),
                    indicator: descriptor.id.clone(),
                });
            }
            let operands = [left, right];
            if !operands.iter().any(|operand| matches!(operand, Operand::Output { .. })) {
                return Err(DefinitionError::PredicateIgnoresIndicator(id.to_owned()));
            }
            for operand in operands {
                let Operand::Output { key } = operand else { continue };
                if !descriptor.outputs.iter().any(|output| output.key == *key) {
                    return Err(DefinitionError::UnknownOutput {
                        condition: id.to_owned(),
                        indicator: descriptor.id.clone(),
                        output: key.clone(),
                    });
                }
            }
        }
    }
    Ok(())
}

const fn predicate_operands(predicate: &Predicate) -> [Option<&Operand>; 2] {
    match predicate {
        Predicate::Threshold { left, right, .. } | Predicate::Cross { left, right, .. } => [Some(left), Some(right)],
        Predicate::Divergence { .. } => [None, None],
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use domain::Timeframe;
    use indicators::Instance;
    use serde_json::json;

    use super::{
        ComparisonOp, Condition, DEFINITION_VERSION, DefinitionError, Direction, DirectionFilter, KindFilter, Node,
        Operand, Predicate, SignalDefinition,
    };

    fn threshold() -> Condition {
        Condition {
            timeframe: Timeframe::Hour1,
            indicator: Instance {
                id: "sma".to_owned(),
                params: [("period".to_owned(), 20.0)].into_iter().collect(),
            },
            predicate: Predicate::Threshold {
                left: Operand::Output { key: "sma".to_owned() },
                op: ComparisonOp::GreaterThan,
                right: Operand::Constant { value: 80.0 },
            },
            within_bars: 0,
        }
    }

    fn definition(root: Node) -> SignalDefinition {
        SignalDefinition {
            version: DEFINITION_VERSION,
            direction: Direction::Bearish,
            root,
            conditions: [("hot".to_owned(), threshold())].into_iter().collect(),
        }
    }

    #[test]
    fn validates_a_well_formed_definition() {
        let signal = definition(Node::Condition { id: "hot".to_owned() });
        assert!(signal.validate().is_ok());
    }

    #[test]
    fn rejects_unknown_and_unreferenced_conditions() {
        let unknown = definition(Node::Condition {
            id: "missing".to_owned(),
        });
        assert!(matches!(unknown.validate(), Err(DefinitionError::UnknownCondition(id)) if id == "missing"));

        let mut unreferenced = definition(Node::Condition { id: "hot".to_owned() });
        unreferenced.conditions.insert("unused".to_owned(), threshold());
        assert!(matches!(
            unreferenced.validate(),
            Err(DefinitionError::UnreferencedCondition(id)) if id == "unused"
        ));
    }

    #[test]
    fn rejects_predicates_incompatible_with_indicator_capability() {
        let divergence_on_sma = SignalDefinition {
            version: DEFINITION_VERSION,
            direction: Direction::Bullish,
            root: Node::Condition { id: "div".to_owned() },
            conditions: [(
                "div".to_owned(),
                Condition {
                    timeframe: Timeframe::Hour1,
                    indicator: Instance {
                        id: "sma".to_owned(),
                        params: BTreeMap::new(),
                    },
                    predicate: Predicate::Divergence {
                        kind: KindFilter::Any,
                        direction: DirectionFilter::Any,
                    },
                    within_bars: 0,
                },
            )]
            .into_iter()
            .collect(),
        };
        assert!(matches!(
            divergence_on_sma.validate(),
            Err(DefinitionError::DivergenceRequiresDivergenceIndicator { .. })
        ));

        let mut bad_output = definition(Node::Condition { id: "hot".to_owned() });
        let Some(condition) = bad_output.conditions.get_mut("hot") else {
            panic!("fixture condition")
        };
        condition.predicate = Predicate::Threshold {
            left: Operand::Output { key: "nope".to_owned() },
            op: ComparisonOp::GreaterThan,
            right: Operand::Constant { value: 80.0 },
        };
        assert!(matches!(bad_output.validate(), Err(DefinitionError::UnknownOutput { .. })));
    }

    #[test]
    fn serialized_contract_is_tagged_camel_case_and_round_trips() {
        let mut signal = definition(Node::Condition { id: "hot".to_owned() });
        let Some(condition) = signal.conditions.get_mut("hot") else {
            panic!("fixture condition")
        };
        condition.within_bars = 3;

        let value = serde_json::to_value(&signal).expect("serialize definition");
        assert_eq!(value.pointer("/direction"), Some(&json!("bearish")));
        assert_eq!(value.pointer("/root/type"), Some(&json!("condition")));
        assert_eq!(value.pointer("/conditions/hot/timeframe"), Some(&json!("1h")));
        assert_eq!(value.pointer("/conditions/hot/withinBars"), Some(&json!(3)));
        assert_eq!(value.pointer("/conditions/hot/predicate/type"), Some(&json!("threshold")));
        assert_eq!(value.pointer("/conditions/hot/predicate/left/type"), Some(&json!("output")));

        let round_trip: SignalDefinition = serde_json::from_value(value).expect("deserialize definition");
        assert_eq!(round_trip, signal);
    }
}
