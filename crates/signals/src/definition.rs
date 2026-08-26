//! The persisted signal model: market and indicator conditions composed into a
//! boolean logic tree.
//!
//! Definitions use an explicitly tagged, camel-case JSON contract because the
//! same representation is stored in SQLite and edited by the frontend. Validation
//! resolves every tree leaf and checks indicator capabilities before evaluation.

use std::collections::{BTreeMap, BTreeSet};

use domain::Timeframe;
use indicators::{
    DescriptorKind, DivergenceDirection, DivergenceKind, Instance, InstanceError, divergence_input,
    series_input_requirement, validate_instance,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Current schema version of serialized signal definitions.
///
/// Version 1 was internal-only. Version 2 is the first builder-ready contract
/// and moves configured indicators onto the operands that consume them.
pub const DEFINITION_VERSION: u32 = 2;

/// Sanity bound preventing corrupted definitions from requesting unbounded work.
/// Valid requirements within this bound may be loaded beyond the hot cache.
const MAX_WITHIN_BARS: u32 = 10_000;

/// A definition validation failure.
#[derive(Debug, Error)]
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
    /// The configured indicator id or one of its parameters is invalid.
    #[error("condition '{condition}' has an invalid indicator configuration: {source}")]
    InvalidIndicator {
        /// Condition id.
        condition: String,
        /// Descriptor validation failure.
        #[source]
        source: InstanceError,
    },
    /// A predicate constant is NaN or infinite.
    #[error("condition '{0}' has a non-finite constant")]
    NonFiniteConstant(String),
    /// An operand adjustment is NaN or infinite.
    #[error("condition '{0}' has a non-finite operand adjustment")]
    NonFiniteAdjustment(String),
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
    /// A series predicate compares constants without reading market data.
    #[error("condition '{0}' must reference at least one market or indicator series")]
    PredicateIgnoresMarketData(String),
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

/// A single non-composable adjustment applied to a series before comparison.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
pub enum OperandAdjustment {
    /// Add a fixed value in the series' own units.
    Absolute {
        /// Signed value to add.
        value: f64,
    },
    /// Scale by `1 + value / 100`.
    Percent {
        /// Signed percentage, such as `3` for three percent above.
        value: f64,
    },
}

impl OperandAdjustment {
    pub(crate) fn apply(self, value: f64) -> f64 {
        match self {
            Self::Absolute { value: offset } => value + offset,
            Self::Percent { value: percent } => value * (1.0 + percent / 100.0),
        }
    }

    const fn value(self) -> f64 {
        match self {
            Self::Absolute { value } | Self::Percent { value } => value,
        }
    }
}

/// Which scalar or series an operand reads from.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
pub enum Operand {
    /// One output of a configured indicator instance.
    Indicator {
        /// Indicator id and resolved parameters. Presets are copied here rather
        /// than referenced so a saved signal remains stable.
        instance: Instance,
        /// Descriptor output key.
        output: String,
        /// Optional absolute or percentage adjustment.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        adjustment: Option<OperandAdjustment>,
    },
    /// A raw OHLCV candle field.
    Field {
        /// Field to read.
        field: CandleField,
        /// Optional absolute or percentage adjustment.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        adjustment: Option<OperandAdjustment>,
    },
    /// A literal number broadcast over the series.
    Constant {
        /// Literal value.
        value: f64,
    },
}

impl Operand {
    const fn instance(&self) -> Option<&Instance> {
        match self {
            Self::Indicator { instance, .. } => Some(instance),
            Self::Field { .. } | Self::Constant { .. } => None,
        }
    }

    const fn adjustment(&self) -> Option<OperandAdjustment> {
        match self {
            Self::Indicator { adjustment, .. } | Self::Field { adjustment, .. } => *adjustment,
            Self::Constant { .. } => None,
        }
    }
}

/// Raw OHLCV fields usable as operands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CandleField {
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

/// How a condition judges market fields or configured indicator outputs.
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
        /// Configured divergence indicator. Preset parameters are snapshotted.
        instance: Instance,
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
    /// What the condition tests.
    pub predicate: Predicate,
    /// Bars of tolerance back from the latest completed bar (`0` = latest only).
    #[serde(default)]
    pub within_bars: u32,
}

pub(crate) struct ConditionRequirement {
    pub(crate) completed_bars: usize,
    pub(crate) confirmation_bars: usize,
}

impl Condition {
    pub(crate) fn requirement(&self) -> Result<ConditionRequirement, InstanceError> {
        let freshness = self.within_bars as usize;
        match &self.predicate {
            Predicate::Threshold { left, right, .. } => Ok(ConditionRequirement {
                completed_bars: operand_requirement(left)?.max(operand_requirement(right)?) + freshness,
                confirmation_bars: 0,
            }),
            Predicate::Cross { left, right, .. } => Ok(ConditionRequirement {
                completed_bars: operand_requirement(left)?.max(operand_requirement(right)?) + freshness + 1,
                confirmation_bars: 0,
            }),
            Predicate::Divergence { instance, .. } => {
                let input = divergence_input(instance)?;
                // The recent pivot is known after its right-hand confirmation;
                // its prior same-kind pivot may be `max_gap` bars earlier, and
                // that pivot needs both left context and a defined oscillator.
                Ok(ConditionRequirement {
                    completed_bars: freshness
                        + input.pivot_right
                        + input.max_gap
                        + input.pivot_left.max(input.oscillator - 1)
                        + 1,
                    confirmation_bars: input.pivot_right,
                })
            }
        }
    }

    pub(crate) const fn instances(&self) -> [Option<&Instance>; 2] {
        match &self.predicate {
            Predicate::Threshold { left, right, .. } | Predicate::Cross { left, right, .. } => {
                [left.instance(), right.instance()]
            }
            Predicate::Divergence { instance, .. } => [Some(instance), None],
        }
    }

    /// Completed candles needed to evaluate the full freshness window.
    pub fn required_completed_bars(&self) -> Result<usize, InstanceError> {
        self.requirement().map(|requirement| requirement.completed_bars)
    }
}

fn operand_requirement(operand: &Operand) -> Result<usize, InstanceError> {
    match operand {
        Operand::Indicator { instance, output, .. } => series_input_requirement(instance, output),
        Operand::Field { .. } => Ok(1),
        Operand::Constant { .. } => Ok(0),
    }
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
    /// Validate structure, registry membership, configured sources, and predicate
    /// compatibility before persistence or evaluation.
    pub fn validate(&self) -> Result<(), DefinitionError> {
        if self.version != DEFINITION_VERSION {
            return Err(DefinitionError::UnsupportedVersion(self.version));
        }
        if self.conditions.is_empty() {
            return Err(DefinitionError::NoConditions);
        }

        for (id, condition) in &self.conditions {
            validate_condition(id, condition)?;
        }

        let mut referenced = BTreeSet::new();
        validate_node(&self.root, &self.conditions, &mut referenced)?;
        if let Some(id) = self.conditions.keys().find(|id| !referenced.contains(*id)) {
            return Err(DefinitionError::UnreferencedCondition(id.clone()));
        }
        Ok(())
    }

    /// Maximum completed-candle requirement per referenced timeframe.
    pub fn required_history(&self) -> Result<BTreeMap<Timeframe, usize>, DefinitionError> {
        self.validate()?;
        let mut requirements = BTreeMap::new();
        for (id, condition) in &self.conditions {
            let required = condition
                .requirement()
                .map_err(|source| DefinitionError::InvalidIndicator {
                    condition: id.clone(),
                    source,
                })?
                .completed_bars;
            requirements
                .entry(condition.timeframe)
                .and_modify(|current: &mut usize| *current = (*current).max(required))
                .or_insert(required);
        }
        Ok(requirements)
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

fn validate_condition(id: &str, condition: &Condition) -> Result<(), DefinitionError> {
    if condition.within_bars > MAX_WITHIN_BARS {
        return Err(DefinitionError::FreshnessTooLarge {
            condition: id.to_owned(),
            maximum: MAX_WITHIN_BARS,
        });
    }

    match &condition.predicate {
        Predicate::Threshold { left, right, .. } | Predicate::Cross { left, right, .. } => {
            let operands = [left, right];
            if operands.iter().all(|operand| matches!(operand, Operand::Constant { .. })) {
                return Err(DefinitionError::PredicateIgnoresMarketData(id.to_owned()));
            }
            for operand in operands {
                validate_operand(id, operand)?;
            }
        }
        Predicate::Divergence { instance, .. } => {
            let descriptor = validate_instance(instance).map_err(|source| DefinitionError::InvalidIndicator {
                condition: id.to_owned(),
                source,
            })?;
            if descriptor.kind != DescriptorKind::Divergence {
                return Err(DefinitionError::DivergenceRequiresDivergenceIndicator {
                    condition: id.to_owned(),
                    indicator: descriptor.id.clone(),
                });
            }
        }
    }
    Ok(())
}

fn validate_operand(id: &str, operand: &Operand) -> Result<(), DefinitionError> {
    if operand.adjustment().is_some_and(|adjustment| !adjustment.value().is_finite()) {
        return Err(DefinitionError::NonFiniteAdjustment(id.to_owned()));
    }

    match operand {
        Operand::Constant { value } => {
            if !value.is_finite() {
                return Err(DefinitionError::NonFiniteConstant(id.to_owned()));
            }
        }
        Operand::Field { .. } => {}
        Operand::Indicator { instance, output, .. } => {
            let descriptor = validate_instance(instance).map_err(|source| DefinitionError::InvalidIndicator {
                condition: id.to_owned(),
                source,
            })?;
            if descriptor.kind != DescriptorKind::Series {
                return Err(DefinitionError::SeriesRequiresSeriesIndicator {
                    condition: id.to_owned(),
                    indicator: descriptor.id.clone(),
                });
            }
            if !descriptor.outputs.iter().any(|candidate| candidate.key == *output) {
                return Err(DefinitionError::UnknownOutput {
                    condition: id.to_owned(),
                    indicator: descriptor.id.clone(),
                    output: output.clone(),
                });
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use domain::Timeframe;
    use indicators::{Instance, InstanceError};
    use serde_json::json;

    use super::{
        CandleField, ComparisonOp, Condition, CrossDirection, DEFINITION_VERSION, DefinitionError, Direction,
        DirectionFilter, KindFilter, Node, Operand, OperandAdjustment, Predicate, SignalDefinition,
    };

    fn sma(period: f64) -> Instance {
        Instance {
            id: "sma".to_owned(),
            params: [("period".to_owned(), period)].into_iter().collect(),
        }
    }

    fn sma_output(period: f64) -> Operand {
        Operand::Indicator {
            instance: sma(period),
            output: "sma".to_owned(),
            adjustment: None,
        }
    }

    fn indicator_instance_mut(condition: &mut Condition) -> &mut Instance {
        let Predicate::Threshold {
            left: Operand::Indicator { instance, .. },
            ..
        } = &mut condition.predicate
        else {
            panic!("fixture indicator operand")
        };
        instance
    }

    fn threshold() -> Condition {
        Condition {
            timeframe: Timeframe::Hour1,
            predicate: Predicate::Threshold {
                left: sma_output(20.0),
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
    fn validates_indicator_and_raw_market_conditions() {
        let signal = definition(Node::Condition { id: "hot".to_owned() });
        assert!(signal.validate().is_ok());

        let mut raw = signal;
        raw.conditions.get_mut("hot").expect("fixture condition").predicate = Predicate::Threshold {
            left: Operand::Field {
                field: CandleField::Close,
                adjustment: None,
            },
            op: ComparisonOp::GreaterThan,
            right: Operand::Constant { value: 80.0 },
        };
        assert!(raw.validate().is_ok());
        assert_eq!(raw.conditions["hot"].required_completed_bars().expect("raw history"), 1);

        raw.conditions.get_mut("hot").expect("fixture condition").predicate = Predicate::Threshold {
            left: Operand::Constant { value: 100.0 },
            op: ComparisonOp::GreaterThan,
            right: Operand::Constant { value: 80.0 },
        };
        assert!(matches!(
            raw.validate(),
            Err(DefinitionError::PredicateIgnoresMarketData(id)) if id == "hot"
        ));
    }

    #[test]
    fn rejects_indicator_parameters_outside_descriptor_contracts() {
        let mut unknown = definition(Node::Condition { id: "hot".to_owned() });
        indicator_instance_mut(unknown.conditions.get_mut("hot").expect("fixture condition"))
            .params
            .insert("typo".to_owned(), 20.0);
        assert!(matches!(
            unknown.validate(),
            Err(DefinitionError::InvalidIndicator {
                source: InstanceError::UnknownParameter { parameter, .. },
                ..
            }) if parameter == "typo"
        ));

        let mut fractional = definition(Node::Condition { id: "hot".to_owned() });
        indicator_instance_mut(fractional.conditions.get_mut("hot").expect("fixture condition"))
            .params
            .insert("period".to_owned(), 20.5);
        assert!(matches!(
            fractional.validate(),
            Err(DefinitionError::InvalidIndicator {
                source: InstanceError::ParameterOffStep { parameter, .. },
                ..
            }) if parameter == "period"
        ));
    }

    #[test]
    fn calculates_exact_history_per_condition_and_timeframe() {
        let mut threshold = threshold();
        threshold.within_bars = 3;
        assert_eq!(threshold.required_completed_bars().expect("threshold history"), 23);

        let mut cross = threshold.clone();
        cross.predicate = Predicate::Cross {
            left: sma_output(20.0),
            right: sma_output(50.0),
            direction: CrossDirection::Over,
        };
        assert_eq!(cross.required_completed_bars().expect("cross history"), 54);

        let divergence = Condition {
            timeframe: Timeframe::Day1,
            predicate: Predicate::Divergence {
                instance: Instance {
                    id: "rsiDivergence".to_owned(),
                    params: BTreeMap::new(),
                },
                kind: KindFilter::Any,
                direction: DirectionFilter::Any,
            },
            within_bars: 3,
        };
        assert_eq!(divergence.required_completed_bars().expect("divergence history"), 83);

        let signal = SignalDefinition {
            version: DEFINITION_VERSION,
            direction: Direction::Bullish,
            root: Node::All {
                children: vec![
                    Node::Condition {
                        id: "threshold".to_owned(),
                    },
                    Node::Condition {
                        id: "divergence".to_owned(),
                    },
                ],
            },
            conditions: [
                ("threshold".to_owned(), threshold),
                ("divergence".to_owned(), divergence),
            ]
            .into_iter()
            .collect(),
        };
        let requirements = signal.required_history().expect("valid requirements");
        assert_eq!(requirements.get(&Timeframe::Hour1), Some(&23));
        assert_eq!(requirements.get(&Timeframe::Day1), Some(&83));
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
                    predicate: Predicate::Divergence {
                        instance: Instance {
                            id: "sma".to_owned(),
                            params: BTreeMap::new(),
                        },
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
            left: Operand::Indicator {
                instance: sma(20.0),
                output: "nope".to_owned(),
                adjustment: None,
            },
            op: ComparisonOp::GreaterThan,
            right: Operand::Constant { value: 80.0 },
        };
        assert!(matches!(bad_output.validate(), Err(DefinitionError::UnknownOutput { .. })));
    }

    #[test]
    fn rejects_non_finite_operand_adjustments() {
        let mut signal = definition(Node::Condition { id: "hot".to_owned() });
        let Predicate::Threshold {
            left: Operand::Indicator { adjustment, .. },
            ..
        } = &mut signal.conditions.get_mut("hot").expect("fixture condition").predicate
        else {
            panic!("fixture indicator operand")
        };
        *adjustment = Some(OperandAdjustment::Percent { value: f64::NAN });

        assert!(matches!(
            signal.validate(),
            Err(DefinitionError::NonFiniteAdjustment(id)) if id == "hot"
        ));
    }

    #[test]
    fn serialized_contract_is_tagged_camel_case_and_round_trips() {
        let mut signal = definition(Node::Condition { id: "hot".to_owned() });
        let Some(condition) = signal.conditions.get_mut("hot") else {
            panic!("fixture condition")
        };
        condition.within_bars = 3;
        let Predicate::Threshold {
            left: Operand::Indicator { adjustment, .. },
            ..
        } = &mut condition.predicate
        else {
            panic!("fixture indicator operand")
        };
        *adjustment = Some(OperandAdjustment::Percent { value: 3.0 });

        let value = serde_json::to_value(&signal).expect("serialize definition");
        assert_eq!(value.pointer("/version"), Some(&json!(2)));
        assert_eq!(value.pointer("/direction"), Some(&json!("bearish")));
        assert_eq!(value.pointer("/root/type"), Some(&json!("condition")));
        assert_eq!(value.pointer("/conditions/hot/timeframe"), Some(&json!("1h")));
        assert_eq!(value.pointer("/conditions/hot/withinBars"), Some(&json!(3)));
        assert_eq!(value.pointer("/conditions/hot/predicate/type"), Some(&json!("threshold")));
        assert_eq!(value.pointer("/conditions/hot/predicate/left/type"), Some(&json!("indicator")));
        assert_eq!(value.pointer("/conditions/hot/predicate/left/instance/id"), Some(&json!("sma")));
        assert_eq!(
            value.pointer("/conditions/hot/predicate/left/adjustment/type"),
            Some(&json!("percent"))
        );
        assert_eq!(
            value.pointer("/conditions/hot/predicate/left/adjustment/value"),
            Some(&json!(3.0))
        );

        let round_trip: SignalDefinition = serde_json::from_value(value).expect("deserialize definition");
        assert_eq!(round_trip, signal);
    }

    #[test]
    fn nested_indicator_instances_reject_unknown_fields() {
        let signal = definition(Node::Condition { id: "hot".to_owned() });
        let mut value = serde_json::to_value(signal).expect("serialize definition");
        let Some(instance) = value
            .pointer_mut("/conditions/hot/predicate/left/instance")
            .and_then(serde_json::Value::as_object_mut)
        else {
            panic!("serialized instance object")
        };
        instance.insert("presetId".to_owned(), json!("fast"));

        assert!(serde_json::from_value::<SignalDefinition>(value).is_err());
    }
}
