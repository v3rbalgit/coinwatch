//! The instance-driven indicator engine: configured `{id, params}` instances
//! computed over one OHLCV series.
//!
//! [`descriptors`] is the registry metadata (labels, tunable params, plotted
//! outputs) that drives editor UIs; [`compute`] turns instances into output
//! series and divergence spans. Consumers share this one dispatch so indicator
//! ids, param keys, and defaults can never drift between them.

use std::collections::BTreeMap;
use std::sync::OnceLock;

use serde::{Deserialize, Serialize};
use strum::{AsRefStr, Display, EnumCount, EnumString};
use thiserror::Error;

use crate::adx::{Adx, adx};
use crate::atr::atr;
use crate::bollinger::{Bollinger, bollinger};
use crate::cci::cci;
use crate::divergence::{DivergenceDirection, DivergenceKind, detect_divergences};
use crate::donchian::{Donchian, donchian};
use crate::macd::{Macd, ema, macd};
use crate::mfi::mfi;
use crate::obv::obv;
use crate::psar::psar;
use crate::rsi::rsi;
use crate::sma::sma;
use crate::stochastic::{Stochastic, stochastic};
use crate::supertrend::{Supertrend, supertrend};
use crate::vwap::vwap;
use crate::williams::williams_r;
use crate::wma::wma;

/// Where an indicator draws: over the price pane, or in its own pane below.
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Target {
    /// Drawn over the price/candles pane.
    Overlay,
    /// Drawn in a dedicated pane below the price.
    Pane,
}

/// What an indicator instance produces.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum DescriptorKind {
    /// One or more numeric output series.
    Series,
    /// Divergence spans between price and an oscillator.
    Divergence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, AsRefStr, EnumCount, EnumString)]
#[strum(serialize_all = "camelCase")]
enum IndicatorId {
    Sma,
    Ema,
    Wma,
    Bollinger,
    Vwap,
    Donchian,
    Supertrend,
    Psar,
    Rsi,
    Macd,
    Stochastic,
    Williams,
    Cci,
    Mfi,
    Adx,
    Atr,
    Obv,
    RsiDivergence,
    MacdDivergence,
}

#[derive(Debug, Clone, Copy)]
enum DivergenceOscillator {
    Rsi,
    Macd,
}

impl IndicatorId {
    const fn divergence_oscillator(self) -> Option<DivergenceOscillator> {
        match self {
            Self::RsiDivergence => Some(DivergenceOscillator::Rsi),
            Self::MacdDivergence => Some(DivergenceOscillator::Macd),
            Self::Sma
            | Self::Ema
            | Self::Wma
            | Self::Bollinger
            | Self::Vwap
            | Self::Donchian
            | Self::Supertrend
            | Self::Psar
            | Self::Rsi
            | Self::Macd
            | Self::Stochastic
            | Self::Williams
            | Self::Cci
            | Self::Mfi
            | Self::Adx
            | Self::Atr
            | Self::Obv => None,
        }
    }

    /// Anchor- and path-dependent indicators never converge under finite history.
    const fn supports_signals(self) -> bool {
        !matches!(self, Self::Vwap | Self::Obv | Self::Psar | Self::Supertrend)
    }

    const fn output_domain(self) -> ValueDomain {
        match self {
            Self::Sma
            | Self::Ema
            | Self::Wma
            | Self::Bollinger
            | Self::Vwap
            | Self::Donchian
            | Self::Supertrend
            | Self::Psar => ValueDomain::Price,
            Self::Atr => ValueDomain::PriceDelta,
            Self::Obv => ValueDomain::Volume,
            Self::Rsi | Self::Stochastic | Self::Williams | Self::Mfi | Self::Adx | Self::RsiDivergence => {
                ValueDomain::BoundedOscillator
            }
            Self::Macd | Self::Cci | Self::MacdDivergence => ValueDomain::UnboundedOscillator,
        }
    }
}

/// How one output series is plotted.
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum OutputKind {
    /// A continuous line.
    Line,
    /// A histogram (e.g. the MACD histogram).
    Histogram,
}

/// Numeric family shared by every output of one indicator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display, Serialize)]
#[serde(rename_all = "camelCase")]
#[strum(serialize_all = "camelCase")]
pub enum ValueDomain {
    /// Absolute market-price levels.
    Price,
    /// Price movement or volatility rather than an absolute level.
    PriceDelta,
    /// Raw or accumulated traded volume.
    Volume,
    /// Oscillators with a fixed finite range.
    BoundedOscillator,
    /// Oscillators without a fixed finite range.
    UnboundedOscillator,
}

/// A tunable parameter — surfaced with conventional defaults, editable by callers.
#[derive(Debug, Clone, Serialize)]
pub struct Param {
    /// Instance-params key.
    pub key: String,
    /// Human label.
    pub label: String,
    /// Conventional default value.
    pub default: f64,
    /// Minimum for an editor.
    pub min: f64,
    /// Maximum for an editor.
    pub max: f64,
    /// Step for an editor.
    pub step: f64,
}

/// One plotted output of an indicator (keyed to the series `compute` returns).
#[derive(Debug, Clone, Serialize)]
pub struct Output {
    /// Output key, matching the `OutputSeries.key` from `compute`.
    pub key: String,
    /// Human label.
    pub label: String,
    /// Line vs histogram.
    pub kind: OutputKind,
    /// Default line/bar color (hex); renderers may override from their theme.
    pub color: String,
}

/// Static metadata for one indicator: drives pickers/editors and the renderer.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Descriptor {
    /// Stable id (matches `Instance.id`).
    pub id: String,
    /// Human label.
    pub label: String,
    /// Whether instances produce numeric series or divergence spans.
    pub kind: DescriptorKind,
    /// Numeric family represented by the descriptor's outputs or divergence oscillator.
    pub output_domain: ValueDomain,
    /// Whether values are stable under the signal engine's finite history contract.
    pub supports_signals: bool,
    /// Overlay vs own pane.
    pub target: Target,
    /// Tunable params with defaults.
    pub params: Vec<Param>,
    /// Plotted outputs.
    pub outputs: Vec<Output>,
    /// Horizontal guide levels (e.g. RSI 30/70); empty for none.
    pub guides: Vec<f64>,
    /// Fixed pane range (e.g. `[0, 100]`); `None` autoscales.
    pub range: Option<[f64; 2]>,
}

/// A configured indicator to compute: an id plus its params.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Instance {
    /// Which indicator (matches a [`Descriptor::id`]).
    pub id: String,
    /// Params by key; missing keys fall back to conventional defaults.
    #[serde(default)]
    pub params: BTreeMap<String, f64>,
}

/// Why a configured indicator instance is invalid.
#[derive(Debug, Error)]
pub enum InstanceError {
    /// The instance id is absent from the descriptor registry.
    #[error("unknown indicator '{0}'")]
    UnknownIndicator(String),
    /// A supplied parameter is absent from the indicator descriptor.
    #[error("indicator '{indicator}' has no '{parameter}' parameter")]
    UnknownParameter {
        /// Indicator id.
        indicator: String,
        /// Unknown parameter key.
        parameter: String,
    },
    /// A parameter is NaN or infinite.
    #[error("indicator '{indicator}' has a non-finite '{parameter}' parameter")]
    NonFiniteParameter {
        /// Indicator id.
        indicator: String,
        /// Parameter key.
        parameter: String,
    },
    /// A parameter lies outside its inclusive descriptor bounds.
    #[error("indicator '{indicator}' parameter '{parameter}' value {value} is outside {min}..={max}")]
    ParameterOutOfRange {
        /// Indicator id.
        indicator: String,
        /// Parameter key.
        parameter: String,
        /// Supplied value.
        value: f64,
        /// Inclusive minimum.
        min: f64,
        /// Inclusive maximum.
        max: f64,
    },
    /// A parameter does not lie on the descriptor's step grid.
    #[error("indicator '{indicator}' parameter '{parameter}' value {value} is not aligned to step {step}")]
    ParameterOffStep {
        /// Indicator id.
        indicator: String,
        /// Parameter key.
        parameter: String,
        /// Supplied value.
        value: f64,
        /// Grid step.
        step: f64,
    },
    /// A requested series output is absent from the descriptor.
    #[error("indicator '{indicator}' has no '{output}' output")]
    UnknownOutput {
        /// Indicator id.
        indicator: String,
        /// Unknown output key.
        output: String,
    },
    /// Divergence metadata was requested from a numeric-series indicator.
    #[error("indicator '{0}' does not produce divergences")]
    NotDivergence(String),
}

/// Inputs needed to search for a divergence over a completed-bar freshness window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DivergenceInput {
    /// Candles required for the oscillator's first defined value.
    pub oscillator: usize,
    /// Bars left of a pivot center.
    pub pivot_left: usize,
    /// Bars right of a pivot center before confirmation.
    pub pivot_right: usize,
    /// Maximum distance between consecutive same-kind pivots.
    pub max_gap: usize,
}

/// One computed output series, aligned index-for-index with the input (`NaN` where
/// undefined), keyed to the matching [`Output`].
#[derive(Debug, Clone, Serialize)]
pub struct OutputSeries {
    /// Output key (matches [`Output::key`]).
    pub key: String,
    /// Values, `NaN` for undefined bars.
    pub values: Vec<f64>,
}

/// A divergence line between two pivots (indices into the input series); the
/// renderer styles it by direction (color) and kind (solid/dashed).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DivergenceSpan {
    /// Index of the earlier pivot.
    pub from_index: usize,
    /// Index of the later pivot.
    pub to_index: usize,
    /// Reversal (`Regular`) or continuation (`Hidden`).
    pub kind: DivergenceKind,
    /// Bullish or bearish.
    pub direction: DivergenceDirection,
}

/// One computed instance: plotted series and/or divergence spans (either may be
/// empty), in descriptor order.
#[derive(Debug, Clone, Serialize)]
pub struct InstanceOutput {
    /// One entry per plotted output series.
    pub outputs: Vec<OutputSeries>,
    /// Divergence spans (empty for non-divergence indicators).
    pub divergences: Vec<DivergenceSpan>,
}

/// OHLCV column slices (equal length) for one instrument+timeframe.
#[derive(Debug, Clone, Copy)]
pub struct Ohlcv<'a> {
    /// High prices.
    pub high: &'a [f64],
    /// Low prices.
    pub low: &'a [f64],
    /// Close prices.
    pub close: &'a [f64],
    /// Volumes.
    pub volume: &'a [f64],
}

fn nan(series: &[Option<f64>]) -> Vec<f64> {
    series.iter().map(|value| value.unwrap_or(f64::NAN)).collect()
}

fn out(key: &str, series: &[Option<f64>]) -> OutputSeries {
    OutputSeries {
        key: key.to_owned(),
        values: nan(series),
    }
}

/// Descriptor defaults are the single source; compute never carries its own literals.
fn get(descriptor: &Descriptor, params: &BTreeMap<String, f64>, key: &str) -> f64 {
    params.get(key).copied().unwrap_or_else(|| {
        descriptor
            .params
            .iter()
            .find(|param| param.key == key)
            .map_or(f64::NAN, |param| param.default)
    })
}

fn period(descriptor: &Descriptor, params: &BTreeMap<String, f64>, key: &str) -> usize {
    get(descriptor, params, key).max(1.0) as usize
}

fn descriptor_of(indicator: IndicatorId) -> Option<&'static Descriptor> {
    descriptors().iter().find(|descriptor| descriptor.id == indicator.as_ref())
}

/// Compute one instance's output series over `data`. Unknown ids yield no outputs.
fn compute_one(instance: &Instance, data: &Ohlcv) -> Vec<OutputSeries> {
    let Ok(indicator) = instance.id.parse::<IndicatorId>() else {
        return Vec::new();
    };
    let Some(descriptor) = descriptor_of(indicator) else {
        return Vec::new();
    };
    let params = &instance.params;
    let (high, low, close, volume) = (data.high, data.low, data.close, data.volume);
    match indicator {
        IndicatorId::Sma => vec![out("sma", &sma(close, period(descriptor, params, "period")))],
        IndicatorId::Ema => vec![out("ema", &ema(close, period(descriptor, params, "period")))],
        IndicatorId::Wma => vec![out("wma", &wma(close, period(descriptor, params, "period")))],
        IndicatorId::Bollinger => {
            let Bollinger { middle, upper, lower } =
                bollinger(close, period(descriptor, params, "period"), get(descriptor, params, "mult"));
            vec![out("upper", &upper), out("middle", &middle), out("lower", &lower)]
        }
        IndicatorId::Vwap => vec![out("vwap", &vwap(high, low, close, volume))],
        IndicatorId::Donchian => {
            let Donchian { upper, middle, lower } = donchian(high, low, period(descriptor, params, "period"));
            vec![out("upper", &upper), out("middle", &middle), out("lower", &lower)]
        }
        IndicatorId::Supertrend => {
            let Supertrend { line, .. } = supertrend(
                high,
                low,
                close,
                period(descriptor, params, "period"),
                get(descriptor, params, "mult"),
            );
            vec![out("supertrend", &line)]
        }
        IndicatorId::Psar => vec![out(
            "psar",
            &psar(high, low, get(descriptor, params, "step"), get(descriptor, params, "max")),
        )],
        IndicatorId::Rsi => vec![out("rsi", &rsi(close, period(descriptor, params, "period")))],
        IndicatorId::Macd => {
            let Macd {
                macd: line,
                signal,
                histogram,
            } = macd(
                close,
                period(descriptor, params, "fast"),
                period(descriptor, params, "slow"),
                period(descriptor, params, "signal"),
            );
            vec![out("macd", &line), out("signal", &signal), out("histogram", &histogram)]
        }
        IndicatorId::Stochastic => {
            let Stochastic { k, d } = stochastic(
                high,
                low,
                close,
                period(descriptor, params, "period"),
                period(descriptor, params, "smoothD"),
            );
            vec![out("k", &k), out("d", &d)]
        }
        IndicatorId::Williams => vec![out(
            "williams",
            &williams_r(high, low, close, period(descriptor, params, "period")),
        )],
        IndicatorId::Cci => vec![out("cci", &cci(high, low, close, period(descriptor, params, "period")))],
        IndicatorId::Mfi => vec![out(
            "mfi",
            &mfi(high, low, close, volume, period(descriptor, params, "period")),
        )],
        IndicatorId::Adx => {
            let Adx {
                adx: line,
                plus_di,
                minus_di,
            } = adx(high, low, close, period(descriptor, params, "period"));
            vec![out("adx", &line), out("plusDi", &plus_di), out("minusDi", &minus_di)]
        }
        IndicatorId::Atr => vec![out("atr", &atr(high, low, close, period(descriptor, params, "period")))],
        IndicatorId::Obv => vec![out("obv", &obv(close, volume))],
        IndicatorId::RsiDivergence | IndicatorId::MacdDivergence => Vec::new(),
    }
}

/// Divergence spans for a divergence instance (empty for other ids): `close` is
/// compared against the instance's oscillator using its pivot/gap params (defaults
/// mirror the divergence detector's conventional settings).
fn compute_divergences(instance: &Instance, data: &Ohlcv) -> Vec<DivergenceSpan> {
    let Ok(indicator) = instance.id.parse::<IndicatorId>() else {
        return Vec::new();
    };
    let Some(divergence) = indicator.divergence_oscillator() else {
        return Vec::new();
    };
    let Some(descriptor) = descriptor_of(indicator) else {
        return Vec::new();
    };
    let params = &instance.params;
    let close = data.close;
    let oscillator = match divergence {
        DivergenceOscillator::Rsi => rsi(close, period(descriptor, params, "period")),
        DivergenceOscillator::Macd => {
            macd(
                close,
                period(descriptor, params, "fast"),
                period(descriptor, params, "slow"),
                period(descriptor, params, "signal"),
            )
            .macd
        }
    };
    detect_divergences(
        close,
        &oscillator,
        period(descriptor, params, "pivotLeft"),
        period(descriptor, params, "pivotRight"),
        period(descriptor, params, "maxGap"),
    )
    .into_iter()
    .map(|divergence| DivergenceSpan {
        from_index: divergence.prior_index,
        to_index: divergence.recent_index,
        kind: divergence.kind,
        direction: divergence.direction,
    })
    .collect()
}

/// Compute every instance over one OHLCV series: series outputs and divergence spans.
#[must_use]
pub fn compute(instances: &[Instance], data: &Ohlcv) -> Vec<InstanceOutput> {
    instances
        .iter()
        .map(|instance| InstanceOutput {
            outputs: compute_one(instance, data),
            divergences: compute_divergences(instance, data),
        })
        .collect()
}

fn param(key: &str, label: &str, default: f64, min: f64, max: f64, step: f64) -> Param {
    Param {
        key: key.to_owned(),
        label: label.to_owned(),
        default,
        min,
        max,
        step,
    }
}

fn line(key: &str, label: &str, color: &str) -> Output {
    Output {
        key: key.to_owned(),
        label: label.to_owned(),
        kind: OutputKind::Line,
        color: color.to_owned(),
    }
}

fn descriptor(id: IndicatorId, label: &str, target: Target, params: Vec<Param>, outputs: Vec<Output>) -> Descriptor {
    Descriptor {
        id: id.as_ref().to_owned(),
        label: label.to_owned(),
        kind: DescriptorKind::Series,
        output_domain: id.output_domain(),
        supports_signals: id.supports_signals(),
        target,
        params,
        outputs,
        guides: Vec::new(),
        range: None,
    }
}

static DESCRIPTORS: OnceLock<Vec<Descriptor>> = OnceLock::new();

/// The registry of supported indicators, with conventional defaults.
///
/// Metadata is allocated once because validation, the chart, and the builder all
/// read the same immutable registry.
#[must_use]
pub fn descriptors() -> &'static [Descriptor] {
    DESCRIPTORS.get_or_init(build_descriptors)
}

fn step_aligned(value: f64, parameter: &Param) -> bool {
    let steps = (value - parameter.min) / parameter.step;
    let nearest = steps.round();
    let scale = steps.abs().max(nearest.abs()).max(1.0);
    (steps - nearest).abs() <= f64::EPSILON * scale
}

fn validated_instance(instance: &Instance) -> Result<(IndicatorId, &'static Descriptor), InstanceError> {
    let indicator = instance
        .id
        .parse::<IndicatorId>()
        .map_err(|_| InstanceError::UnknownIndicator(instance.id.clone()))?;
    let descriptor = descriptors()
        .iter()
        .find(|descriptor| descriptor.id == indicator.as_ref())
        .ok_or_else(|| InstanceError::UnknownIndicator(instance.id.clone()))?;

    for (key, &value) in &instance.params {
        let parameter = descriptor
            .params
            .iter()
            .find(|parameter| parameter.key == *key)
            .ok_or_else(|| InstanceError::UnknownParameter {
                indicator: instance.id.clone(),
                parameter: key.clone(),
            })?;
        if !value.is_finite() {
            return Err(InstanceError::NonFiniteParameter {
                indicator: instance.id.clone(),
                parameter: key.clone(),
            });
        }
        if value < parameter.min || value > parameter.max {
            return Err(InstanceError::ParameterOutOfRange {
                indicator: instance.id.clone(),
                parameter: key.clone(),
                value,
                min: parameter.min,
                max: parameter.max,
            });
        }
        if !step_aligned(value, parameter) {
            return Err(InstanceError::ParameterOffStep {
                indicator: instance.id.clone(),
                parameter: key.clone(),
                value,
                step: parameter.step,
            });
        }
    }
    Ok((indicator, descriptor))
}

/// Validate an instance against its descriptor.
///
/// Missing parameters are valid and use descriptor defaults. Supplied parameters
/// must be known, finite, within inclusive bounds, and aligned to the step grid.
pub fn validate_instance(instance: &Instance) -> Result<&'static Descriptor, InstanceError> {
    validated_instance(instance).map(|(_, descriptor)| descriptor)
}

/// Validate an instance and return a stable configuration containing every
/// descriptor parameter, with omitted values replaced by their current defaults.
pub fn resolve_instance(instance: &Instance) -> Result<Instance, InstanceError> {
    let descriptor = validate_instance(instance)?;
    Ok(Instance {
        id: instance.id.clone(),
        params: descriptor
            .params
            .iter()
            .map(|parameter| {
                (
                    parameter.key.clone(),
                    instance.params.get(&parameter.key).copied().unwrap_or(parameter.default),
                )
            })
            .collect(),
    })
}

fn resolved_period(instance: &Instance, descriptor: &Descriptor, key: &str) -> Result<usize, InstanceError> {
    let parameter = descriptor.params.iter().find(|parameter| parameter.key == key).ok_or_else(|| {
        InstanceError::UnknownParameter {
            indicator: instance.id.clone(),
            parameter: key.to_owned(),
        }
    })?;
    Ok(instance.params.get(key).copied().unwrap_or(parameter.default) as usize)
}

fn unknown_output(instance: &Instance, output: &str) -> InstanceError {
    InstanceError::UnknownOutput {
        indicator: instance.id.clone(),
        output: output.to_owned(),
    }
}

/// Minimum candles needed for the selected numeric output's first defined value.
/// Recursive and cumulative indicators seed from the first supplied candle; this
/// reports availability, not convergence against an earlier market history.
pub fn series_input_requirement(instance: &Instance, output: &str) -> Result<usize, InstanceError> {
    let (indicator, descriptor) = validated_instance(instance)?;
    if !descriptor.outputs.iter().any(|candidate| candidate.key == output) {
        return Err(unknown_output(instance, output));
    }

    match indicator {
        IndicatorId::Sma
        | IndicatorId::Ema
        | IndicatorId::Wma
        | IndicatorId::Bollinger
        | IndicatorId::Donchian
        | IndicatorId::Supertrend
        | IndicatorId::Williams
        | IndicatorId::Cci
        | IndicatorId::Atr => resolved_period(instance, descriptor, "period"),
        IndicatorId::Vwap | IndicatorId::Obv => Ok(1),
        IndicatorId::Psar => Ok(2),
        IndicatorId::Rsi | IndicatorId::Mfi => Ok(resolved_period(instance, descriptor, "period")? + 1),
        IndicatorId::Macd => {
            let base =
                resolved_period(instance, descriptor, "fast")?.max(resolved_period(instance, descriptor, "slow")?);
            match output {
                "macd" => Ok(base),
                "signal" | "histogram" => Ok(base + resolved_period(instance, descriptor, "signal")? - 1),
                _ => Err(unknown_output(instance, output)),
            }
        }
        IndicatorId::Stochastic => {
            let period = resolved_period(instance, descriptor, "period")?;
            match output {
                "k" => Ok(period),
                "d" => Ok(period + resolved_period(instance, descriptor, "smoothD")? - 1),
                _ => Err(unknown_output(instance, output)),
            }
        }
        IndicatorId::Adx => {
            let period = resolved_period(instance, descriptor, "period")?;
            match output {
                "adx" => Ok(2 * period),
                "plusDi" | "minusDi" => Ok(period + 1),
                _ => Err(unknown_output(instance, output)),
            }
        }
        IndicatorId::RsiDivergence | IndicatorId::MacdDivergence => Err(unknown_output(instance, output)),
    }
}

/// Wilder/EMA seed error decays to at most `e^-4` after this many periods.
const CONVERGENCE_PERIODS: usize = 4;

const fn rsi_signal_bars(period: usize) -> usize {
    period + 1 + CONVERGENCE_PERIODS * period
}

fn macd_slow(instance: &Instance, descriptor: &Descriptor) -> Result<usize, InstanceError> {
    Ok(resolved_period(instance, descriptor, "fast")?.max(resolved_period(instance, descriptor, "slow")?))
}

/// [`series_input_requirement`] plus convergence padding for exponentially seeded outputs.
pub fn signal_input_requirement(instance: &Instance, output: &str) -> Result<usize, InstanceError> {
    let base = series_input_requirement(instance, output)?;
    let (indicator, descriptor) = validated_instance(instance)?;
    let padding = match indicator {
        IndicatorId::Ema | IndicatorId::Rsi | IndicatorId::Atr | IndicatorId::Adx => {
            CONVERGENCE_PERIODS * resolved_period(instance, descriptor, "period")?
        }
        IndicatorId::Macd => {
            let slow = macd_slow(instance, descriptor)?;
            match output {
                "macd" => CONVERGENCE_PERIODS * slow,
                "signal" | "histogram" => {
                    CONVERGENCE_PERIODS * (slow + resolved_period(instance, descriptor, "signal")?)
                }
                _ => return Err(unknown_output(instance, output)),
            }
        }
        IndicatorId::Sma
        | IndicatorId::Wma
        | IndicatorId::Bollinger
        | IndicatorId::Vwap
        | IndicatorId::Donchian
        | IndicatorId::Supertrend
        | IndicatorId::Psar
        | IndicatorId::Stochastic
        | IndicatorId::Williams
        | IndicatorId::Cci
        | IndicatorId::Mfi
        | IndicatorId::Obv
        | IndicatorId::RsiDivergence
        | IndicatorId::MacdDivergence => 0,
    };
    Ok(base + padding)
}

/// Parameters needed to derive an exact divergence-condition history requirement.
pub fn divergence_input(instance: &Instance) -> Result<DivergenceInput, InstanceError> {
    let (indicator, descriptor) = validated_instance(instance)?;
    let divergence = indicator
        .divergence_oscillator()
        .ok_or_else(|| InstanceError::NotDivergence(instance.id.clone()))?;
    let oscillator = match divergence {
        DivergenceOscillator::Rsi => rsi_signal_bars(resolved_period(instance, descriptor, "period")?),
        DivergenceOscillator::Macd => {
            let slow = macd_slow(instance, descriptor)?;
            slow + CONVERGENCE_PERIODS * slow
        }
    };
    Ok(DivergenceInput {
        oscillator,
        pivot_left: resolved_period(instance, descriptor, "pivotLeft")?,
        pivot_right: resolved_period(instance, descriptor, "pivotRight")?,
        max_gap: resolved_period(instance, descriptor, "maxGap")?,
    })
}

fn divergence_params() -> Vec<Param> {
    vec![
        param("pivotLeft", "Pivot left", 5.0, 1.0, 50.0, 1.0),
        param("pivotRight", "Pivot right", 5.0, 1.0, 50.0, 1.0),
        param("maxGap", "Max gap", 60.0, 1.0, 500.0, 1.0),
    ]
}

fn build_descriptors() -> Vec<Descriptor> {
    let period = |default: f64| param("period", "Period", default, 1.0, 500.0, 1.0);
    vec![
        descriptor(
            IndicatorId::Sma,
            "SMA",
            Target::Overlay,
            vec![period(20.0)],
            vec![line("sma", "SMA", "#3b82f6")],
        ),
        descriptor(
            IndicatorId::Ema,
            "EMA",
            Target::Overlay,
            vec![period(20.0)],
            vec![line("ema", "EMA", "#f59e0b")],
        ),
        descriptor(
            IndicatorId::Wma,
            "WMA",
            Target::Overlay,
            vec![period(20.0)],
            vec![line("wma", "WMA", "#a855f7")],
        ),
        descriptor(
            IndicatorId::Bollinger,
            "Bollinger Bands",
            Target::Overlay,
            vec![period(20.0), param("mult", "Std. dev", 2.0, 0.5, 5.0, 0.1)],
            vec![
                line("upper", "Upper", "#6b7280"),
                line("middle", "Basis", "#3b82f6"),
                line("lower", "Lower", "#6b7280"),
            ],
        ),
        descriptor(
            IndicatorId::Vwap,
            "VWAP",
            Target::Overlay,
            Vec::new(),
            vec![line("vwap", "VWAP", "#eab308")],
        ),
        descriptor(
            IndicatorId::Donchian,
            "Donchian Channel",
            Target::Overlay,
            vec![period(20.0)],
            vec![
                line("upper", "Upper", "#6b7280"),
                line("middle", "Basis", "#3b82f6"),
                line("lower", "Lower", "#6b7280"),
            ],
        ),
        descriptor(
            IndicatorId::Supertrend,
            "Supertrend",
            Target::Overlay,
            vec![
                param("period", "ATR period", 10.0, 1.0, 100.0, 1.0),
                param("mult", "Multiplier", 3.0, 0.5, 10.0, 0.1),
            ],
            vec![line("supertrend", "Supertrend", "#22c55e")],
        ),
        descriptor(
            IndicatorId::Psar,
            "Parabolic SAR",
            Target::Overlay,
            vec![
                param("step", "Step", 0.02, 0.01, 0.2, 0.01),
                param("max", "Max step", 0.2, 0.05, 1.0, 0.05),
            ],
            vec![line("psar", "PSAR", "#e879f9")],
        ),
        Descriptor {
            guides: vec![30.0, 70.0],
            range: Some([0.0, 100.0]),
            ..descriptor(
                IndicatorId::Rsi,
                "RSI",
                Target::Pane,
                vec![period(14.0)],
                vec![line("rsi", "RSI", "#c084fc")],
            )
        },
        {
            let mut macd_descriptor = descriptor(
                IndicatorId::Macd,
                "MACD",
                Target::Pane,
                vec![
                    param("fast", "Fast", 12.0, 1.0, 100.0, 1.0),
                    param("slow", "Slow", 26.0, 1.0, 200.0, 1.0),
                    param("signal", "Signal", 9.0, 1.0, 100.0, 1.0),
                ],
                vec![line("macd", "MACD", "#3b82f6"), line("signal", "Signal", "#f59e0b")],
            );
            macd_descriptor.outputs.push(Output {
                key: "histogram".to_owned(),
                label: "Histogram".to_owned(),
                kind: OutputKind::Histogram,
                color: "#6b7280".to_owned(),
            });
            macd_descriptor.guides = vec![0.0];
            macd_descriptor
        },
        Descriptor {
            guides: vec![20.0, 80.0],
            range: Some([0.0, 100.0]),
            ..descriptor(
                IndicatorId::Stochastic,
                "Stochastic",
                Target::Pane,
                vec![period(14.0), param("smoothD", "%D smoothing", 3.0, 1.0, 50.0, 1.0)],
                vec![line("k", "%K", "#3b82f6"), line("d", "%D", "#f59e0b")],
            )
        },
        Descriptor {
            guides: vec![-20.0, -80.0],
            range: Some([-100.0, 0.0]),
            ..descriptor(
                IndicatorId::Williams,
                "Williams %R",
                Target::Pane,
                vec![period(14.0)],
                vec![line("williams", "%R", "#c084fc")],
            )
        },
        Descriptor {
            guides: vec![-100.0, 100.0],
            ..descriptor(
                IndicatorId::Cci,
                "CCI",
                Target::Pane,
                vec![period(20.0)],
                vec![line("cci", "CCI", "#c084fc")],
            )
        },
        Descriptor {
            guides: vec![20.0, 80.0],
            range: Some([0.0, 100.0]),
            ..descriptor(
                IndicatorId::Mfi,
                "MFI",
                Target::Pane,
                vec![period(14.0)],
                vec![line("mfi", "MFI", "#22c55e")],
            )
        },
        Descriptor {
            range: Some([0.0, 100.0]),
            ..descriptor(
                IndicatorId::Adx,
                "ADX",
                Target::Pane,
                vec![period(14.0)],
                vec![
                    line("adx", "ADX", "#c084fc"),
                    line("plusDi", "+DI", "#22c55e"),
                    line("minusDi", "-DI", "#ef4444"),
                ],
            )
        },
        descriptor(
            IndicatorId::Atr,
            "ATR",
            Target::Pane,
            vec![period(14.0)],
            vec![line("atr", "ATR", "#f59e0b")],
        ),
        descriptor(
            IndicatorId::Obv,
            "OBV",
            Target::Pane,
            Vec::new(),
            vec![line("obv", "OBV", "#22c55e")],
        ),
        {
            let mut params = vec![period(14.0)];
            params.extend(divergence_params());
            Descriptor {
                kind: DescriptorKind::Divergence,
                ..descriptor(
                    IndicatorId::RsiDivergence,
                    "RSI Divergence",
                    Target::Overlay,
                    params,
                    Vec::new(),
                )
            }
        },
        {
            let mut params = vec![
                param("fast", "Fast", 12.0, 1.0, 100.0, 1.0),
                param("slow", "Slow", 26.0, 1.0, 200.0, 1.0),
                param("signal", "Signal", 9.0, 1.0, 100.0, 1.0),
            ];
            params.extend(divergence_params());
            Descriptor {
                kind: DescriptorKind::Divergence,
                ..descriptor(
                    IndicatorId::MacdDivergence,
                    "MACD Divergence",
                    Target::Overlay,
                    params,
                    Vec::new(),
                )
            }
        },
    ]
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::slice;

    use strum::EnumCount;

    use crate::divergence::detect_divergences;
    use crate::rsi::rsi;

    use super::{
        DescriptorKind, IndicatorId, Instance, InstanceError, Ohlcv, ValueDomain, compute, descriptors,
        divergence_input, resolve_instance, series_input_requirement, signal_input_requirement, validate_instance,
    };

    fn series() -> Vec<f64> {
        (1..=60).map(f64::from).collect()
    }

    #[test]
    fn compute_defaults_come_from_the_descriptor_registry() {
        let values = series();
        let data = data(&values);
        for descriptor in descriptors() {
            let bare = Instance {
                id: descriptor.id.clone(),
                params: BTreeMap::new(),
            };
            let explicit = Instance {
                id: descriptor.id.clone(),
                params: descriptor
                    .params
                    .iter()
                    .map(|param| (param.key.clone(), param.default))
                    .collect(),
            };
            let bare_out = compute(&[bare], &data);
            let explicit_out = compute(&[explicit], &data);
            assert_eq!(
                format!("{bare_out:?}"),
                format!("{explicit_out:?}"),
                "descriptor '{}' defaults drifted from compute",
                descriptor.id
            );
        }
    }

    fn data(values: &[f64]) -> Ohlcv<'_> {
        Ohlcv {
            high: values,
            low: values,
            close: values,
            volume: values,
        }
    }

    fn instance(id: &str, params: &[(&str, f64)]) -> Instance {
        Instance {
            id: id.to_owned(),
            params: params.iter().map(|(key, value)| ((*key).to_owned(), *value)).collect(),
        }
    }

    #[test]
    fn descriptors_cover_every_indicator() {
        let ids: BTreeSet<&str> = descriptors().iter().map(|descriptor| descriptor.id.as_str()).collect();
        assert_eq!(ids.len(), IndicatorId::COUNT);
        for id in ids {
            let parsed = id.parse::<IndicatorId>().expect("descriptor id");
            assert_eq!(parsed.as_ref(), id);
        }
    }

    #[test]
    fn descriptor_domains_distinguish_numeric_families() {
        let domain = |id: &str| {
            descriptors()
                .iter()
                .find(|descriptor| descriptor.id == id)
                .map(|descriptor| descriptor.output_domain)
                .expect("descriptor domain")
        };
        assert_eq!(domain("sma"), ValueDomain::Price);
        assert_eq!(domain("atr"), ValueDomain::PriceDelta);
        assert_eq!(domain("obv"), ValueDomain::Volume);
        assert_eq!(domain("rsi"), ValueDomain::BoundedOscillator);
        assert_eq!(domain("rsiDivergence"), ValueDomain::BoundedOscillator);
        assert_eq!(domain("macd"), ValueDomain::UnboundedOscillator);
        assert_eq!(domain("macdDivergence"), ValueDomain::UnboundedOscillator);

        let supports_signals = |id: &str| {
            descriptors()
                .iter()
                .find(|descriptor| descriptor.id == id)
                .map(|descriptor| descriptor.supports_signals)
                .expect("descriptor signal capability")
        };
        assert!(supports_signals("rsi"));
        assert!(!supports_signals("vwap"));
        assert!(!supports_signals("obv"));
        assert!(!supports_signals("psar"));
        assert!(!supports_signals("supertrend"));
    }

    #[test]
    fn resolving_an_instance_fills_every_descriptor_default() {
        let resolved = resolve_instance(&instance("macd", &[("fast", 8.0)])).expect("valid partial instance");
        assert_eq!(
            resolved.params,
            BTreeMap::from([
                ("fast".to_owned(), 8.0),
                ("signal".to_owned(), 9.0),
                ("slow".to_owned(), 26.0),
            ])
        );
    }

    #[test]
    fn every_descriptor_computes_full_length_outputs() {
        let values = series();
        let ohlcv = data(&values);
        for descriptor in descriptors() {
            let instance = Instance {
                id: descriptor.id.clone(),
                params: descriptor
                    .params
                    .iter()
                    .map(|param| (param.key.clone(), param.default))
                    .collect(),
            };
            let computed = compute(&[instance], &ohlcv);
            let outputs = &computed.first().expect("one instance").outputs;
            assert_eq!(outputs.len(), descriptor.outputs.len(), "{} output count", descriptor.id);
            for series in outputs {
                assert_eq!(series.values.len(), values.len(), "{}/{} length", descriptor.id, series.key);
            }
        }
    }

    #[test]
    fn validates_every_descriptor_boundary_and_step() {
        for descriptor in descriptors() {
            assert!(validate_instance(&instance(&descriptor.id, &[])).is_ok());
            for parameter in &descriptor.params {
                for value in [parameter.min, parameter.default, parameter.max] {
                    assert!(
                        validate_instance(&instance(&descriptor.id, &[(parameter.key.as_str(), value)])).is_ok(),
                        "{}/{} should accept {value}",
                        descriptor.id,
                        parameter.key,
                    );
                }
            }
        }

        assert!(validate_instance(&instance("bollinger", &[("mult", 2.1)])).is_ok());
        assert!(validate_instance(&instance("psar", &[("step", 0.07)])).is_ok());

        assert!(matches!(
            validate_instance(&instance("nope", &[])),
            Err(InstanceError::UnknownIndicator(indicator)) if indicator == "nope"
        ));
        assert!(matches!(
            validate_instance(&instance("sma", &[("typo", 20.0)])),
            Err(InstanceError::UnknownParameter { parameter, .. }) if parameter == "typo"
        ));
        assert!(matches!(
            validate_instance(&instance("sma", &[("period", f64::INFINITY)])),
            Err(InstanceError::NonFiniteParameter { parameter, .. }) if parameter == "period"
        ));
        assert!(matches!(
            validate_instance(&instance("sma", &[("period", 501.0)])),
            Err(InstanceError::ParameterOutOfRange { parameter, .. }) if parameter == "period"
        ));
        assert!(matches!(
            validate_instance(&instance("sma", &[("period", 14.5)])),
            Err(InstanceError::ParameterOffStep { parameter, .. }) if parameter == "period"
        ));
    }

    #[test]
    fn requirements_match_each_series_first_defined_output() {
        let values = series();
        for descriptor in descriptors()
            .iter()
            .filter(|descriptor| descriptor.kind == DescriptorKind::Series)
        {
            let instance = instance(&descriptor.id, &[]);
            for output in &descriptor.outputs {
                let required = series_input_requirement(&instance, &output.key).expect("requirement metadata");
                let exact = values.get(..required).expect("default requirement fits fixture");
                let computed = compute(slice::from_ref(&instance), &data(exact));
                let output_values = computed
                    .first()
                    .and_then(|result| result.outputs.iter().find(|candidate| candidate.key == output.key))
                    .map(|series| series.values.as_slice())
                    .expect("descriptor output");
                assert!(
                    output_values.last().is_some_and(|value| value.is_finite()),
                    "{}/{} should first be defined with {required} candles",
                    descriptor.id,
                    output.key,
                );

                let shorter = exact.get(..required - 1).unwrap_or_default();
                let computed = compute(slice::from_ref(&instance), &data(shorter));
                let has_value = computed
                    .first()
                    .and_then(|result| result.outputs.iter().find(|candidate| candidate.key == output.key))
                    .is_some_and(|series| series.values.iter().any(|value| value.is_finite()));
                assert!(
                    !has_value,
                    "{}/{} should be undefined one candle earlier",
                    descriptor.id, output.key
                );
            }
        }
    }

    #[test]
    fn requirements_follow_output_specific_parameters() {
        let macd = instance("macd", &[("fast", 30.0), ("slow", 10.0), ("signal", 5.0)]);
        assert_eq!(series_input_requirement(&macd, "macd").expect("MACD line"), 30);
        assert_eq!(series_input_requirement(&macd, "signal").expect("MACD signal"), 34);
        assert_eq!(series_input_requirement(&macd, "histogram").expect("MACD histogram"), 34);

        let stochastic = instance("stochastic", &[("period", 14.0), ("smoothD", 3.0)]);
        assert_eq!(series_input_requirement(&stochastic, "k").expect("stochastic K"), 14);
        assert_eq!(series_input_requirement(&stochastic, "d").expect("stochastic D"), 16);

        let adx = instance("adx", &[("period", 14.0)]);
        assert_eq!(series_input_requirement(&adx, "plusDi").expect("plus DI"), 15);
        assert_eq!(series_input_requirement(&adx, "adx").expect("ADX"), 28);

        let divergence = divergence_input(&instance("rsiDivergence", &[])).expect("divergence input");
        assert_eq!(divergence.oscillator, 71);
        assert_eq!(divergence.pivot_left, 5);
        assert_eq!(divergence.pivot_right, 5);
        assert_eq!(divergence.max_gap, 60);
    }

    #[test]
    fn signal_requirements_pad_exponentially_seeded_indicators() {
        let padded = |id: &str, params: &[(&str, f64)], output: &str| {
            signal_input_requirement(&instance(id, params), output).expect("signal requirement")
        };
        assert_eq!(padded("sma", &[("period", 20.0)], "sma"), 20);
        assert_eq!(padded("stochastic", &[], "d"), 16);
        assert_eq!(padded("ema", &[("period", 20.0)], "ema"), 100);
        assert_eq!(padded("rsi", &[("period", 14.0)], "rsi"), 71);
        assert_eq!(padded("atr", &[("period", 14.0)], "atr"), 70);
        assert_eq!(padded("adx", &[("period", 14.0)], "adx"), 84);
        assert_eq!(padded("adx", &[("period", 14.0)], "plusDi"), 71);
        assert_eq!(padded("macd", &[], "macd"), 130);
        assert_eq!(padded("macd", &[], "signal"), 174);
        assert_eq!(padded("macd", &[], "histogram"), 174);

        let divergence = divergence_input(&instance("macdDivergence", &[])).expect("divergence input");
        assert_eq!(divergence.oscillator, 130);
    }

    #[test]
    fn unknown_id_yields_no_outputs() {
        let values = series();
        let ohlcv = data(&values);
        let computed = compute(
            &[Instance {
                id: "nope".to_owned(),
                params: BTreeMap::new(),
            }],
            &ohlcv,
        );
        let output = computed.first().expect("one instance");
        assert!(output.outputs.is_empty() && output.divergences.is_empty());
    }

    #[test]
    fn rsi_divergence_matches_detect_divergences_and_has_no_series() {
        let close: Vec<f64> = (0..120)
            .map(|index| 100.0 + (f64::from(index) / 7.0).sin() * 15.0 - f64::from(index) * 0.05)
            .collect();
        let ohlcv = data(&close);
        let computed = compute(
            &[Instance {
                id: "rsiDivergence".to_owned(),
                params: BTreeMap::new(),
            }],
            &ohlcv,
        );
        let output = computed.first().expect("one instance");
        assert!(output.outputs.is_empty(), "a divergence indicator has no series outputs");
        let expected = detect_divergences(&close, &rsi(&close, 14), 5, 5, 60);
        assert_eq!(output.divergences.len(), expected.len());
        for (span, divergence) in output.divergences.iter().zip(&expected) {
            assert_eq!(span.from_index, divergence.prior_index);
            assert_eq!(span.to_index, divergence.recent_index);
        }
    }
}
