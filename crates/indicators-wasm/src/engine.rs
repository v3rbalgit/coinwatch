//! The instance-driven indicator engine behind the chart.
//!
//! [`descriptors`] lists every indicator the chart can draw (metadata for its
//! toggle UI); [`compute`] runs a set of configured instances over one OHLCV
//! series. The chart only toggles visibility and passes conventional default
//! params — indicator *configuration* lives in the future signal builder — but the
//! same instance shape (`{id, params}`) lets a fired signal later hand the chart
//! its exact conditions to draw. All math is the shared `indicators` crate, so the
//! chart can never drift from the scanner.

use std::collections::HashMap;

use indicators::{
    Adx, Bollinger, Donchian, Macd, Stochastic, Supertrend, adx, atr, bollinger, cci, donchian, ema, macd, mfi, obv,
    psar, rsi, sma, stochastic, supertrend, vwap, williams_r, wma,
};
use serde::{Deserialize, Serialize};

/// Where an indicator draws: over the price pane, or in its own pane below.
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Target {
    /// Drawn over the price/candles pane.
    Overlay,
    /// Drawn in a dedicated pane below the price.
    Pane,
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

/// A tunable parameter — surfaced with defaults now, editable in the signal builder later.
#[derive(Debug, Clone, Serialize)]
pub struct Param {
    /// Instance-params key.
    pub key: String,
    /// Human label.
    pub label: String,
    /// Conventional default value.
    pub default: f64,
    /// Minimum for a future editor.
    pub min: f64,
    /// Maximum for a future editor.
    pub max: f64,
    /// Step for a future editor.
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
    /// Default line/bar color (hex); the chart may override from its theme.
    pub color: String,
}

/// Static metadata for one indicator: drives the toggle picker and the renderer.
#[derive(Debug, Clone, Serialize)]
pub struct Descriptor {
    /// Stable id (matches `Instance.id`).
    pub id: String,
    /// Human label.
    pub label: String,
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
#[derive(Debug, Clone, Deserialize)]
pub struct Instance {
    /// Which indicator (matches a [`Descriptor::id`]).
    pub id: String,
    /// Params by key; missing keys fall back to conventional defaults.
    #[serde(default)]
    pub params: HashMap<String, f64>,
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

/// The outputs of one computed instance, in descriptor order.
#[derive(Debug, Clone, Serialize)]
pub struct InstanceOutput {
    /// One entry per plotted output.
    pub outputs: Vec<OutputSeries>,
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
    OutputSeries { key: key.to_owned(), values: nan(series) }
}

fn get(params: &HashMap<String, f64>, key: &str, default: f64) -> f64 {
    params.get(key).copied().unwrap_or(default)
}

fn period(params: &HashMap<String, f64>, key: &str, default: f64) -> usize {
    let value = get(params, key, default).max(1.0);
    #[expect(clippy::cast_possible_truncation, clippy::cast_sign_loss, reason = "periods are small positive integers")]
    let period = value as usize;
    period
}

/// Compute one instance's output series over `data`. Unknown ids yield no outputs.
fn compute_one(instance: &Instance, data: &Ohlcv) -> Vec<OutputSeries> {
    let params = &instance.params;
    let (high, low, close, volume) = (data.high, data.low, data.close, data.volume);
    match instance.id.as_str() {
        "sma" => vec![out("sma", &sma(close, period(params, "period", 20.0)))],
        "ema" => vec![out("ema", &ema(close, period(params, "period", 20.0)))],
        "wma" => vec![out("wma", &wma(close, period(params, "period", 20.0)))],
        "bollinger" => {
            let Bollinger { middle, upper, lower } = bollinger(close, period(params, "period", 20.0), get(params, "mult", 2.0));
            vec![out("upper", &upper), out("middle", &middle), out("lower", &lower)]
        }
        "vwap" => vec![out("vwap", &vwap(high, low, close, volume))],
        "donchian" => {
            let Donchian { upper, middle, lower } = donchian(high, low, period(params, "period", 20.0));
            vec![out("upper", &upper), out("middle", &middle), out("lower", &lower)]
        }
        "supertrend" => {
            let Supertrend { line, .. } = supertrend(high, low, close, period(params, "period", 10.0), get(params, "mult", 3.0));
            vec![out("supertrend", &line)]
        }
        "psar" => vec![out("psar", &psar(high, low, get(params, "step", 0.02), get(params, "max", 0.2)))],
        "rsi" => vec![out("rsi", &rsi(close, period(params, "period", 14.0)))],
        "macd" => {
            let Macd { macd: line, signal, histogram } = macd(
                close,
                period(params, "fast", 12.0),
                period(params, "slow", 26.0),
                period(params, "signal", 9.0),
            );
            vec![out("macd", &line), out("signal", &signal), out("histogram", &histogram)]
        }
        "stochastic" => {
            let Stochastic { k, d } =
                stochastic(high, low, close, period(params, "period", 14.0), period(params, "smoothD", 3.0));
            vec![out("k", &k), out("d", &d)]
        }
        "williams" => vec![out("williams", &williams_r(high, low, close, period(params, "period", 14.0)))],
        "cci" => vec![out("cci", &cci(high, low, close, period(params, "period", 20.0)))],
        "mfi" => vec![out("mfi", &mfi(high, low, close, volume, period(params, "period", 14.0)))],
        "adx" => {
            let Adx { adx: line, plus_di, minus_di } = adx(high, low, close, period(params, "period", 14.0));
            vec![out("adx", &line), out("plusDi", &plus_di), out("minusDi", &minus_di)]
        }
        "atr" => vec![out("atr", &atr(high, low, close, period(params, "period", 14.0)))],
        "obv" => vec![out("obv", &obv(close, volume))],
        _ => Vec::new(),
    }
}

/// Compute every instance's outputs over one OHLCV series.
#[must_use]
pub fn compute(instances: &[Instance], data: &Ohlcv) -> Vec<InstanceOutput> {
    instances.iter().map(|instance| InstanceOutput { outputs: compute_one(instance, data) }).collect()
}

fn param(key: &str, label: &str, default: f64, min: f64, max: f64, step: f64) -> Param {
    Param { key: key.to_owned(), label: label.to_owned(), default, min, max, step }
}

fn line(key: &str, label: &str, color: &str) -> Output {
    Output { key: key.to_owned(), label: label.to_owned(), kind: OutputKind::Line, color: color.to_owned() }
}

fn descriptor(id: &str, label: &str, target: Target, params: Vec<Param>, outputs: Vec<Output>) -> Descriptor {
    Descriptor { id: id.to_owned(), label: label.to_owned(), target, params, outputs, guides: Vec::new(), range: None }
}

/// The registry of indicators the chart can toggle, with conventional defaults.
#[must_use]
pub fn descriptors() -> Vec<Descriptor> {
    let period = |default: f64| param("period", "Period", default, 1.0, 500.0, 1.0);
    vec![
        descriptor("sma", "SMA", Target::Overlay, vec![period(20.0)], vec![line("sma", "SMA", "#3b82f6")]),
        descriptor("ema", "EMA", Target::Overlay, vec![period(20.0)], vec![line("ema", "EMA", "#f59e0b")]),
        descriptor("wma", "WMA", Target::Overlay, vec![period(20.0)], vec![line("wma", "WMA", "#a855f7")]),
        descriptor(
            "bollinger",
            "Bollinger Bands",
            Target::Overlay,
            vec![period(20.0), param("mult", "Std. dev", 2.0, 0.5, 5.0, 0.1)],
            vec![
                line("upper", "Upper", "#6b7280"),
                line("middle", "Basis", "#3b82f6"),
                line("lower", "Lower", "#6b7280"),
            ],
        ),
        descriptor("vwap", "VWAP", Target::Overlay, Vec::new(), vec![line("vwap", "VWAP", "#eab308")]),
        descriptor(
            "donchian",
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
            "supertrend",
            "Supertrend",
            Target::Overlay,
            vec![param("period", "ATR period", 10.0, 1.0, 100.0, 1.0), param("mult", "Multiplier", 3.0, 0.5, 10.0, 0.1)],
            vec![line("supertrend", "Supertrend", "#22c55e")],
        ),
        descriptor(
            "psar",
            "Parabolic SAR",
            Target::Overlay,
            vec![param("step", "Step", 0.02, 0.01, 0.2, 0.01), param("max", "Max step", 0.2, 0.05, 1.0, 0.05)],
            vec![line("psar", "PSAR", "#e879f9")],
        ),
        Descriptor {
            guides: vec![30.0, 70.0],
            range: Some([0.0, 100.0]),
            ..descriptor("rsi", "RSI", Target::Pane, vec![period(14.0)], vec![line("rsi", "RSI", "#c084fc")])
        },
        {
            let mut macd = descriptor(
                "macd",
                "MACD",
                Target::Pane,
                vec![
                    param("fast", "Fast", 12.0, 1.0, 100.0, 1.0),
                    param("slow", "Slow", 26.0, 1.0, 200.0, 1.0),
                    param("signal", "Signal", 9.0, 1.0, 100.0, 1.0),
                ],
                vec![line("macd", "MACD", "#3b82f6"), line("signal", "Signal", "#f59e0b")],
            );
            macd.outputs.push(Output {
                key: "histogram".to_owned(),
                label: "Histogram".to_owned(),
                kind: OutputKind::Histogram,
                color: "#6b7280".to_owned(),
            });
            macd.guides = vec![0.0];
            macd
        },
        Descriptor {
            guides: vec![20.0, 80.0],
            range: Some([0.0, 100.0]),
            ..descriptor(
                "stochastic",
                "Stochastic",
                Target::Pane,
                vec![period(14.0), param("smoothD", "%D smoothing", 3.0, 1.0, 50.0, 1.0)],
                vec![line("k", "%K", "#3b82f6"), line("d", "%D", "#f59e0b")],
            )
        },
        Descriptor {
            guides: vec![-20.0, -80.0],
            range: Some([-100.0, 0.0]),
            ..descriptor("williams", "Williams %R", Target::Pane, vec![period(14.0)], vec![line("williams", "%R", "#c084fc")])
        },
        Descriptor {
            guides: vec![-100.0, 100.0],
            ..descriptor("cci", "CCI", Target::Pane, vec![period(20.0)], vec![line("cci", "CCI", "#c084fc")])
        },
        Descriptor {
            guides: vec![20.0, 80.0],
            range: Some([0.0, 100.0]),
            ..descriptor("mfi", "MFI", Target::Pane, vec![period(14.0)], vec![line("mfi", "MFI", "#22c55e")])
        },
        Descriptor {
            range: Some([0.0, 100.0]),
            ..descriptor(
                "adx",
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
        descriptor("atr", "ATR", Target::Pane, vec![period(14.0)], vec![line("atr", "ATR", "#f59e0b")]),
        descriptor("obv", "OBV", Target::Pane, Vec::new(), vec![line("obv", "OBV", "#22c55e")]),
    ]
}

#[cfg(test)]
mod tests {
    use super::{Instance, Ohlcv, compute, descriptors};
    use std::collections::HashMap;

    fn series() -> Vec<f64> {
        (1..=60).map(f64::from).collect()
    }

    #[test]
    fn descriptors_cover_the_seventeen_indicators() {
        let ids: Vec<String> = descriptors().into_iter().map(|descriptor| descriptor.id).collect();
        assert_eq!(ids.len(), 17);
        for id in ["sma", "ema", "wma", "bollinger", "vwap", "donchian", "supertrend", "psar", "rsi", "macd", "obv"] {
            assert!(ids.iter().any(|candidate| candidate == id), "missing descriptor {id}");
        }
    }

    #[test]
    fn every_descriptor_computes_full_length_outputs() {
        let values = series();
        let data = Ohlcv { high: &values, low: &values, close: &values, volume: &values };
        for descriptor in descriptors() {
            let instance = Instance {
                id: descriptor.id.clone(),
                params: descriptor.params.iter().map(|param| (param.key.clone(), param.default)).collect(),
            };
            let computed = compute(&[instance], &data);
            let outputs = &computed.first().expect("one instance").outputs;
            assert_eq!(outputs.len(), descriptor.outputs.len(), "{} output count", descriptor.id);
            for series in outputs {
                assert_eq!(series.values.len(), values.len(), "{}/{} length", descriptor.id, series.key);
            }
        }
    }

    #[test]
    fn unknown_id_yields_no_outputs() {
        let values = series();
        let data = Ohlcv { high: &values, low: &values, close: &values, volume: &values };
        let computed = compute(&[Instance { id: "nope".to_owned(), params: HashMap::new() }], &data);
        assert!(computed.first().expect("one instance").outputs.is_empty());
    }
}
