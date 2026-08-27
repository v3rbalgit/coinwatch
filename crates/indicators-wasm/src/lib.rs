//! WebAssembly boundary for the shared [`indicators`] engine.
//!
//! The WebView reads the native descriptor registry and computes configured
//! indicator instances through the same engine the signal evaluator uses directly.

use indicators::{Instance, Ohlcv, compute, descriptors, resolve_instance};
use wasm_bindgen::prelude::{JsValue, wasm_bindgen};

/// Return the indicator registry that drives chart and signal-editor controls.
#[wasm_bindgen(js_name = indicatorDescriptors)]
pub fn indicator_descriptors() -> Result<JsValue, JsValue> {
    serde_wasm_bindgen::to_value(descriptors()).map_err(JsValue::from)
}

/// Compute validated indicator instances over equally sized OHLCV columns.
#[wasm_bindgen(js_name = computeIndicators)]
pub fn compute_indicators(
    instances: JsValue,
    high: &[f64],
    low: &[f64],
    close: &[f64],
    volume: &[f64],
) -> Result<JsValue, JsValue> {
    if high.len() != low.len() || high.len() != close.len() || high.len() != volume.len() {
        return Err(JsValue::from_str("OHLCV columns must be equally sized"));
    }
    let instances: Vec<Instance> = serde_wasm_bindgen::from_value(instances).map_err(JsValue::from)?;
    let instances = instances
        .iter()
        .map(|instance| resolve_instance(instance).map_err(|error| JsValue::from_str(&error.to_string())))
        .collect::<Result<Vec<_>, JsValue>>()?;
    let data = Ohlcv {
        high,
        low,
        close,
        volume,
    };
    serde_wasm_bindgen::to_value(&compute(&instances, &data)).map_err(JsValue::from)
}
