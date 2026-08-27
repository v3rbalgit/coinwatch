//! Typed indicator-preset commands over the opaque storage records.

use std::collections::BTreeMap;
use std::sync::Arc;

use indicators::{Instance, resolve_instance};
use serde::Serialize;
use storage::{IndicatorPresetRecord, SqliteStore, StoreError};
use thiserror::Error;
use uuid::Uuid;

use crate::naming::normalized_name;

const ID_GENERATION_ATTEMPTS: usize = 2;

#[derive(Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "type", rename_all = "camelCase", rename_all_fields = "camelCase")]
pub(crate) enum IndicatorPresetIssue {
    InvalidPreset { message: String },
}

#[derive(Debug, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "camelCase", rename_all_fields = "camelCase")]
pub(crate) enum IndicatorPresetEntry {
    Ready {
        id: String,
        indicator_id: String,
        name: String,
        params: BTreeMap<String, f64>,
    },
    Unavailable {
        id: String,
        indicator_id: String,
        name: String,
        issue: IndicatorPresetIssue,
    },
}

#[derive(Debug, PartialEq, Eq, Error, Serialize)]
#[serde(tag = "type", rename_all = "camelCase", rename_all_fields = "camelCase")]
pub(crate) enum IndicatorPresetError {
    #[error("an indicator preset name cannot be empty")]
    InvalidName,
    #[error("indicator preset is invalid: {message}")]
    InvalidPreset { message: String },
    #[error("a preset named '{name}' already exists for indicator '{indicator_id}'")]
    DuplicateName { indicator_id: String, name: String },
    #[error("no indicator preset with id '{id}' exists")]
    NotFound { id: String },
    #[error("indicator preset storage is unavailable")]
    Storage,
}

fn normalize_name(name: String) -> Result<String, IndicatorPresetError> {
    normalized_name(&name).ok_or(IndicatorPresetError::InvalidName)
}

fn map_store_error(error: StoreError) -> IndicatorPresetError {
    match error {
        StoreError::DuplicatePresetName { indicator_id, name } => {
            IndicatorPresetError::DuplicateName { indicator_id, name }
        }
        StoreError::PresetNotFound(id) => IndicatorPresetError::NotFound { id },
        error => {
            tracing::error!(%error, "indicator preset storage operation failed");
            IndicatorPresetError::Storage
        }
    }
}

fn resolve_params(
    indicator_id: &str,
    params: BTreeMap<String, f64>,
) -> Result<BTreeMap<String, f64>, IndicatorPresetError> {
    let instance = Instance {
        id: indicator_id.to_owned(),
        params,
    };
    resolve_instance(&instance)
        .map(|resolved| resolved.params)
        .map_err(|error| IndicatorPresetError::InvalidPreset {
            message: error.to_string(),
        })
}

fn decode_record(record: IndicatorPresetRecord) -> IndicatorPresetEntry {
    let IndicatorPresetRecord {
        id,
        indicator_id,
        name,
        params,
    } = record;
    let decoded = serde_json::from_str::<BTreeMap<String, f64>>(&params).map_err(|error| error.to_string());
    let resolved = decoded.and_then(|params| resolve_params(&indicator_id, params).map_err(|error| error.to_string()));
    match resolved {
        Ok(params) => IndicatorPresetEntry::Ready {
            id,
            indicator_id,
            name,
            params,
        },
        Err(message) => IndicatorPresetEntry::Unavailable {
            id,
            indicator_id,
            name,
            issue: IndicatorPresetIssue::InvalidPreset { message },
        },
    }
}

async fn list_from_store(store: &SqliteStore) -> Result<Vec<IndicatorPresetEntry>, IndicatorPresetError> {
    store
        .indicator_presets()
        .await
        .map(|records| records.into_iter().map(decode_record).collect())
        .map_err(map_store_error)
}

async fn create_in_store(
    store: &SqliteStore,
    indicator_id: String,
    name: String,
    params: BTreeMap<String, f64>,
) -> Result<IndicatorPresetEntry, IndicatorPresetError> {
    let name = normalize_name(name)?;
    let params = resolve_params(&indicator_id, params)?;
    let encoded = serde_json::to_string(&params).map_err(|error| IndicatorPresetError::InvalidPreset {
        message: error.to_string(),
    })?;
    for _ in 0..ID_GENERATION_ATTEMPTS {
        let id = Uuid::new_v4().to_string();
        match store.create_indicator_preset(&id, &indicator_id, &name, &encoded).await {
            Ok(()) => {
                return Ok(IndicatorPresetEntry::Ready {
                    id,
                    indicator_id,
                    name,
                    params,
                });
            }
            Err(StoreError::DuplicatePresetId(_)) => {}
            Err(error) => return Err(map_store_error(error)),
        }
    }
    tracing::error!("failed to generate a unique indicator preset id");
    Err(IndicatorPresetError::Storage)
}

async fn update_in_store(
    store: &SqliteStore,
    id: String,
    name: String,
    params: BTreeMap<String, f64>,
) -> Result<IndicatorPresetEntry, IndicatorPresetError> {
    let name = normalize_name(name)?;
    let record = store
        .indicator_preset(&id)
        .await
        .map_err(map_store_error)?
        .ok_or_else(|| IndicatorPresetError::NotFound { id: id.clone() })?;
    let indicator_id = record.indicator_id;
    let params = resolve_params(&indicator_id, params)?;
    let encoded = serde_json::to_string(&params).map_err(|error| IndicatorPresetError::InvalidPreset {
        message: error.to_string(),
    })?;
    store
        .update_indicator_preset(&id, &indicator_id, &name, &encoded)
        .await
        .map_err(map_store_error)?;
    Ok(IndicatorPresetEntry::Ready {
        id,
        indicator_id,
        name,
        params,
    })
}

async fn delete_from_store(store: &SqliteStore, id: String) -> Result<(), IndicatorPresetError> {
    store.delete_indicator_preset(&id).await.map_err(map_store_error)
}

#[tauri::command]
pub(crate) async fn list_indicator_presets(
    store: tauri::State<'_, Arc<SqliteStore>>,
) -> Result<Vec<IndicatorPresetEntry>, IndicatorPresetError> {
    list_from_store(store.inner()).await
}

#[tauri::command]
pub(crate) async fn create_indicator_preset(
    store: tauri::State<'_, Arc<SqliteStore>>,
    indicator_id: String,
    name: String,
    params: BTreeMap<String, f64>,
) -> Result<IndicatorPresetEntry, IndicatorPresetError> {
    create_in_store(store.inner(), indicator_id, name, params).await
}

#[tauri::command]
pub(crate) async fn update_indicator_preset(
    store: tauri::State<'_, Arc<SqliteStore>>,
    id: String,
    name: String,
    params: BTreeMap<String, f64>,
) -> Result<IndicatorPresetEntry, IndicatorPresetError> {
    update_in_store(store.inner(), id, name, params).await
}

#[tauri::command]
pub(crate) async fn delete_indicator_preset(
    store: tauri::State<'_, Arc<SqliteStore>>,
    id: String,
) -> Result<(), IndicatorPresetError> {
    delete_from_store(store.inner(), id).await
}

#[cfg(test)]
mod tests;
