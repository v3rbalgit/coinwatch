//! Typed saved-signal catalog commands over the opaque storage records.

use std::sync::Arc;

use serde::Serialize;
use signals::{DefinitionDecodeError, SignalDefinition, decode_definition};
use storage::{SignalRecord, SqliteStore, StoreError};
use thiserror::Error;
use uuid::Uuid;

use crate::naming::normalized_name;

const ID_GENERATION_ATTEMPTS: usize = 2;

#[derive(Debug, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "camelCase", rename_all_fields = "camelCase")]
pub(crate) enum SignalCatalogIssue {
    UnsupportedVersion { version: u32 },
    InvalidDefinition { message: String },
}

#[derive(Debug, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "camelCase", rename_all_fields = "camelCase")]
pub(crate) enum SignalCatalogEntry {
    Ready {
        id: String,
        name: String,
        definition: SignalDefinition,
    },
    Unavailable {
        id: String,
        name: String,
        issue: SignalCatalogIssue,
    },
}

#[derive(Debug, PartialEq, Error, Serialize)]
#[serde(tag = "type", rename_all = "camelCase", rename_all_fields = "camelCase")]
pub(crate) enum SignalCatalogError {
    #[error("a signal name cannot be empty")]
    InvalidName,
    #[error("signal definition is invalid: {message}")]
    InvalidDefinition { message: String },
    #[error("a signal named '{name}' already exists")]
    DuplicateName { name: String },
    #[error("no signal with id '{id}' exists")]
    NotFound { id: String },
    #[error("signal catalog storage is unavailable")]
    Storage,
}

fn normalize_name(name: String) -> Result<String, SignalCatalogError> {
    normalized_name(&name).ok_or(SignalCatalogError::InvalidName)
}

fn map_store_error(error: StoreError) -> SignalCatalogError {
    match error {
        StoreError::DuplicateSignalName(name) => SignalCatalogError::DuplicateName { name },
        StoreError::SignalNotFound(id) => SignalCatalogError::NotFound { id },
        error => {
            tracing::error!(%error, "signal catalog storage operation failed");
            SignalCatalogError::Storage
        }
    }
}

fn prepare_definition(mut definition: SignalDefinition) -> Result<(SignalDefinition, String), SignalCatalogError> {
    definition
        .resolve_indicator_defaults()
        .map_err(|error| SignalCatalogError::InvalidDefinition {
            message: error.to_string(),
        })?;
    let encoded = serde_json::to_string(&definition).map_err(|error| SignalCatalogError::InvalidDefinition {
        message: error.to_string(),
    })?;
    Ok((definition, encoded))
}

fn decode_record(record: SignalRecord) -> SignalCatalogEntry {
    let SignalRecord { id, name, definition } = record;
    match decode_definition(&definition) {
        Err(DefinitionDecodeError::UnsupportedVersion(version)) => SignalCatalogEntry::Unavailable {
            id,
            name,
            issue: SignalCatalogIssue::UnsupportedVersion { version },
        },
        Err(DefinitionDecodeError::InvalidJson(error)) => SignalCatalogEntry::Unavailable {
            id,
            name,
            issue: SignalCatalogIssue::InvalidDefinition {
                message: error.to_string(),
            },
        },
        Ok(mut definition) => match definition.resolve_indicator_defaults() {
            Ok(()) => SignalCatalogEntry::Ready { id, name, definition },
            Err(error) => SignalCatalogEntry::Unavailable {
                id,
                name,
                issue: SignalCatalogIssue::InvalidDefinition {
                    message: error.to_string(),
                },
            },
        },
    }
}

async fn list_from_store(store: &SqliteStore) -> Result<Vec<SignalCatalogEntry>, SignalCatalogError> {
    store
        .signals()
        .await
        .map(|records| records.into_iter().map(decode_record).collect())
        .map_err(map_store_error)
}

async fn get_from_store(store: &SqliteStore, id: String) -> Result<SignalCatalogEntry, SignalCatalogError> {
    store
        .signal(&id)
        .await
        .map_err(map_store_error)?
        .map(decode_record)
        .ok_or(SignalCatalogError::NotFound { id })
}

async fn create_in_store(
    store: &SqliteStore,
    name: String,
    definition: SignalDefinition,
) -> Result<SignalCatalogEntry, SignalCatalogError> {
    let name = normalize_name(name)?;
    let (definition, encoded) = prepare_definition(definition)?;
    for _ in 0..ID_GENERATION_ATTEMPTS {
        let id = Uuid::new_v4().to_string();
        match store.create_signal(&id, &name, &encoded).await {
            Ok(()) => return Ok(SignalCatalogEntry::Ready { id, name, definition }),
            Err(StoreError::DuplicateSignalId(_)) => {}
            Err(error) => return Err(map_store_error(error)),
        }
    }
    tracing::error!("failed to generate a unique signal id");
    Err(SignalCatalogError::Storage)
}

async fn update_in_store(
    store: &SqliteStore,
    id: String,
    name: String,
    definition: SignalDefinition,
) -> Result<SignalCatalogEntry, SignalCatalogError> {
    let name = normalize_name(name)?;
    let (definition, encoded) = prepare_definition(definition)?;
    store.update_signal(&id, &name, &encoded).await.map_err(map_store_error)?;
    Ok(SignalCatalogEntry::Ready { id, name, definition })
}

async fn delete_from_store(store: &SqliteStore, id: String) -> Result<(), SignalCatalogError> {
    store.delete_signal(&id).await.map_err(map_store_error)
}

#[tauri::command]
pub(crate) async fn list_signals(
    store: tauri::State<'_, Arc<SqliteStore>>,
) -> Result<Vec<SignalCatalogEntry>, SignalCatalogError> {
    list_from_store(store.inner()).await
}

#[tauri::command]
pub(crate) async fn get_signal(
    store: tauri::State<'_, Arc<SqliteStore>>,
    id: String,
) -> Result<SignalCatalogEntry, SignalCatalogError> {
    get_from_store(store.inner(), id).await
}

#[tauri::command]
pub(crate) async fn create_signal(
    store: tauri::State<'_, Arc<SqliteStore>>,
    name: String,
    definition: SignalDefinition,
) -> Result<SignalCatalogEntry, SignalCatalogError> {
    create_in_store(store.inner(), name, definition).await
}

#[tauri::command]
pub(crate) async fn update_signal(
    store: tauri::State<'_, Arc<SqliteStore>>,
    id: String,
    name: String,
    definition: SignalDefinition,
) -> Result<SignalCatalogEntry, SignalCatalogError> {
    update_in_store(store.inner(), id, name, definition).await
}

#[tauri::command]
pub(crate) async fn delete_signal(
    store: tauri::State<'_, Arc<SqliteStore>>,
    id: String,
) -> Result<(), SignalCatalogError> {
    delete_from_store(store.inner(), id).await
}

#[cfg(test)]
mod tests;
