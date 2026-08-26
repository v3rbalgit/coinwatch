use std::collections::BTreeMap;

use serde_json::json;
use storage::SqliteStore;
use tempfile::TempDir;
use uuid::Uuid;

use super::{
    IndicatorPresetEntry, IndicatorPresetError, IndicatorPresetIssue, create_in_store, delete_from_store,
    list_from_store, update_in_store,
};

fn params(period: f64) -> BTreeMap<String, f64> {
    BTreeMap::from([("period".to_owned(), period)])
}

async fn store() -> (TempDir, SqliteStore) {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = SqliteStore::open(&dir.path().join("cw.db")).await.expect("open store");
    (dir, store)
}

#[tokio::test]
async fn create_resolves_defaults_generates_id_and_persists_typed_params() {
    let (_dir, store) = store().await;
    let entry = create_in_store(&store, "rsi".to_owned(), "  Standard  ".to_owned(), BTreeMap::new())
        .await
        .expect("create preset");
    let IndicatorPresetEntry::Ready {
        id,
        indicator_id,
        name,
        params: resolved,
    } = entry
    else {
        panic!("created preset is ready")
    };

    assert!(Uuid::parse_str(&id).is_ok());
    assert_eq!(indicator_id, "rsi");
    assert_eq!(name, "Standard");
    assert_eq!(resolved, params(14.0));
    let record = store
        .indicator_preset(&id)
        .await
        .expect("stored preset")
        .expect("preset exists");
    assert_eq!(
        serde_json::from_str::<BTreeMap<String, f64>>(&record.params).expect("typed params"),
        resolved
    );
}

#[tokio::test]
async fn writes_reject_invalid_names_indicators_params_and_duplicates() {
    let (_dir, store) = store().await;
    assert_eq!(
        create_in_store(&store, "rsi".to_owned(), "   ".to_owned(), params(14.0)).await,
        Err(IndicatorPresetError::InvalidName)
    );
    assert!(matches!(
        create_in_store(&store, "unknown".to_owned(), "Unknown".to_owned(), BTreeMap::new()).await,
        Err(IndicatorPresetError::InvalidPreset { .. })
    ));
    assert!(matches!(
        create_in_store(&store, "rsi".to_owned(), "Invalid".to_owned(), params(0.0)).await,
        Err(IndicatorPresetError::InvalidPreset { .. })
    ));

    create_in_store(&store, "rsi".to_owned(), "Fast".to_owned(), params(7.0))
        .await
        .expect("create RSI preset");
    create_in_store(&store, "ema".to_owned(), "Fast".to_owned(), params(10.0))
        .await
        .expect("same name on another indicator");
    assert!(matches!(
        create_in_store(&store, "rsi".to_owned(), " fast ".to_owned(), params(5.0)).await,
        Err(IndicatorPresetError::DuplicateName { indicator_id, name })
            if indicator_id == "rsi" && name == "fast"
    ));
}

#[tokio::test]
async fn update_is_distinct_preserves_indicator_and_delete_is_idempotent() {
    let (_dir, store) = store().await;
    assert!(matches!(
        update_in_store(&store, "missing".to_owned(), "Missing".to_owned(), params(14.0)).await,
        Err(IndicatorPresetError::NotFound { id }) if id == "missing"
    ));

    let created = create_in_store(&store, "rsi".to_owned(), "Original".to_owned(), params(14.0))
        .await
        .expect("create preset");
    let IndicatorPresetEntry::Ready { id, .. } = created else {
        panic!("created preset is ready")
    };
    let updated = update_in_store(&store, id.clone(), "Quick".to_owned(), params(5.0))
        .await
        .expect("update preset");
    assert!(matches!(
        updated,
        IndicatorPresetEntry::Ready {
            indicator_id,
            name,
            params: updated,
            ..
        } if indicator_id == "rsi" && name == "Quick" && updated == params(5.0)
    ));

    delete_from_store(&store, id.clone()).await.expect("delete preset");
    delete_from_store(&store, id.clone()).await.expect("delete missing preset");
    assert!(store.indicator_preset(&id).await.expect("get deleted preset").is_none());
}

#[tokio::test]
async fn listing_is_resilient_and_resolves_defaults_for_ready_rows() {
    let (_dir, store) = store().await;
    store
        .create_indicator_preset("ready", "rsi", "Alpha", "{}")
        .await
        .expect("insert partial ready row");
    store
        .create_indicator_preset("corrupt", "rsi", "Beta", "not JSON")
        .await
        .expect("insert corrupt row");
    store
        .create_indicator_preset("invalid", "rsi", "Gamma", r#"{"period":0}"#)
        .await
        .expect("insert invalid row");

    let entries = list_from_store(&store).await.expect("list presets");
    assert_eq!(entries.len(), 3);
    assert!(entries.iter().any(|entry| matches!(
        entry,
        IndicatorPresetEntry::Ready {
            id,
            params: resolved,
            ..
        } if id == "ready" && resolved == &params(14.0)
    )));
    for id in ["corrupt", "invalid"] {
        assert!(entries.iter().any(|entry| matches!(
            entry,
            IndicatorPresetEntry::Unavailable {
                id: entry_id,
                issue: IndicatorPresetIssue::InvalidPreset { .. },
                ..
            } if entry_id == id
        )));
    }
}

#[test]
fn entry_and_error_serialization_are_stable() {
    assert_eq!(
        serde_json::to_value(IndicatorPresetEntry::Unavailable {
            id: "old".to_owned(),
            indicator_id: "rsi".to_owned(),
            name: "Old".to_owned(),
            issue: IndicatorPresetIssue::InvalidPreset {
                message: "invalid params".to_owned(),
            },
        })
        .expect("serialize entry"),
        json!({
            "status": "unavailable",
            "id": "old",
            "indicatorId": "rsi",
            "name": "Old",
            "issue": { "type": "invalidPreset", "message": "invalid params" },
        })
    );
    assert_eq!(
        serde_json::to_value(IndicatorPresetError::DuplicateName {
            indicator_id: "rsi".to_owned(),
            name: "Fast".to_owned(),
        })
        .expect("serialize error"),
        json!({
            "type": "duplicateName",
            "indicatorId": "rsi",
            "name": "Fast",
        })
    );
}
