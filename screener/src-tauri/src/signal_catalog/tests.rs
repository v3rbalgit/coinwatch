use std::collections::BTreeMap;

use domain::Timeframe;
use indicators::Instance;
use serde_json::json;
use signals::{
    CandleField, ComparisonOp, Condition, DEFINITION_VERSION, Direction, Node, Operand, Predicate, SignalDefinition,
};
use storage::SqliteStore;
use tempfile::TempDir;
use uuid::Uuid;

use super::{
    SignalCatalogEntry, SignalCatalogError, SignalCatalogIssue, create_in_store, delete_from_store, get_from_store,
    list_from_store, update_in_store,
};

fn definition(value: f64) -> SignalDefinition {
    SignalDefinition {
        version: DEFINITION_VERSION,
        direction: Direction::Bullish,
        root: Node::Condition { id: "price".to_owned() },
        conditions: BTreeMap::from([(
            "price".to_owned(),
            Condition {
                timeframe: Timeframe::Hour1,
                predicate: Predicate::Threshold {
                    left: Operand::Field {
                        field: CandleField::Close,
                        adjustment: None,
                    },
                    op: ComparisonOp::GreaterThan,
                    right: Operand::Constant { value },
                },
                within_bars: 0,
            },
        )]),
    }
}

async fn store() -> (TempDir, SqliteStore) {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = SqliteStore::open(&dir.path().join("cw.db")).await.expect("open store");
    (dir, store)
}

#[tokio::test]
async fn create_normalizes_name_generates_id_and_persists_typed_definition() {
    let (_dir, store) = store().await;
    let entry = create_in_store(&store, "  Breakout  ".to_owned(), definition(100.0))
        .await
        .expect("create signal");
    let SignalCatalogEntry::Ready {
        id,
        name,
        definition: stored,
    } = entry
    else {
        panic!("created signal is ready")
    };

    assert!(Uuid::parse_str(&id).is_ok());
    assert_eq!(name, "Breakout");
    assert_eq!(stored, definition(100.0));
    assert!(store.signal(&id).await.expect("stored signal").is_some());
    assert!(matches!(
        get_from_store(&store, id).await.expect("get signal"),
        SignalCatalogEntry::Ready { .. }
    ));
}

#[tokio::test]
async fn create_resolves_indicator_defaults_before_persistence() {
    let (_dir, store) = store().await;
    let signal = SignalDefinition {
        version: DEFINITION_VERSION,
        direction: Direction::Bullish,
        root: Node::Condition { id: "trend".to_owned() },
        conditions: BTreeMap::from([(
            "trend".to_owned(),
            Condition {
                timeframe: Timeframe::Hour1,
                predicate: Predicate::Threshold {
                    left: Operand::Indicator {
                        instance: Instance {
                            id: "macd".to_owned(),
                            params: BTreeMap::from([("fast".to_owned(), 8.0)]),
                        },
                        output: "macd".to_owned(),
                        adjustment: None,
                    },
                    op: ComparisonOp::GreaterThan,
                    right: Operand::Constant { value: 0.0 },
                },
                within_bars: 0,
            },
        )]),
    };

    let entry = create_in_store(&store, "Trend".to_owned(), signal)
        .await
        .expect("create signal");
    let SignalCatalogEntry::Ready { definition, .. } = entry else {
        panic!("created signal is ready")
    };
    let Predicate::Threshold {
        left: Operand::Indicator { instance, .. },
        ..
    } = &definition.conditions["trend"].predicate
    else {
        panic!("indicator operand")
    };
    assert_eq!(
        instance.params,
        BTreeMap::from([
            ("fast".to_owned(), 8.0),
            ("signal".to_owned(), 9.0),
            ("slow".to_owned(), 26.0),
        ])
    );
}

#[tokio::test]
async fn writes_reject_invalid_definitions_names_and_duplicates() {
    let (_dir, store) = store().await;
    let mut invalid = definition(100.0);
    invalid.conditions.clear();
    assert!(matches!(
        create_in_store(&store, "Invalid".to_owned(), invalid).await,
        Err(SignalCatalogError::InvalidDefinition { .. })
    ));
    assert_eq!(
        create_in_store(&store, "   ".to_owned(), definition(100.0)).await,
        Err(SignalCatalogError::InvalidName)
    );

    create_in_store(&store, "Alpha".to_owned(), definition(100.0))
        .await
        .expect("create first signal");
    assert!(matches!(
        create_in_store(&store, " alpha ".to_owned(), definition(200.0)).await,
        Err(SignalCatalogError::DuplicateName { name }) if name == "alpha"
    ));
}

#[tokio::test]
async fn update_is_distinct_from_create_and_delete_is_idempotent() {
    let (_dir, store) = store().await;
    assert!(matches!(
        update_in_store(
            &store,
            "missing".to_owned(),
            "Missing".to_owned(),
            definition(100.0),
        )
        .await,
        Err(SignalCatalogError::NotFound { id }) if id == "missing"
    ));

    let created = create_in_store(&store, "Original".to_owned(), definition(100.0))
        .await
        .expect("create signal");
    let SignalCatalogEntry::Ready { id, .. } = created else {
        panic!("created signal is ready")
    };
    let updated = update_in_store(&store, id.clone(), "Updated".to_owned(), definition(200.0))
        .await
        .expect("update signal");
    assert!(matches!(
        updated,
        SignalCatalogEntry::Ready {
            name,
            definition: updated,
            ..
        } if name == "Updated" && updated == definition(200.0)
    ));

    delete_from_store(&store, id.clone()).await.expect("delete signal");
    delete_from_store(&store, id.clone()).await.expect("delete missing signal");
    assert_eq!(
        get_from_store(&store, id.clone()).await,
        Err(SignalCatalogError::NotFound { id })
    );
}

#[tokio::test]
async fn listing_is_resilient_to_unsupported_corrupt_and_invalid_rows() {
    let (_dir, store) = store().await;
    let ready = definition(100.0);
    store
        .create_signal(
            "ready",
            "Alpha",
            &serde_json::to_string(&ready).expect("serialize ready definition"),
        )
        .await
        .expect("insert ready row");
    store
        .create_signal("old", "Beta", r#"{"version":1}"#)
        .await
        .expect("insert old row");
    store
        .create_signal("corrupt", "Delta", "not JSON")
        .await
        .expect("insert corrupt row");
    store
        .create_signal(
            "invalid",
            "Gamma",
            &json!({
                "version": 3,
                "direction": "bullish",
                "root": { "type": "condition", "id": "missing" },
                "conditions": {},
            })
            .to_string(),
        )
        .await
        .expect("insert semantically invalid row");

    let entries = list_from_store(&store).await.expect("list catalog");
    assert_eq!(entries.len(), 4);
    assert!(entries.iter().any(|entry| matches!(
        entry,
        SignalCatalogEntry::Ready { id, definition, .. } if id == "ready" && definition == &ready
    )));
    assert!(entries.iter().any(|entry| matches!(
        entry,
        SignalCatalogEntry::Unavailable {
            id,
            issue: SignalCatalogIssue::UnsupportedVersion { version: 1 },
            ..
        } if id == "old"
    )));
    for id in ["corrupt", "invalid"] {
        assert!(entries.iter().any(|entry| matches!(
            entry,
            SignalCatalogEntry::Unavailable {
                id: entry_id,
                issue: SignalCatalogIssue::InvalidDefinition { .. },
                ..
            } if entry_id == id
        )));
    }
}

#[test]
fn entry_and_error_serialization_are_stable() {
    assert_eq!(
        serde_json::to_value(SignalCatalogEntry::Unavailable {
            id: "old".to_owned(),
            name: "Old".to_owned(),
            issue: SignalCatalogIssue::UnsupportedVersion { version: 1 },
        })
        .expect("serialize entry"),
        json!({
            "status": "unavailable",
            "id": "old",
            "name": "Old",
            "issue": { "type": "unsupportedVersion", "version": 1 },
        })
    );
    assert_eq!(
        serde_json::to_value(SignalCatalogError::NotFound {
            id: "missing".to_owned(),
        })
        .expect("serialize error"),
        json!({ "type": "notFound", "id": "missing" })
    );
}
