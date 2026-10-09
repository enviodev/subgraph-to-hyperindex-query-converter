use crate::http_client;
use dashmap::DashMap;
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

// Schema cache - stores the parsed schema structure
// Key: entity name (e.g., "Trade"), Value: map of field name -> FieldInfo
// Using DashMap for lock-free concurrent reads (eliminates lock contention)
type SchemaCache = Arc<DashMap<String, HashMap<String, FieldInfo>>>;

#[derive(Debug, Clone)]
pub struct FieldInfo {
    pub is_nested_entity: bool,
    pub nested_type_name: Option<String>, // If nested, the type name (e.g., "Pair")
    pub field_type: String, // The actual type name (e.g., "String", "Int", "orderaction")
    /// True when any wrapper on the field's type is a LIST (`[X!]!`, `[X]`, ...).
    pub is_list: bool,
}

// Track when the schema was last updated
static SCHEMA_LAST_UPDATED: once_cell::sync::Lazy<Arc<RwLock<Option<u64>>>> =
    once_cell::sync::Lazy::new(|| Arc::new(RwLock::new(None)));

static SCHEMA_CACHE: once_cell::sync::Lazy<SchemaCache> =
    once_cell::sync::Lazy::new(|| Arc::new(DashMap::new()));

/// Fetch the GraphQL schema via introspection query
pub async fn fetch_schema() -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    let hyperindex_url = std::env::var("HYPERINDEX_URL")
        .map_err(|_| "HYPERINDEX_URL must be set")?;

    // Standard GraphQL introspection query to get all types and their fields
    let introspection_query = r#"
                                query IntrospectionQuery {
                                __schema {
                                    types {
                                    name
                                    kind
                                    fields {
                                        name
                                        type {
                                        name
                                        kind
                                        ofType {
                                            name
                                            kind
                                            ofType {
                                            name
                                            kind
                                            ofType {
                                                name
                                                kind
                                            }
                                            }
                                        }
                                        }
                                    }
                                    }
                                }
                                }
                                "#;

    let payload = serde_json::json!({
        "query": introspection_query
    });

    let response = http_client::HTTP_CLIENT
        .post(&hyperindex_url)
        .header("Content-Type", "application/json")
        .json(&payload)
        .send()
        .await?;

    let response_json: Value = response.json().await?;

    // Check for errors
    if let Some(errors) = response_json.get("errors") {
        return Err(
            format!(
                "Introspection query failed: {}",
                serde_json::to_string(errors)?
            )
            .into(),
        );
    }

    // Best-effort: write the full introspection response to a local file for inspection.
    // This is mainly for local debugging so you can see exactly what the schema looks like.
    if let Ok(pretty) = serde_json::to_string_pretty(&response_json) {
        if let Err(e) = fs::write("schema_introspection.json", pretty) {
            tracing::warn!("Failed to write schema_introspection.json: {}", e);
        }
    }

    Ok(response_json)
}

/// Parse the introspection response and build a schema cache
pub fn parse_and_cache_schema(introspection_response: &Value) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cache = SCHEMA_CACHE.clone();
    cache.clear();

    let schema = introspection_response
        .get("data")
        .and_then(|d| d.get("__schema"))
        .and_then(|s| s.get("types"))
        .and_then(|t| t.as_array())
        .ok_or_else(|| "Invalid introspection response structure".to_string())?;

    for type_info in schema {
        let type_name = type_info
            .get("name")
            .and_then(|n| n.as_str())
            .ok_or_else(|| "Type missing name".to_string())?;

        // Skip introspection types and built-in types
        if type_name.starts_with("__") || type_name == "Query" || type_name == "Mutation" {
            continue;
        }

        // Skip if it's not an OBJECT type (we only care about entity types)
        let kind = type_info
            .get("kind")
            .and_then(|k| k.as_str())
            .unwrap_or("");
        if kind != "OBJECT" {
            continue;
        }

        let fields = type_info
            .get("fields")
            .and_then(|f| f.as_array())
            .ok_or_else(|| "Type missing fields".to_string())?;

        let mut field_map = HashMap::new();

        for field in fields {
            let field_name = field
                .get("name")
                .and_then(|n| n.as_str())
                .ok_or_else(|| "Field missing name".to_string())?;

            let field_type = field.get("type").ok_or_else(|| "Field missing type".to_string())?;
            
            // Navigate through type wrappers (NonNull, List, etc.) to get the actual type
            let actual_type = get_actual_type(field_type);
            
            let is_nested_entity = is_object_type(&actual_type);
            let nested_type_name = if is_nested_entity {
                actual_type.get("name").and_then(|n| n.as_str()).map(|s| s.to_string())
            } else {
                None
            };
            
            // Get the actual type name for enum/scalar detection
            let type_name_str = actual_type
                .get("name")
                .and_then(|n| n.as_str())
                .unwrap_or("String")
                .to_string();

            field_map.insert(
                field_name.to_string(),
                FieldInfo {
                    is_nested_entity,
                    nested_type_name,
                    field_type: type_name_str,
                    is_list: type_has_list_wrapper(field_type),
                },
            );
        }

        cache.insert(type_name.to_string(), field_map);
    }

    // Update the timestamp
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    *SCHEMA_LAST_UPDATED.write().unwrap() = Some(timestamp);

    tracing::info!("Parsed and cached schema with {} entity types", cache.len());
    Ok(())
}

/// Navigate through type wrappers (NonNull, List) to get the actual underlying type
fn get_actual_type(type_info: &Value) -> &Value {
    let mut current = type_info;
    loop {
        let kind = current.get("kind").and_then(|k| k.as_str()).unwrap_or("");
        if kind == "NON_NULL" || kind == "LIST" {
            if let Some(of_type) = current.get("ofType") {
                current = of_type;
                continue;
            }
        }
        break;
    }
    current
}

/// True when any NON_NULL/LIST wrapper on the way down to the named type is a LIST.
fn type_has_list_wrapper(type_info: &Value) -> bool {
    let mut current = type_info;
    loop {
        match current.get("kind").and_then(|k| k.as_str()).unwrap_or("") {
            "LIST" => return true,
            "NON_NULL" => match current.get("ofType") {
                Some(of_type) => current = of_type,
                None => return false,
            },
            _ => return false,
        }
    }
}

/// Check if a type is an OBJECT type (i.e., a nested entity)
fn is_object_type(type_info: &Value) -> bool {
    let kind = type_info.get("kind").and_then(|k| k.as_str()).unwrap_or("");
    kind == "OBJECT"
}

/// Get field information for a specific entity and field
/// Using DashMap for lock-free concurrent reads (eliminates lock contention)
pub fn get_field_info(entity_name: &str, field_name: &str) -> Option<FieldInfo> {
    SCHEMA_CACHE
        .get(entity_name)
        .and_then(|fields_ref| fields_ref.value().get(field_name).cloned())
}

/// Result of looking for the field on `parent` that links to a `target` entity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LinkLookup {
    /// Exactly one single-object field of `parent` has type `target`.
    One(String),
    /// No such field, or `parent` is not in the schema cache.
    None,
    /// Several fields qualify, so the right one cannot be chosen from the type alone.
    Ambiguous,
}

/// Find the single-object field of `parent` whose type is `target`.
///
/// Hyperindex has no GraphQL interfaces, so a subgraph interface such as
/// `UserTransaction` is a concrete entity with one nullable link per implementing
/// type (`supply: Supply`, `borrow: Borrow`, ...). This is how
/// `... on Supply { }` is translated to `supply { }`. List-valued fields are
/// ignored: a link carries at most one record.
pub fn link_field_for_type(parent: &str, target: &str) -> LinkLookup {
    let Some(fields) = SCHEMA_CACHE.get(parent) else {
        return LinkLookup::None;
    };
    let mut names: Vec<&String> = fields
        .value()
        .iter()
        .filter(|(_, info)| {
            info.is_nested_entity
                && !info.is_list
                && info.nested_type_name.as_deref() == Some(target)
        })
        .map(|(name, _)| name)
        .collect();
    match names.len() {
        0 => LinkLookup::None,
        1 => LinkLookup::One(names.remove(0).clone()),
        _ => LinkLookup::Ambiguous,
    }
}

/// Entity type of the object-valued field `field` on `parent`, if both are known.
pub fn nested_type_of(parent: &str, field: &str) -> Option<String> {
    get_field_info(parent, field).and_then(|info| info.nested_type_name)
}

/// Find the cached entity whose graph-node collection field is `field_name`.
///
/// graph-node's pluralization is not reversible from spelling alone —
/// `txTransferses` could singularize to `TxTransfers` or `TxTransferse`, and
/// guessing wrong queries a table that does not exist, which surfaces as an
/// empty list rather than an error. The schema knows which entity is real, so
/// ask it before falling back to word rules.
///
/// Returns `None` when the cache is empty or holds no match, leaving the caller
/// on its lexical fallback.
pub fn resolve_entity_for_collection_field(field_name: &str) -> Option<String> {
    let cache = SCHEMA_CACHE.clone();

    // Exact match against every root-field spelling graph-node could produce.
    let exact = cache
        .iter()
        .find(|entry| graph_node_field_names(entry.key()).iter().any(|c| c == field_name))
        .map(|entry| entry.key().clone());
    if exact.is_some() {
        return exact;
    }

    // Case-insensitive fallback. Clients hand-write the other casing often
    // enough that guessing an entity that does not exist - which surfaces as an
    // empty list rather than an error - is the worse outcome.
    let lower = field_name.to_lowercase();
    let insensitive = cache
        .iter()
        .find(|entry| {
            graph_node_field_names(entry.key())
                .iter()
                .any(|c| c.to_lowercase() == lower)
        })
        .map(|entry| entry.key().clone());
    insensitive
}

/// Every root-field spelling that can refer to `entity`, singular and plural.
///
/// graph-node lowercases the *leading run of capitals*, not just the first
/// letter: `ATokenBalanceHistoryItem` is queried as `atokenBalanceHistoryItems`
/// and `EModeCategory` as `emodeCategories`. Verified against graph-node, which
/// rejects `aTokenBalanceHistoryItems` and `eModeCategories` outright. The
/// lowercase-first-letter form is kept as an accepted alias so queries written
/// against the Hyperindex schema keep working.
fn graph_node_field_names(entity: &str) -> Vec<String> {
    let mut singulars = vec![lower_leading_acronym(entity), lower_first_char(entity)];
    singulars.dedup();
    let mut names = Vec::with_capacity(singulars.len() * 2);
    for singular in singulars {
        names.push(pluralize(&singular));
        names.push(singular);
    }
    names
}

/// `UserReserve` -> `userReserve`
fn lower_first_char(entity: &str) -> String {
    let mut chars = entity.chars();
    match chars.next() {
        None => String::new(),
        Some(f) => f.to_lowercase().collect::<String>() + chars.as_str(),
    }
}

/// `ATokenBalanceHistoryItem` -> `atokenBalanceHistoryItem`, `UserReserve` -> `userReserve`.
fn lower_leading_acronym(entity: &str) -> String {
    let run = entity
        .chars()
        .take_while(|c| c.is_uppercase())
        .count()
        .max(1);
    let (head, tail) = entity.split_at(
        entity
            .char_indices()
            .nth(run)
            .map(|(i, _)| i)
            .unwrap_or(entity.len()),
    );
    head.to_lowercase() + tail
}

/// English pluralization, applied to an already-lowercased-head field name.
fn pluralize(name: &str) -> String {
    let ends_with_any = |suffixes: &[&str]| suffixes.iter().any(|s| name.ends_with(s));

    if name.ends_with('y')
        && !name.ends_with("ay")
        && !name.ends_with("ey")
        && !name.ends_with("iy")
        && !name.ends_with("oy")
        && !name.ends_with("uy")
    {
        format!("{}ies", &name[..name.len() - 1])
    } else if ends_with_any(&["s", "x", "z", "ch", "sh"]) {
        format!("{}es", name)
    } else {
        format!("{}s", name)
    }
}

/// Check if a field is a nested entity
pub fn is_nested_entity(entity_name: &str, field_name: &str) -> bool {
    get_field_info(entity_name, field_name)
        .map(|info| info.is_nested_entity)
        .unwrap_or(false)
}

/// Check if a type is a standard scalar (not an enum)
pub fn is_standard_scalar(type_name: &str) -> bool {
    matches!(
        type_name,
        "String" | "Int" | "Float" | "Boolean" | "ID" | "BigInt" | "BigDecimal" | "Bytes" | "numeric"
    )
}

/// Initialize the schema by fetching and caching it (called once on startup)
pub async fn initialize_schema() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing::info!("Fetching GraphQL schema via introspection...");
    let fetch_start = std::time::Instant::now();
    let schema_response = fetch_schema().await?;
    let fetch_duration_ms = fetch_start.elapsed().as_secs_f64() * 1000.0;
    
    // Track initialization metrics
    crate::metrics::SCHEMA_FETCH_DURATION.observe(fetch_duration_ms);
    crate::metrics::SCHEMA_REFRESH_COUNTER.inc();
    
    parse_and_cache_schema(&schema_response)?;
    tracing::info!("Schema initialized and cached");
    Ok(())
}

/// Refresh the schema by fetching it again and updating the cache
/// This is a manual operation - the schema doesn't change automatically once deployed
pub async fn refresh_schema() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing::info!("Refreshing GraphQL schema via introspection...");
    let fetch_start = std::time::Instant::now();
    let schema_response = fetch_schema().await?;
    let fetch_duration_ms = fetch_start.elapsed().as_secs_f64() * 1000.0;
    
    // Track refresh metrics
    crate::metrics::SCHEMA_FETCH_DURATION.observe(fetch_duration_ms);
    crate::metrics::SCHEMA_REFRESH_COUNTER.inc();
    
    parse_and_cache_schema(&schema_response)?;
    tracing::info!("Schema refreshed and cached");
    Ok(())
}

/// Check if the schema cache is empty (needs initialization)
pub fn is_schema_empty() -> bool {
    SCHEMA_CACHE.is_empty()
}

/// Ensure schema is initialized - if empty, fetch it
pub async fn ensure_schema_initialized() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if is_schema_empty() {
        tracing::info!("Schema cache is empty, initializing...");
        initialize_schema().await?;
    }
    Ok(())
}

/// Get cache statistics
pub fn get_cache_stats() -> (usize, Option<u64>) {
    let entity_count = SCHEMA_CACHE.len();
    let last_updated = *SCHEMA_LAST_UPDATED.read().unwrap();
    (entity_count, last_updated)
}

/// Get a JSON snapshot of the current schema cache for debugging/inspection
pub fn get_schema_cache_json() -> Value {
    let mut entities: HashMap<String, Value> = HashMap::new();

    for entry in SCHEMA_CACHE.iter() {
        let entity_name = entry.key();
        let fields = entry.value();
        let mut field_map: HashMap<String, Value> = HashMap::new();
        for (field_name, info) in fields.iter() {
            field_map.insert(
                field_name.clone(),
                serde_json::json!({
                    "is_nested_entity": info.is_nested_entity,
                    "nested_type_name": info.nested_type_name,
                }),
            );
        }
        entities.insert(entity_name.clone(), serde_json::json!(field_map));
    }

    serde_json::json!({
        "entities": entities,
        "stats": {
            "entity_count": entities.len(),
            "last_updated": *SCHEMA_LAST_UPDATED.read().unwrap(),
        }
    })
}


#[cfg(test)]
/// Clear the schema cache (for tests)
pub fn clear_schema_cache() {
    SCHEMA_CACHE.clear();
    *SCHEMA_LAST_UPDATED.write().unwrap() = None;
}

#[cfg(test)]
/// Initialize the test schema exactly once per test binary.
///
/// `init_test_schema` clears the process-global cache before repopulating it, so
/// calling it again while other tests run in parallel can briefly blank out
/// entities they depend on. Every test should go through this.
pub fn init_test_schema_once() {
    static INIT_TEST_SCHEMA: std::sync::Once = std::sync::Once::new();
    INIT_TEST_SCHEMA.call_once(init_test_schema);
}

#[cfg(test)]
/// Initialize schema cache with test data for unit tests
/// This allows tests to run without requiring a live Hyperindex endpoint
/// This function ALWAYS clears and resets the schema to ensure deterministic tests
pub fn init_test_schema() {
    // Always clear first to ensure we start with a clean slate
    clear_schema_cache();
    let cache = SCHEMA_CACHE.clone();

    // Trade entity with nested pair field
    let mut trade_fields = HashMap::new();
    trade_fields.insert("id".to_string(), FieldInfo {
        is_nested_entity: false,
        nested_type_name: None,
        field_type: "String".to_string(),
        is_list: false,
    });
    trade_fields.insert("pair".to_string(), FieldInfo {
        is_nested_entity: true,
        nested_type_name: Some("Pair".to_string()),
        field_type: "Pair".to_string(),
        is_list: false,
    });
    // Note: token can be either nested or regular depending on context
    // For Trade, we'll make it a regular field by default (can be overridden in specific tests)
    trade_fields.insert("token".to_string(), FieldInfo {
        is_nested_entity: false,
        nested_type_name: None,
        field_type: "String".to_string(),
        is_list: false,
    });
    trade_fields.insert("amount".to_string(), FieldInfo {
        is_nested_entity: false,
        nested_type_name: None,
        field_type: "numeric".to_string(),
        is_list: false,
    });
    trade_fields.insert("isOpen".to_string(), FieldInfo {
        is_nested_entity: false,
        nested_type_name: None,
        field_type: "Boolean".to_string(),
        is_list: false,
    });
    trade_fields.insert("type".to_string(), FieldInfo {
        is_nested_entity: false,
        nested_type_name: None,
        field_type: "String".to_string(),
        is_list: false,
    });
    cache.insert("Trade".to_string(), trade_fields);

    // Pair entity with nested fee and token fields
    let mut pair_fields = HashMap::new();
    pair_fields.insert("id".to_string(), FieldInfo {
        is_nested_entity: false,
        nested_type_name: None,
        field_type: "String".to_string(),
        is_list: false,
    });
    pair_fields.insert("fee".to_string(), FieldInfo {
        is_nested_entity: true,
        nested_type_name: Some("Fee".to_string()),
        field_type: "Fee".to_string(),
        is_list: false,
    });
    pair_fields.insert("token".to_string(), FieldInfo {
        is_nested_entity: true,
        nested_type_name: Some("Token".to_string()),
        field_type: "Token".to_string(),
        is_list: false,
    });
    pair_fields.insert("from".to_string(), FieldInfo {
        is_nested_entity: false,
        nested_type_name: None,
        field_type: "String".to_string(),
        is_list: false,
    });
    pair_fields.insert("name".to_string(), FieldInfo {
        is_nested_entity: false,
        nested_type_name: None,
        field_type: "String".to_string(),
        is_list: false,
    });
    cache.insert("Pair".to_string(), pair_fields);

    // Token entity
    let mut token_fields = HashMap::new();
    token_fields.insert("id".to_string(), FieldInfo {
        is_nested_entity: false,
        nested_type_name: None,
        field_type: "String".to_string(),
        is_list: false,
    });
    token_fields.insert("amount".to_string(), FieldInfo {
        is_nested_entity: false,
        nested_type_name: None,
        field_type: "numeric".to_string(),
        is_list: false,
    });
    token_fields.insert("name".to_string(), FieldInfo {
        is_nested_entity: false,
        nested_type_name: None,
        field_type: "String".to_string(),
        is_list: false,
    });
    cache.insert("Token".to_string(), token_fields);

    // Fee entity
    let mut fee_fields = HashMap::new();
    fee_fields.insert("id".to_string(), FieldInfo {
        is_nested_entity: false,
        nested_type_name: None,
        field_type: "String".to_string(),
        is_list: false,
    });
    fee_fields.insert("liqFeeP".to_string(), FieldInfo {
        is_nested_entity: false,
        nested_type_name: None,
        field_type: "numeric".to_string(),
        is_list: false,
    });
    cache.insert("Fee".to_string(), fee_fields);

    // LpAction entity (for the bug fix test case)
    let mut lp_action_fields = HashMap::new();
    lp_action_fields.insert("user".to_string(), FieldInfo {
        is_nested_entity: true,
        nested_type_name: Some("User".to_string()),
        field_type: "User".to_string(),
        is_list: false,
    });
    lp_action_fields.insert("type".to_string(), FieldInfo {
        is_nested_entity: false,
        nested_type_name: None,
        field_type: "String".to_string(),
        is_list: false,
    });
    lp_action_fields.insert("withdrawUnlockEpoch".to_string(), FieldInfo {
        is_nested_entity: false,
        nested_type_name: None,
        field_type: "Int".to_string(),
        is_list: false,
    });
    cache.insert("LpAction".to_string(), lp_action_fields);

    // UserGroupStat entity (for the bug fix test case)
    let mut user_group_stat_fields = HashMap::new();
    user_group_stat_fields.insert("user".to_string(), FieldInfo {
        is_nested_entity: true,
        nested_type_name: Some("User".to_string()),
        field_type: "User".to_string(),
        is_list: false,
    });
    cache.insert("UserGroupStat".to_string(), user_group_stat_fields);

    // User entity
    let mut user_fields = HashMap::new();
    user_fields.insert("id".to_string(), FieldInfo {
        is_nested_entity: false,
        nested_type_name: None,
        field_type: "String".to_string(),
        is_list: false,
    });
    cache.insert("User".to_string(), user_fields);

    // Order entity (for enum type conversion test)
    let mut order_fields = HashMap::new();
    order_fields.insert("id".to_string(), FieldInfo {
        is_nested_entity: false,
        nested_type_name: None,
        field_type: "String".to_string(),
        is_list: false,
    });
    order_fields.insert("trader".to_string(), FieldInfo {
        is_nested_entity: false,
        nested_type_name: None,
        field_type: "String".to_string(),
        is_list: false,
    });
    order_fields.insert("orderAction".to_string(), FieldInfo {
        is_nested_entity: false,
        nested_type_name: None,
        field_type: "orderaction".to_string(), // enum type
        is_list: false,
    });
    order_fields.insert("isPending".to_string(), FieldInfo {
        is_nested_entity: false,
        nested_type_name: None,
        field_type: "Boolean".to_string(),
        is_list: false,
    });
    order_fields.insert("executedAt".to_string(), FieldInfo {
        is_nested_entity: false,
        nested_type_name: None,
        field_type: "numeric".to_string(),
        is_list: false,
    });
    order_fields.insert("pair".to_string(), FieldInfo {
        is_nested_entity: true,
        nested_type_name: Some("Pair".to_string()),
        field_type: "Pair".to_string(),
        is_list: false,
    });
    cache.insert("Order".to_string(), order_fields);

    // Argus-shaped entities, used by the tests covering their production queries.
    // `Launch` is referenced by Swap/Holder/LaunchHourData/LaunchDayData, which is
    // what makes `launch_in` a nested-entity filter rather than a scalar one.
    let mut launch_fields = HashMap::new();
    for (name, ty) in [
        ("id", "String"),
        ("name", "String"),
        ("symbol", "String"),
        ("tracker", "String"),
        ("creator", "String"),
        ("quoteAsset", "String"),
        ("quoteSymbol", "String"),
        ("createdAt", "numeric"),
        ("dividendsPaid", "numeric"),
    ] {
        launch_fields.insert(name.to_string(), FieldInfo {
            is_nested_entity: false,
            nested_type_name: None,
            field_type: ty.to_string(),
            is_list: false,
        });
    }
    cache.insert("Launch".to_string(), launch_fields);

    for entity in ["LaunchHourData", "LaunchDayData", "Swap", "Holder"] {
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), FieldInfo {
            is_nested_entity: false,
            nested_type_name: None,
            field_type: "String".to_string(),
            is_list: false,
        });
        fields.insert("launch".to_string(), FieldInfo {
            is_nested_entity: true,
            nested_type_name: Some("Launch".to_string()),
            field_type: "Launch".to_string(),
            is_list: false,
        });
        for (name, ty) in [
            ("periodStart", "numeric"),
            ("volumeQuote", "numeric"),
            ("timestamp", "numeric"),
            ("ordinal", "numeric"),
            ("balance", "numeric"),
            ("trader", "String"),
            ("account", "String"),
            ("isSystem", "Boolean"),
        ] {
            fields.insert(name.to_string(), FieldInfo {
                is_nested_entity: false,
                nested_type_name: None,
                field_type: ty.to_string(),
                is_list: false,
            });
        }
        cache.insert(entity.to_string(), fields);
    }

    // Entities whose graph-node plural is not recoverable by word rules alone.
    for entity in ["Protocol", "ProtocolDayData", "TxTransfers"] {
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), FieldInfo {
            is_nested_entity: false,
            nested_type_name: None,
            field_type: "String".to_string(),
            is_list: false,
        });
        cache.insert(entity.to_string(), fields);
    }

    // Stand-in for a subgraph interface, as Hyperindex models it: a concrete
    // `UserTransaction` entity with one nullable single-object link per implementing
    // type, plus the implementing types themselves.
    {
        let scalar = |ty: &str| FieldInfo {
            is_nested_entity: false,
            nested_type_name: None,
            field_type: ty.to_string(),
            is_list: false,
        };
        let object = |ty: &str, is_list: bool| FieldInfo {
            is_nested_entity: true,
            nested_type_name: Some(ty.to_string()),
            field_type: ty.to_string(),
            is_list,
        };
        let build = |scalars: &[(&str, &str)], objects: &[(&str, &str, bool)]| {
            let mut m = HashMap::new();
            for (n, t) in scalars {
                m.insert(n.to_string(), scalar(t));
            }
            for (n, t, l) in objects {
                m.insert(n.to_string(), object(t, *l));
            }
            m
        };
        cache.insert(
            "UserTransaction".to_string(),
            build(
                &[("id", "String"), ("txHash", "String"), ("action", "String"), ("timestamp", "Int")],
                &[
                    ("supply", "Supply", false),
                    ("borrow", "Borrow", false),
                    ("repay", "Repay", false),
                    ("user", "TxUser", false),
                ],
            ),
        );
        cache.insert(
            "Supply".to_string(),
            build(
                &[("id", "String"), ("amount", "numeric"), ("assetPriceUSD", "numeric")],
                &[("reserve", "Reserve", false)],
            ),
        );
        cache.insert(
            "Borrow".to_string(),
            build(
                &[("id", "String"), ("amount", "numeric"), ("borrowRateMode", "Int")],
                &[("reserve", "Reserve", false)],
            ),
        );
        cache.insert(
            "Repay".to_string(),
            build(&[("id", "String"), ("amount", "numeric")], &[("reserve", "Reserve", false)]),
        );
        cache.insert(
            "Reserve".to_string(),
            build(&[("id", "String"), ("symbol", "String"), ("decimals", "Int")], &[]),
        );
        cache.insert("TxUser".to_string(), build(&[("id", "String")], &[("userTransactions", "UserTransaction", true)]));
        // A list of the interface type and two links to one target: the list must be
        // ignored and the pair must be reported as ambiguous.
        cache.insert(
            "TxAccount".to_string(),
            build(
                &[("id", "String")],
                &[
                    ("txs", "UserTransaction", true),
                    ("first", "Supply", false),
                    ("second", "Supply", false),
                ],
            ),
        );
    }

    // Entities whose names open with a run of capitals. graph-node queries these
    // as `atokenBalanceHistoryItems` / `emodeCategories`, not `aToken…` / `eMode…`.
    {
        let mut id_only = HashMap::new();
        id_only.insert(
            "id".to_string(),
            FieldInfo {
                is_nested_entity: false,
                nested_type_name: None,
                field_type: "String".to_string(),
                is_list: false,
            },
        );
        cache.insert("ATokenBalanceHistoryItem".to_string(), id_only.clone());
        cache.insert("EModeCategory".to_string(), id_only);
    }

    // Update timestamp
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    *SCHEMA_LAST_UPDATED.write().unwrap() = Some(timestamp);
}

