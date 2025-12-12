use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

// Schema cache - stores the parsed schema structure
// Key: entity name (e.g., "Trade"), Value: map of field name -> FieldInfo
type SchemaCache = Arc<RwLock<HashMap<String, HashMap<String, FieldInfo>>>>;

#[derive(Debug, Clone)]
pub struct FieldInfo {
    pub is_nested_entity: bool,
    pub nested_type_name: Option<String>, // If nested, the type name (e.g., "Pair")
}

// Track when the schema was last updated
static SCHEMA_LAST_UPDATED: once_cell::sync::Lazy<Arc<RwLock<Option<u64>>>> =
    once_cell::sync::Lazy::new(|| Arc::new(RwLock::new(None)));

static SCHEMA_CACHE: once_cell::sync::Lazy<SchemaCache> =
    once_cell::sync::Lazy::new(|| Arc::new(RwLock::new(HashMap::new())));

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
                                            }
                                        }
                                        }
                                    }
                                    }
                                }
                                }
                                "#;

    let client = reqwest::Client::new();
    let payload = serde_json::json!({
        "query": introspection_query
    });

    let response = client
        .post(&hyperindex_url)
        .header("Content-Type", "application/json")
        .json(&payload)
        .send()
        .await?;

    let response_json: Value = response.json().await?;

    // Check for errors
    if let Some(errors) = response_json.get("errors") {
        return Err(format!("Introspection query failed: {}", serde_json::to_string(errors)?).into());
    }

    Ok(response_json)
}

/// Parse the introspection response and build a schema cache
pub fn parse_and_cache_schema(introspection_response: &Value) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut cache = SCHEMA_CACHE.write().unwrap();
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

            field_map.insert(
                field_name.to_string(),
                FieldInfo {
                    is_nested_entity,
                    nested_type_name,
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

/// Check if a type is an OBJECT type (i.e., a nested entity)
fn is_object_type(type_info: &Value) -> bool {
    let kind = type_info.get("kind").and_then(|k| k.as_str()).unwrap_or("");
    kind == "OBJECT"
}

/// Get field information for a specific entity and field
pub fn get_field_info(entity_name: &str, field_name: &str) -> Option<FieldInfo> {
    let cache = SCHEMA_CACHE.read().unwrap();
    cache
        .get(entity_name)
        .and_then(|fields| fields.get(field_name))
        .cloned()
}

/// Check if a field is a nested entity
pub fn is_nested_entity(entity_name: &str, field_name: &str) -> bool {
    get_field_info(entity_name, field_name)
        .map(|info| info.is_nested_entity)
        .unwrap_or(false)
}

/// Initialize the schema by fetching and caching it
pub async fn initialize_schema() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing::info!("Fetching GraphQL schema via introspection...");
    let schema_response = fetch_schema().await?;
    parse_and_cache_schema(&schema_response)?;
    tracing::info!("Schema initialized and cached");
    Ok(())
}

/// Refresh the schema by fetching it again and updating the cache
pub async fn refresh_schema() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing::info!("Refreshing GraphQL schema via introspection...");
    let schema_response = fetch_schema().await?;
    parse_and_cache_schema(&schema_response)?;
    tracing::info!("Schema refreshed and cached");
    Ok(())
}

/// Check if the schema cache is empty (needs initialization)
pub fn is_schema_empty() -> bool {
    let cache = SCHEMA_CACHE.read().unwrap();
    cache.is_empty()
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
    let cache = SCHEMA_CACHE.read().unwrap();
    let entity_count = cache.len();
    let last_updated = *SCHEMA_LAST_UPDATED.read().unwrap();
    (entity_count, last_updated)
}

/// Check if GraphQL errors suggest a schema mismatch that could be fixed by refreshing
/// Returns true if the errors look like they could be caused by stale schema cache
pub fn is_schema_stale_error(errors: &serde_json::Value) -> bool {
    // Check if errors array exists
    let errors_array = match errors.as_array() {
        Some(arr) => arr,
        None => return false,
    };

    // Look for common schema-related error patterns
    for error in errors_array {
        // Get the error message
        let message = error
            .get("message")
            .and_then(|m| m.as_str())
            .unwrap_or("")
            .to_lowercase();

        // Check for patterns that suggest schema mismatch:
        // 1. "field '_eq' not found in type: 'X_bool_exp'" - nested entity treated as primitive
        // 2. "field 'id' not found in type: 'X_comparison_exp'" - primitive treated as nested
        // 3. "field 'X' not found in type: 'Y'" - field doesn't exist (could be schema change)
        // 4. "field not found" in general
        if message.contains("field '_eq' not found in type") 
            || message.contains("field 'id' not found in type")
            || (message.contains("field") && message.contains("not found in type"))
            || message.contains("field not found") {
            return true;
        }

        // Check extensions for validation errors that might indicate schema issues
        if let Some(extensions) = error.get("extensions") {
            if let Some(code) = extensions.get("code").and_then(|c| c.as_str()) {
                if code == "validation-failed" {
                    // Validation errors could be schema-related
                    return true;
                }
            }
        }
    }

    false
}

#[cfg(test)]
/// Clear the schema cache (for tests)
pub fn clear_schema_cache() {
    let mut cache = SCHEMA_CACHE.write().unwrap();
    cache.clear();
    *SCHEMA_LAST_UPDATED.write().unwrap() = None;
}

#[cfg(test)]
/// Initialize schema cache with test data for unit tests
/// This allows tests to run without requiring a live Hyperindex endpoint
/// This function ALWAYS clears and resets the schema to ensure deterministic tests
pub fn init_test_schema() {
    // Always clear first to ensure we start with a clean slate
    clear_schema_cache();
    let mut cache = SCHEMA_CACHE.write().unwrap();

    // Trade entity with nested pair field
    let mut trade_fields = HashMap::new();
    trade_fields.insert("id".to_string(), FieldInfo {
        is_nested_entity: false,
        nested_type_name: None,
    });
    trade_fields.insert("pair".to_string(), FieldInfo {
        is_nested_entity: true,
        nested_type_name: Some("Pair".to_string()),
    });
    // Note: token can be either nested or regular depending on context
    // For Trade, we'll make it a regular field by default (can be overridden in specific tests)
    trade_fields.insert("token".to_string(), FieldInfo {
        is_nested_entity: false,
        nested_type_name: None,
    });
    trade_fields.insert("amount".to_string(), FieldInfo {
        is_nested_entity: false,
        nested_type_name: None,
    });
    trade_fields.insert("isOpen".to_string(), FieldInfo {
        is_nested_entity: false,
        nested_type_name: None,
    });
    trade_fields.insert("type".to_string(), FieldInfo {
        is_nested_entity: false,
        nested_type_name: None,
    });
    cache.insert("Trade".to_string(), trade_fields);

    // Pair entity with nested fee and token fields
    let mut pair_fields = HashMap::new();
    pair_fields.insert("id".to_string(), FieldInfo {
        is_nested_entity: false,
        nested_type_name: None,
    });
    pair_fields.insert("fee".to_string(), FieldInfo {
        is_nested_entity: true,
        nested_type_name: Some("Fee".to_string()),
    });
    pair_fields.insert("token".to_string(), FieldInfo {
        is_nested_entity: true,
        nested_type_name: Some("Token".to_string()),
    });
    pair_fields.insert("from".to_string(), FieldInfo {
        is_nested_entity: false,
        nested_type_name: None,
    });
    pair_fields.insert("name".to_string(), FieldInfo {
        is_nested_entity: false,
        nested_type_name: None,
    });
    cache.insert("Pair".to_string(), pair_fields);

    // Token entity
    let mut token_fields = HashMap::new();
    token_fields.insert("id".to_string(), FieldInfo {
        is_nested_entity: false,
        nested_type_name: None,
    });
    token_fields.insert("amount".to_string(), FieldInfo {
        is_nested_entity: false,
        nested_type_name: None,
    });
    token_fields.insert("name".to_string(), FieldInfo {
        is_nested_entity: false,
        nested_type_name: None,
    });
    cache.insert("Token".to_string(), token_fields);

    // Fee entity
    let mut fee_fields = HashMap::new();
    fee_fields.insert("id".to_string(), FieldInfo {
        is_nested_entity: false,
        nested_type_name: None,
    });
    fee_fields.insert("liqFeeP".to_string(), FieldInfo {
        is_nested_entity: false,
        nested_type_name: None,
    });
    cache.insert("Fee".to_string(), fee_fields);

    // LpAction entity (for the bug fix test case)
    let mut lp_action_fields = HashMap::new();
    lp_action_fields.insert("user".to_string(), FieldInfo {
        is_nested_entity: true,
        nested_type_name: Some("User".to_string()),
    });
    lp_action_fields.insert("type".to_string(), FieldInfo {
        is_nested_entity: false,
        nested_type_name: None,
    });
    lp_action_fields.insert("withdrawUnlockEpoch".to_string(), FieldInfo {
        is_nested_entity: false,
        nested_type_name: None,
    });
    cache.insert("LpAction".to_string(), lp_action_fields);

    // UserGroupStat entity (for the bug fix test case)
    let mut user_group_stat_fields = HashMap::new();
    user_group_stat_fields.insert("user".to_string(), FieldInfo {
        is_nested_entity: true,
        nested_type_name: Some("User".to_string()),
    });
    cache.insert("UserGroupStat".to_string(), user_group_stat_fields);

    // User entity
    let mut user_fields = HashMap::new();
    user_fields.insert("id".to_string(), FieldInfo {
        is_nested_entity: false,
        nested_type_name: None,
    });
    cache.insert("User".to_string(), user_fields);

    // Update timestamp
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    *SCHEMA_LAST_UPDATED.write().unwrap() = Some(timestamp);
}

