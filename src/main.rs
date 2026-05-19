use axum::{
    extract::{Json, Path},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use dotenv;
// use reqwest; // avoid bringing reqwest::StatusCode into scope
use serde_json::Value;
use std::net::SocketAddr;
use std::time::Instant;
use tokio::net::TcpListener;
use tower_http::cors::{Any, CorsLayer};
use tracing;
use tracing_subscriber;

mod conversion;
mod http_client;
mod metrics;
mod schema;
#[cfg(test)]
mod integration_tests;

#[tokio::main]
async fn main() {
    // Load environment variables from .env file
    dotenv::dotenv().ok();

    tracing_subscriber::fmt::init();

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([
            axum::http::Method::GET,
            axum::http::Method::POST,
            axum::http::Method::OPTIONS,
        ])
        .allow_headers(Any);

    let app = Router::new()
        .route("/", post(handle_query))
        .route("/debug", post(handle_debug))
        .route("/chainId/:chain_id", post(handle_chain_query))
        .route("/chainId/:chain_id/debug", post(handle_chain_debug))
        .route("/schema/test", get(test_introspection))
        .route("/schema/refresh", post(refresh_schema_endpoint))
        .route("/schema/raw", get(schema_raw_endpoint))
        .route("/metrics", get(metrics_handler))
        .layer(cors);

    // Initialize schema on startup (schema doesn't change once deployed, so we only fetch once)
    if let Err(e) = schema::initialize_schema().await {
        metrics::SCHEMA_REFRESH_ERRORS.inc();
        tracing::warn!("Failed to initialize schema: {}. Will retry on first request.", e);
    }

    let port = std::env::var("PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3000);
    let addr: SocketAddr = format!("0.0.0.0:{}", port).parse().unwrap();
    tracing::info!("listening on {}", addr);
    let listener = TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

/// Execute a query (schema is initialized once on startup)
/// Returns (status_code, response_json)
async fn execute_query_with_retry(
    payload: &Value,
    chain_id: Option<&str>,
) -> (StatusCode, Json<Value>) {
    // Track total request duration
    let request_start = Instant::now();
    metrics::REQUEST_COUNTER.inc();

    // Ensure schema is initialized (lazy initialization)
    if let Err(e) = schema::ensure_schema_initialized().await {
        tracing::error!("Failed to ensure schema is initialized: {}", e);
        metrics::REQUEST_DURATION.observe(request_start.elapsed().as_secs_f64() * 1000.0);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": "Schema initialization failed",
                "details": e.to_string()
            })),
        );
    }

    // Convert the query
    let conversion_start = Instant::now();
    let conversion_result = match conversion::convert_subgraph_to_hyperindex(payload, chain_id) {
        Ok(result) => {
            metrics::CONVERSION_DURATION.observe(conversion_start.elapsed().as_secs_f64() * 1000.0);
            result
        },
        Err(e) => {
            metrics::CONVERSION_DURATION.observe(conversion_start.elapsed().as_secs_f64() * 1000.0);
            metrics::CONVERSION_ERRORS.inc();
            metrics::TOTAL_ERRORS.inc();
            tracing::error!("Conversion error: {}", e);
            let reasoning = match &e {
                conversion::ConversionError::InvalidQueryFormat =>
                    "The provided GraphQL query string could not be parsed. Ensure it is a valid single operation with balanced braces and proper syntax.",
                conversion::ConversionError::MissingField(field) =>
                    if field == "query" { "The request body must include a 'query' string field." } else { "A required field is missing from the request." },
                conversion::ConversionError::UnsupportedFilter(_filter) =>
                    "This filter is not currently supported by the converter. Consider a supported equivalent or remove it.",
                conversion::ConversionError::ComplexMetaQuery =>
                    "Only _meta { block { number } } is supported. Remove extra fields like hash, timestamp, etc.",
            };
            let details = e.to_string();
            let subgraph_debug = maybe_fetch_subgraph_debug(payload.clone()).await;
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "Conversion failed",
                    "details": details,
                    "reasoning": reasoning,
                    "debug": {
                        "inputQuery": payload.get("query").and_then(|q| q.as_str()).unwrap_or_default(),
                        "chainId": chain_id.map(|c| serde_json::Value::String(c.to_string())).unwrap_or(serde_json::Value::Null),
                    },
                    "subgraphResponse": subgraph_debug,
                })),
            );
        }
    };

    let converted_query = conversion_result.query;
    let field_name_map = conversion_result.field_name_map;
    let is_meta_query = conversion_result.is_meta_query;

    if !conversion_result.filter_variable_entities.is_empty() {
        tracing::info!(
            "Translated subgraph filter variables to bool_exp shape: {:?}",
            conversion_result.filter_variable_entities
        );
    }

    tracing::info!("Converted query: {:?}", converted_query);

    // Prepare debug info for potential error responses
    let hyperindex_url = std::env::var("HYPERINDEX_URL").expect("HYPERINDEX_URL must be set");
    let original_query = payload
        .get("query")
        .and_then(|q| q.as_str())
        .unwrap_or_default();
    let converted_query_str = converted_query
        .get("query")
        .and_then(|q| q.as_str())
        .unwrap_or_default();

    // Try forwarding the query
    let query_start = Instant::now();
    let response = match forward_to_hyperindex(&converted_query).await {
        Ok(resp) => {
            metrics::QUERY_RESPONSE_WAIT_DURATION.observe(query_start.elapsed().as_secs_f64() * 1000.0);
            resp
        },
        Err(e) => {
            metrics::QUERY_RESPONSE_WAIT_DURATION.observe(query_start.elapsed().as_secs_f64() * 1000.0);
            metrics::QUERY_EXECUTION_ERRORS.inc();
            metrics::TOTAL_ERRORS.inc();
            tracing::error!("Hyperindex request error: {}", e);
            let details = e.to_string();
            let subgraph_debug = maybe_fetch_subgraph_debug(payload.clone()).await;
            tracing::error!(
                original_query = original_query,
                converted_query = converted_query_str,
                error = %details,
                "Error forwarding converted query to Hyperindex"
            );
            metrics::REQUEST_DURATION.observe(request_start.elapsed().as_secs_f64() * 1000.0);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": "Hyperindex request failed",
                    "details": details,
                    "debug": {
                        "originalQuery": original_query,
                        "convertedQuery": converted_query_str,
                        "hyperindexUrl": hyperindex_url,
                        "chainId": chain_id.map(|c| serde_json::Value::String(c.to_string())).unwrap_or(serde_json::Value::Null),
                    },
                    "subgraphResponse": subgraph_debug,
                })),
            );
        }
    };

    // Check for GraphQL errors
    if let Some(errors) = response.get("errors") {
        // Return GraphQL errors normally (no schema refresh retry)
        metrics::QUERY_EXECUTION_ERRORS.inc();
        metrics::TOTAL_ERRORS.inc();
        let subgraph_debug = maybe_fetch_subgraph_debug(payload.clone()).await;
        tracing::error!(
            original_query = original_query,
            converted_query = converted_query_str,
            "Upstream GraphQL returned errors for converted query"
        );
        let mut debug = serde_json::json!({
            "originalQuery": original_query,
            "convertedQuery": converted_query_str,
            "hyperindexUrl": hyperindex_url,
        });
        if let Some(chain_id) = chain_id {
            debug["chainId"] = serde_json::Value::String(chain_id.to_string());
        }
        metrics::REQUEST_DURATION.observe(request_start.elapsed().as_secs_f64() * 1000.0);
        return (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({
                "errors": errors.clone(),
                "debug": debug,
                "subgraphResponse": subgraph_debug,
            })),
        );
    }

    // Success - no errors
    let transform_start = Instant::now();
    let transformed = transform_response_to_subgraph_shape(response, &field_name_map, is_meta_query);
    let transform_duration_ms = transform_start.elapsed().as_secs_f64() * 1000.0;
    metrics::RESPONSE_TRANSFORM_DURATION.observe(transform_duration_ms);

    // Record total request duration
    let total_duration_ms = request_start.elapsed().as_secs_f64() * 1000.0;
    metrics::REQUEST_DURATION.observe(total_duration_ms);

    (StatusCode::OK, Json(transformed))
}

async fn handle_query(Json(payload): Json<Value>) -> impl IntoResponse {
    tracing::info!("Received query: {:?}", payload);
    execute_query_with_retry(&payload, None).await
}

async fn handle_chain_query(
    Path(chain_id): Path<String>,
    Json(payload): Json<Value>,
) -> impl IntoResponse {
    tracing::info!(
        "Received chain query for chain_id: {}, payload: {:?}",
        chain_id,
        payload
    );
    execute_query_with_retry(&payload, Some(&chain_id)).await
}

async fn handle_debug(Json(payload): Json<Value>) -> impl IntoResponse {
    tracing::info!("Received debug query: {:?}", payload);

    // Ensure schema is initialized (lazy initialization)
    if let Err(e) = schema::ensure_schema_initialized().await {
        tracing::error!("Failed to ensure schema is initialized: {}", e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": "Schema initialization failed",
                "details": e.to_string()
            })),
        );
    }

    match conversion::convert_subgraph_to_hyperindex(&payload, None) {
        Ok(result) => {
            tracing::info!("Converted debug query: {:?}", result.query);
            (StatusCode::OK, Json(result.query))
        }
        Err(e) => {
            tracing::error!("Debug conversion error: {}", e);
            let reasoning = match &e {
                conversion::ConversionError::InvalidQueryFormat =>
                    "The provided GraphQL query string could not be parsed. Ensure it is a valid single operation with balanced braces and proper syntax.",
                conversion::ConversionError::MissingField(field) =>
                    if field == "query" { "The request body must include a 'query' string field." } else { "A required field is missing from the request." },
                conversion::ConversionError::UnsupportedFilter(_filter) =>
                    "This filter is not currently supported by the converter. Consider a supported equivalent or remove it.",
                conversion::ConversionError::ComplexMetaQuery =>
                    "Only _meta { block { number } } is supported. Remove extra fields like hash, timestamp, etc.",
            };
            let details = e.to_string();
            let subgraph_debug = maybe_fetch_subgraph_debug(payload.clone()).await;
            (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "Conversion failed",
                    "details": details,
                    "reasoning": reasoning,
                    "debug": {
                        "inputQuery": payload.get("query").and_then(|q| q.as_str()).unwrap_or_default(),
                        "chainId": serde_json::Value::Null,
                    },
                    "subgraphResponse": subgraph_debug,
                })),
            )
        }
    }
}

async fn handle_chain_debug(
    Path(chain_id): Path<String>,
    Json(payload): Json<Value>,
) -> impl IntoResponse {
    tracing::info!(
        "Received chain debug for chain_id: {}, payload: {:?}",
        chain_id,
        payload
    );

    // Ensure schema is initialized (lazy initialization)
    if let Err(e) = schema::ensure_schema_initialized().await {
        tracing::error!("Failed to ensure schema is initialized: {}", e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": "Schema initialization failed",
                "details": e.to_string()
            })),
        );
    }

    match conversion::convert_subgraph_to_hyperindex(&payload, Some(&chain_id)) {
        Ok(result) => {
            tracing::info!("Converted chain debug query: {:?}", result.query);
            (StatusCode::OK, Json(result.query))
        }
        Err(e) => {
            tracing::error!("Chain debug conversion error: {}", e);
            let reasoning = match &e {
                conversion::ConversionError::InvalidQueryFormat =>
                    "The provided GraphQL query string could not be parsed. Ensure it is a valid single operation with balanced braces and proper syntax.",
                conversion::ConversionError::MissingField(field) =>
                    if field == "query" { "The request body must include a 'query' string field." } else { "A required field is missing from the request." },
                conversion::ConversionError::UnsupportedFilter(_filter) =>
                    "This filter is not currently supported by the converter. Consider a supported equivalent or remove it.",
                conversion::ConversionError::ComplexMetaQuery =>
                    "Only _meta { block { number } } is supported. Remove extra fields like hash, timestamp, etc.",
            };
            let details = e.to_string();
            let subgraph_debug = maybe_fetch_subgraph_debug(payload.clone()).await;
            (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "Conversion failed",
                    "details": details,
                    "reasoning": reasoning,
                    "debug": {
                        "inputQuery": payload.get("query").and_then(|q| q.as_str()).unwrap_or_default(),
                        "chainId": chain_id,
                    },
                    "subgraphResponse": subgraph_debug,
                })),
            )
        }
    }
}

async fn forward_to_hyperindex(
    query: &Value,
) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    let hyperindex_url = std::env::var("HYPERINDEX_URL").expect("HYPERINDEX_URL must be set");

    let response = http_client::HTTP_CLIENT
        .post(&hyperindex_url)
        .header("Content-Type", "application/json")
        .json(query)
        .send()
        .await?;

    let response_json: Value = response.json().await?;
    Ok(response_json)
}

fn transform_response_to_subgraph_shape(resp: Value, field_name_map: &std::collections::HashMap<String, String>, is_meta_query: bool) -> Value {
    let mut root = match resp {
        Value::Object(map) => map,
        other => return other,
    };

    if let Some(Value::Object(data_obj)) = root.get_mut("data") {
        // Special handling for _meta query response
        if is_meta_query {
            if let Some(chain_metadata) = data_obj.get("chain_metadata") {
                // Transform chain_metadata response to _meta format
                // From: {"data":{"chain_metadata":[{"latest_fetched_block_number":419538012}]}}
                // To:   {"data":{"_meta":{"block":{"number":419538012}}}}
                let block_number = chain_metadata
                    .as_array()
                    .and_then(|arr| arr.first())
                    .and_then(|obj| obj.get("latest_fetched_block_number"))
                    .cloned()
                    .unwrap_or(Value::Null);

                let meta_response = serde_json::json!({
                    "_meta": {
                        "block": {
                            "number": block_number
                        }
                    }
                });

                if let Value::Object(meta_obj) = meta_response {
                    *data_obj = meta_obj;
                }
                return Value::Object(root);
            }
        }

        let mut new_data = serde_json::Map::new();
        for (key, value) in data_obj.clone().into_iter() {
            // First, try to use the exact field name from the original query
            let new_key = if let Some(original_name) = field_name_map.get(&key) {
                original_name.clone()
            } else if key.ends_with("_by_pk") {
                // Fallback for _by_pk queries
                key.trim_end_matches("_by_pk").to_ascii_lowercase()
            } else if is_pascal_case(&key) {
                // Fallback: convert to lowercase plural
                pluralize_lowercase(&key)
            } else {
                key
            };
            new_data.insert(new_key, value);
        }
        *data_obj = new_data;
    }

    Value::Object(root)
}

fn is_pascal_case(s: &str) -> bool {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_uppercase() => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphabetic())
}

fn pluralize_lowercase(name: &str) -> String {
    let lower = name.to_ascii_lowercase();
    if lower.ends_with('y') {
        let pre = lower.chars().rev().nth(1).unwrap_or('a');
        if !matches!(pre, 'a' | 'e' | 'i' | 'o' | 'u') {
            return format!("{}ies", &lower[..lower.len() - 1]);
        }
    }
    if lower.ends_with("ch")
        || lower.ends_with("sh")
        || lower.ends_with('x')
        || lower.ends_with('z')
        || lower.ends_with('s')
        || lower.ends_with('o')
    {
        return format!("{}es", lower);
    }
    format!("{}s", lower)
}

async fn test_introspection() -> impl IntoResponse {
    tracing::info!("Testing introspection query...");
    
    match schema::fetch_schema().await {
        Ok(schema_response) => {
            // Try to parse it
            match schema::parse_and_cache_schema(&schema_response) {
                Ok(_) => {
                    // Get some sample data to show it works
                    let sample_entity = "Trade";
                    let sample_field = "pair";
                    let is_nested = schema::is_nested_entity(sample_entity, sample_field);
                    
                    (
                        StatusCode::OK,
                        Json(serde_json::json!({
                            "success": true,
                            "message": "Introspection query successful",
                            "cached_entities": "Schema parsed and cached",
                            "sample_check": {
                                "entity": sample_entity,
                                "field": sample_field,
                                "is_nested_entity": is_nested
                            },
                            "raw_response_preview": {
                                "data_present": schema_response.get("data").is_some(),
                                "types_count": schema_response
                                    .get("data")
                                    .and_then(|d| d.get("__schema"))
                                    .and_then(|s| s.get("types"))
                                    .and_then(|t| t.as_array())
                                    .map(|arr| arr.len())
                            }
                        })),
                    )
                }
                Err(e) => {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({
                            "success": false,
                            "error": "Failed to parse schema",
                            "details": e.to_string(),
                            "raw_response": schema_response
                        })),
                    )
                }
            }
        }
                Err(e) => {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({
                            "success": false,
                            "error": "Failed to fetch schema",
                            "details": e.to_string()
                        })),
                    )
                }
        }
    }

async fn refresh_schema_endpoint() -> impl IntoResponse {
    tracing::info!("Refreshing schema via endpoint...");
    
    match schema::refresh_schema().await {
        Ok(_) => {
            let (entity_count, last_updated) = schema::get_cache_stats();
            
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "success": true,
                    "message": "Schema refreshed successfully",
                    "last_updated": last_updated,
                    "cached_entities": entity_count
                })),
            )
        }
        Err(e) => {
            let err_msg = e.to_string();
            metrics::SCHEMA_REFRESH_ERRORS.inc();
            tracing::error!("Failed to refresh schema: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "success": false,
                    "error": "Failed to refresh schema",
                    "details": err_msg
                })),
            )
        }
    }
}

/// Metrics endpoint handler
async fn metrics_handler() -> impl IntoResponse {
    match metrics::gather_metrics() {
        Ok(metrics) => (
            StatusCode::OK,
            [("Content-Type", "text/plain; version=0.0.4")],
            metrics,
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            [("Content-Type", "text/plain")],
            format!("Error gathering metrics: {}", e),
        ),
    }
}

/// Return the current schema cache as JSON for debugging
async fn schema_raw_endpoint() -> impl IntoResponse {
    let cache_json = schema::get_schema_cache_json();
    (
        StatusCode::OK,
        Json(cache_json),
    )
}

async fn maybe_fetch_subgraph_debug(payload: Value) -> Option<Value> {
    let url = match std::env::var("SUBGRAPH_DEBUG_URL") {
        Ok(v) if !v.trim().is_empty() => v,
        _ => return None,
    };

    let mut req = http_client::HTTP_CLIENT
        .post(url)
        .header("Content-Type", "application/json")
        .json(&payload);

    // Optional auth headers for compatible subgraph endpoints
    // Priority: explicit custom header/value → bearer token → x-api-key fallbacks
    if let (Ok(header_name), Ok(header_value)) = (
        std::env::var("SUBGRAPH_AUTH_HEADER"),
        std::env::var("SUBGRAPH_AUTH_VALUE"),
    ) {
        if !header_name.trim().is_empty() && !header_value.trim().is_empty() {
            req = req.header(header_name, header_value);
        }
    } else if let Ok(token) = std::env::var("SUBGRAPH_BEARER_TOKEN") {
        if !token.trim().is_empty() {
            req = req.header("Authorization", format!("Bearer {}", token));
        }
    } else if let Ok(key) = std::env::var("SUBGRAPH_API_KEY") {
        if !key.is_empty() {
            req = req.header("x-api-key", key);
        }
    } else if let Ok(key) = std::env::var("THEGRAPH_API_KEY") {
        if !key.is_empty() {
            req = req.header("x-api-key", key);
        }
    } else if let Ok(key) = std::env::var("TEST_THEGRAPH_API_KEY") {
        if !key.is_empty() {
            req = req.header("x-api-key", key);
        }
    }

    let resp = match req.send().await {
        Ok(r) => r,
        Err(_) => return None,
    };

    let status = resp.status().as_u16();
    let body: Value = match resp.json().await {
        Ok(b) => b,
        Err(_) => return None,
    };

    Some(serde_json::json!({
        "status": status,
        "body": body,
    }))
}

#[cfg(test)]
mod response_shape_tests {
    use super::*;

    #[test]
    fn test_pluralize_lowercase_basic() {
        assert_eq!(pluralize_lowercase("Stream"), "streams");
        assert_eq!(pluralize_lowercase("Batch"), "batches");
        assert_eq!(pluralize_lowercase("Asset"), "assets");
        assert_eq!(pluralize_lowercase("Action"), "actions");
    }

    #[test]
    fn test_transform_data_keys_fallback() {
        // Test fallback behavior when no field map is provided
        let resp = serde_json::json!({
            "data": {
                "Stream": [ {"id": 1} ],
                "Batch": [ {"id": 2} ],
                "stream_by_pk": {"id": 3}
            }
        });
        let empty_map = std::collections::HashMap::new();
        let out = transform_response_to_subgraph_shape(resp, &empty_map, false);
        let data = out.get("data").unwrap();
        assert!(data.get("streams").is_some());
        assert!(data.get("batches").is_some());
        assert!(data.get("stream").is_some());
        assert!(data.get("Stream").is_none());
        assert!(data.get("Batch").is_none());
        assert!(data.get("stream_by_pk").is_none());
    }

    #[test]
    fn test_transform_uses_field_name_map() {
        // Test that field_name_map is used to preserve original field names
        let resp = serde_json::json!({
            "data": {
                "LpAction": [ {"id": 1} ],
                "LpShare": [ {"id": 2} ],
                "LpNFT": [ {"id": 3} ]
            }
        });
        let mut field_map = std::collections::HashMap::new();
        field_map.insert("LpAction".to_string(), "lpActions".to_string());
        field_map.insert("LpShare".to_string(), "lpShares".to_string());
        field_map.insert("LpNFT".to_string(), "lpNFTs".to_string());

        let out = transform_response_to_subgraph_shape(resp, &field_map, false);
        let data = out.get("data").unwrap();

        // Should use exact names from the map (original query field names)
        assert!(data.get("lpActions").is_some(), "Expected lpActions");
        assert!(data.get("lpShares").is_some(), "Expected lpShares");
        assert!(data.get("lpNFTs").is_some(), "Expected lpNFTs");

        // Should NOT have PascalCase or lowercase versions
        assert!(data.get("LpAction").is_none());
        assert!(data.get("lpactions").is_none());
    }

    #[test]
    fn test_transform_meta_query_response() {
        // Test _meta query response transformation
        let resp = serde_json::json!({
            "data": {
                "chain_metadata": [
                    {"latest_fetched_block_number": 419538012}
                ]
            }
        });
        let empty_map = std::collections::HashMap::new();
        let out = transform_response_to_subgraph_shape(resp, &empty_map, true);
        let data = out.get("data").unwrap();

        // Should have _meta structure
        assert!(data.get("_meta").is_some(), "Expected _meta");
        let meta = data.get("_meta").unwrap();
        assert!(meta.get("block").is_some(), "Expected block");
        let block = meta.get("block").unwrap();
        assert_eq!(block.get("number").unwrap(), 419538012);

        // Should NOT have chain_metadata
        assert!(data.get("chain_metadata").is_none());
    }
}
