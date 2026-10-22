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
mod fragments;
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
        tracing::warn!("Failed to initialize schema: {}. Will retry on first request.", redact_upstream_error(&*e));
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
        tracing::error!("Failed to ensure schema is initialized: {}", redact_upstream_error(&*e));
        metrics::REQUEST_DURATION.observe(request_start.elapsed().as_secs_f64() * 1000.0);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(graphql_errors(["Internal server error: schema initialization failed".to_string()])),
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
            metrics::REQUEST_DURATION.observe(request_start.elapsed().as_secs_f64() * 1000.0);
            // graph-node answers a malformed request with 400 and `{"error"}`,
            // and a query it cannot run with 200 and `{"errors": [...]}`.
            let (status, mut body) = if e.is_request_error() {
                (
                    StatusCode::BAD_REQUEST,
                    serde_json::json!({
                        "error": format!("GraphQL server error (client error): {}", e)
                    }),
                )
            } else {
                (StatusCode::OK, graphql_errors([e.to_string()]))
            };
            if debug_errors_enabled() {
                body["debug"] = serde_json::json!({
                    "reasoning": conversion_error_reasoning(&e),
                    "inputQuery": payload.get("query").and_then(|q| q.as_str()).unwrap_or_default(),
                    "chainId": chain_id,
                });
                body["subgraphResponse"] =
                    maybe_fetch_subgraph_debug(payload.clone()).await.unwrap_or(Value::Null);
            }
            return (status, Json(body));
        }
    };

    let converted_query = conversion_result.query;
    let min_block = conversion_result.min_block;
    let field_name_map = conversion_result.field_name_map;
    let meta_selections = conversion_result.meta_selections;
    let shape_plans = conversion_result.shape_plans;
    let single_row_keys = conversion_result.single_row_keys;

    if !conversion_result.filter_variable_entities.is_empty() {
        tracing::info!(
            "Translated subgraph filter variables to bool_exp shape: {:?}",
            conversion_result.filter_variable_entities
        );
    }

    if !conversion_result.order_by_variables.is_empty() {
        tracing::info!(
            "Rebuilt subgraph order variables into Hasura order_by shape: {:?}",
            conversion_result
                .order_by_variables
                .iter()
                .map(|spec| (spec.target_var.as_str(), spec.entity.as_str()))
                .collect::<Vec<_>>()
        );
    }

    if !conversion_result.dropped_variables.is_empty() {
        // GraphQL rejects a declared-but-unused variable, so these were removed
        // from the operation header and from the forwarded payload alike.
        tracing::info!(
            "Dropped now-unused variable declarations: {:?}",
            conversion_result.dropped_variables
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
            // `reqwest::Error` repeats the request URL, credentials and all.
            let details = redact_upstream_error(&*e);
            tracing::error!(
                original_query = original_query,
                converted_query = converted_query_str,
                hyperindex_url = %loggable_upstream(&hyperindex_url),
                error = %details,
                "Error forwarding converted query to Hyperindex"
            );
            metrics::REQUEST_DURATION.observe(request_start.elapsed().as_secs_f64() * 1000.0);
            // graph-node reports a store it cannot reach as a GraphQL error. The
            // transport error itself names the upstream URL, so it stays in the log.
            let mut body = graphql_errors(["Store error: the indexer could not be reached".to_string()]);
            if debug_errors_enabled() {
                body["debug"] = serde_json::json!({
                    "details": details,
                    "originalQuery": original_query,
                    "convertedQuery": converted_query_str,
                    "chainId": chain_id,
                });
                body["subgraphResponse"] =
                    maybe_fetch_subgraph_debug(payload.clone()).await.unwrap_or(Value::Null);
            }
            return (StatusCode::OK, Json(body));
        }
    };

    let mut response = response;
    enforce_min_block(&mut response, min_block, &deployment_label());

    // Check for GraphQL errors
    if let Some(errors) = response.get("errors") {
        // Return GraphQL errors normally (no schema refresh retry)
        metrics::QUERY_EXECUTION_ERRORS.inc();
        metrics::TOTAL_ERRORS.inc();
        tracing::error!(
            original_query = original_query,
            converted_query = converted_query_str,
            errors = %errors,
            hyperindex_url = %loggable_upstream(&hyperindex_url),
            "Upstream GraphQL returned errors for converted query"
        );
        let mut body = subgraph_errors(errors);
        if debug_errors_enabled() {
            let mut debug = serde_json::json!({
                "upstreamErrors": errors.clone(),
                "originalQuery": original_query,
                "convertedQuery": converted_query_str,
            });
            if let Some(chain_id) = chain_id {
                debug["chainId"] = serde_json::Value::String(chain_id.to_string());
            }
            body["debug"] = debug;
            body["subgraphResponse"] =
                maybe_fetch_subgraph_debug(payload.clone()).await.unwrap_or(Value::Null);
        }
        metrics::REQUEST_DURATION.observe(request_start.elapsed().as_secs_f64() * 1000.0);
        return (StatusCode::OK, Json(body));
    }

    // Success - no errors
    let transform_start = Instant::now();
    unwrap_single_rows(&mut response, &single_row_keys);
    apply_shape_plans(&mut response, &shape_plans);
    let transformed =
        transform_response_to_subgraph_shape(response, &field_name_map, &meta_selections);
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
        tracing::error!("Failed to ensure schema is initialized: {}", redact_upstream_error(&*e));
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": "Schema initialization failed",
                "details": redact_upstream_error(&*e)
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
            let reasoning = conversion_error_reasoning(&e);
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
        tracing::error!("Failed to ensure schema is initialized: {}", redact_upstream_error(&*e));
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": "Schema initialization failed",
                "details": redact_upstream_error(&*e)
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
            let reasoning = conversion_error_reasoning(&e);
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

/// A GraphQL error response as graph-node sends it: `errors` with a `message`
/// each, and no `data`.
fn graphql_errors(messages: impl IntoIterator<Item = String>) -> Value {
    let errors: Vec<Value> = messages
        .into_iter()
        .map(|message| serde_json::json!({ "message": message }))
        .collect();
    serde_json::json!({ "errors": errors })
}

/// Hasura's errors in graph-node's shape: each `message` is kept, and Hasura's
/// `extensions` (its error code and a path through the converted query) are
/// dropped. The original errors are logged.
fn subgraph_errors(errors: &Value) -> Value {
    let messages: Vec<String> = match errors {
        Value::Array(items) => items
            .iter()
            .map(|item| match item.get("message") {
                Some(Value::String(m)) => m.clone(),
                _ => item.to_string(),
            })
            .collect(),
        other => vec![other.to_string()],
    };
    graphql_errors(messages)
}

/// `CONVERTER_DEBUG_ERRORS=on` adds the converter's diagnostics (`debug`,
/// `subgraphResponse`) to error responses, as they were before errors took
/// graph-node's shape. Off by default: they name the upstream URL and the
/// converted query.
fn debug_errors_enabled() -> bool {
    !cfg!(test) && debug_errors_setting(std::env::var("CONVERTER_DEBUG_ERRORS").ok().as_deref())
}

fn debug_errors_setting(value: Option<&str>) -> bool {
    matches!(
        value.map(|v| v.trim().to_ascii_lowercase()).as_deref(),
        Some("1") | Some("true") | Some("on") | Some("yes")
    )
}

/// What graph-node's "only indexed up to block" error names as the subgraph:
/// the deployment hash of `HYPERINDEX_URL`, as `_meta.deployment` reports it.
fn deployment_label() -> String {
    conversion::deployment_id().unwrap_or_else(|| "hyperindex".to_string())
}

/// Why a conversion error happened, for the `/debug` endpoints and
/// `CONVERTER_DEBUG_ERRORS`.
fn conversion_error_reasoning(e: &conversion::ConversionError) -> &'static str {
    match e {
        conversion::ConversionError::InvalidQueryFormat =>
            "The provided GraphQL query string could not be parsed. Ensure it is a valid single operation with balanced braces and proper syntax.",
        conversion::ConversionError::MissingField(field) =>
            if field == "query" { "The request body must include a 'query' string field." } else { "A required field is missing from the request." },
        conversion::ConversionError::QueryNotString =>
            "The request body's 'query' field must be a string.",
        conversion::ConversionError::UnsupportedFilter(_filter) =>
            "This filter is not currently supported by the converter. Consider a supported equivalent or remove it.",
        conversion::ConversionError::NullFilterValue(_) =>
            "graph-node rejects null for a string filter (_contains, _starts_with, _ends_with and their _not_/_nocase forms). Pass a string, or leave the filter out.",
        conversion::ConversionError::UnsupportedNestedArgument(_, _) =>
            "graph-node takes only first, skip, orderBy, orderDirection and where on a nested field. Anything else is rejected rather than converted or dropped, because the answer would not be to the question that was asked.",
        conversion::ConversionError::ComplexMetaQuery(_) =>
            "Hyperindex can answer _meta { deployment block { number timestamp } } (timestamp needs an indexer with Hasura's _meta; deployment is the deployment hash of HYPERINDEX_URL). block.hash and hasIndexingErrors have no Hyperindex source. The query is rejected rather than served partially, because a response missing these fields would be indistinguishable from one where they were false or zero.",
        conversion::ConversionError::MissingOperationName =>
            "The document holds several operations. Set operationName to the one to run.",
        conversion::ConversionError::UnknownOperation(_) =>
            "operationName does not match any operation in the document.",
        conversion::ConversionError::InvalidBlockNumber(_) =>
            "graph-node reads number_gte as a block number and rejects anything else, null included. A negative number is accepted and always met.",
        conversion::ConversionError::UnsupportedBlockConstraint(_) =>
            "Only block: { number_gte: N } can be served: it answers from the latest state once block N is indexed. Reads pinned to a past block (number or hash) need historical state, which Hyperindex does not keep.",
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

/// Answer `block: {number_gte: N}` as graph-node does: with the latest data once
/// block N is indexed, and with an error before then.
///
/// The converted query carries the indexer's progress under
/// `conversion::BLOCK_CHECK_KEY`, one row per chain; the lowest is the block
/// every chain has reached. The key is always removed, so the caller never sees
/// it.
fn enforce_min_block(response: &mut Value, min_block: Option<u64>, deployment: &str) {
    let Some(data) = response.get_mut("data").and_then(Value::as_object_mut) else {
        return;
    };
    let Some(progress) = data.remove(conversion::BLOCK_CHECK_KEY) else {
        return;
    };
    let Some(required) = min_block else {
        return;
    };
    // Upstream errors are reported as they are, not hidden behind this one.
    if response.get("errors").is_some() {
        return;
    }
    let indexed = lowest_progress_block(&progress);
    if indexed.map(|n| n >= required).unwrap_or(false) {
        return;
    }
    // graph-node's wording, so clients that match on it keep working. With no
    // progress row at all the indexer has not processed a block yet.
    let indexed = match indexed {
        Some(n) => format!("has only indexed up to block number {}", n),
        None => "has not indexed any blocks yet".to_string(),
    };
    *response = graphql_errors([format!(
        "Failed to decode `block.number_gte` value: `subgraph {} {} and data for block number {} is therefore not yet available`",
        deployment, indexed, required
    )]);
}

/// The lowest processed block among indexing-progress rows, from Hasura's native
/// `_meta` (`progressBlock`) or `chain_metadata` (`latest_processed_block`).
fn lowest_progress_block(rows: &Value) -> Option<u64> {
    rows.as_array()?
        .iter()
        .filter_map(|row| {
            row.get("progressBlock")
                .or_else(|| row.get("latest_processed_block"))
                .and_then(Value::as_u64)
        })
        .min()
}

/// Replace each one-row list read in place of a `_by_pk` lookup with its row, or
/// null when nothing matched, which is what the lookup would have returned.
fn unwrap_single_rows(response: &mut Value, keys: &[String]) {
    if let Some(Value::Object(data)) = response.get_mut("data") {
        for key in keys {
            if let Some(value) = data.get_mut(key) {
                let row = match value.take() {
                    Value::Array(rows) => rows.into_iter().next().unwrap_or(Value::Null),
                    other => other,
                };
                *value = row;
            }
        }
    }
}

/// Merge rewritten interface-link fields back into their rows (see `fragments`).
/// Runs on the raw Hyperindex response, before root keys are renamed.
fn apply_shape_plans(
    response: &mut Value,
    plans: &std::collections::HashMap<String, fragments::ShapePlan>,
) {
    if plans.is_empty() {
        return;
    }
    if let Some(Value::Object(data)) = response.get_mut("data") {
        for (root_key, plan) in plans {
            if let Some(value) = data.get_mut(root_key) {
                plan.apply(value);
            }
        }
    }
}

/// The upstream URL, with anything secret-shaped removed, for logs.
///
/// `HYPERINDEX_URL` is operator-supplied and can carry credentials in the
/// userinfo or the query string. Scheme, host, port and path are kept because
/// they identify which deployment was being talked to, which is the reason to
/// log it at all.
fn loggable_upstream(url: &str) -> String {
    match reqwest::Url::parse(url) {
        Ok(mut parsed) => {
            let _ = parsed.set_username("");
            let _ = parsed.set_password(None);
            parsed.set_query(None);
            parsed.set_fragment(None);
            parsed.to_string()
        }
        Err(_) => "<unparseable HYPERINDEX_URL>".to_string(),
    }
}

/// An upstream failure with the request URL stripped out.
///
/// `reqwest::Error`'s `Display` embeds the URL it was given - "error sending
/// request for url (http://internal-host:8080/v1/graphql?key=…)" - so it reaches
/// both response bodies and logs. `HYPERINDEX_URL` is operator-supplied and can
/// carry credentials in its userinfo or query string, so neither destination
/// should see it; `loggable_upstream` reports a sanitized address instead.
///
/// Prefer `redact_upstream_error` where the error itself is in hand: `reqwest`
/// records the URL it was given, so it can be removed exactly rather than
/// located in the text. This is the fallback for anything that is only
/// `Display`.
fn redact_upstream_url(e: &impl std::fmt::Display) -> String {
    let text = e.to_string();
    let Some(marker) = text.find("for url (") else {
        return text;
    };
    let open = marker + "for url ".len();

    // The *matching* close paren, not the last one in the message. An error can
    // carry further parenthesized text after the URL ("… (os error 61)"), and
    // running to the final `)` would swallow it.
    let mut depth = 0usize;
    let mut redacted = text.clone();
    for (offset, ch) in text[open..].char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    redacted.replace_range(marker..=open + offset, "for url (<upstream>)");
                    return redacted;
                }
            }
            _ => {}
        }
    }

    // Unbalanced. Redact to the end rather than hand back the URL.
    redacted.replace_range(marker.., "for url (<upstream>)");
    redacted
}

/// An upstream failure with the request URL stripped out, for the boxed errors
/// returned by `forward_to_hyperindex` and the `schema` module.
///
/// `reqwest::Error::url` holds the URL the error was raised against, so it can
/// be removed wherever it appears in the message instead of being matched on the
/// surrounding text. The source chain is walked too, since the recording error
/// may be wrapped. `redact_upstream_url` still runs afterwards, to catch a
/// `for url (…)` in an error that no longer carries the URL itself.
fn redact_upstream_error(e: &(dyn std::error::Error + 'static)) -> String {
    let mut text = e.to_string();

    let mut current = Some(e);
    while let Some(err) = current {
        if let Some(url) = err
            .downcast_ref::<reqwest::Error>()
            .and_then(reqwest::Error::url)
        {
            text = text.replace(url.as_str(), "<upstream>");
        }
        current = err.source();
    }

    redact_upstream_url(&text)
}

fn transform_response_to_subgraph_shape(resp: Value, field_name_map: &std::collections::HashMap<String, String>, meta_selections: &[conversion::MetaSelection]) -> Value {
    let mut root = match resp {
        Value::Object(map) => map,
        other => return other,
    };

    if let Some(Value::Object(data_obj)) = root.get_mut("data") {
        // `_meta` arrives as Hyperindex's progress rows; rebuild graph-node's object.
        for selection in meta_selections {
            if let Some(rows) = data_obj.remove(&selection.key) {
                data_obj.insert(selection.key.clone(), subgraph_meta(&rows, selection));
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

/// graph-node's `_meta` object from Hyperindex's progress rows (Hasura `_meta`
/// or `chain_metadata`), with only the fields the client selected.
///
/// With one row per chain, the lowest processed block is reported: the block
/// every chain has reached.
fn subgraph_meta(rows: &Value, selection: &conversion::MetaSelection) -> Value {
    let progress = |row: &&Value| {
        row.get("progressBlock")
            .or_else(|| row.get("latest_processed_block"))
            .and_then(Value::as_u64)
    };
    let row = rows
        .as_array()
        .and_then(|rows| rows.iter().filter(|row| progress(row).is_some()).min_by_key(|row| progress(row)));

    let mut meta = serde_json::Map::new();
    if selection.block_number || selection.block_timestamp || selection.block_typename {
        let mut block = serde_json::Map::new();
        if selection.block_number {
            let number = row.and_then(|r| progress(&r)).map(Value::from).unwrap_or(Value::Null);
            block.insert("number".to_string(), number);
        }
        if selection.block_timestamp {
            // graph-node's `timestamp` is unix seconds; Hasura gives timestamptz.
            let timestamp = row
                .and_then(|r| r.get("progressBlockTime"))
                .and_then(Value::as_str)
                .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
                .map(|t| Value::from(t.timestamp()))
                .unwrap_or(Value::Null);
            block.insert("timestamp".to_string(), timestamp);
        }
        if selection.block_typename {
            block.insert("__typename".to_string(), Value::String("_Block_".into()));
        }
        meta.insert("block".to_string(), Value::Object(block));
    }
    if selection.deployment {
        // graph-node's IPFS id; Hyperindex's equivalent is the endpoint's hash.
        let deployment = conversion::deployment_id().map(Value::String).unwrap_or(Value::Null);
        meta.insert("deployment".to_string(), deployment);
    }
    if selection.typename {
        meta.insert("__typename".to_string(), Value::String("_Meta_".into()));
    }
    Value::Object(meta)
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
                            "details": redact_upstream_error(&*e),
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
                            "details": redact_upstream_error(&*e)
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
            // Redacted, like the log below. A `reqwest` failure here embeds the
            // introspection URL, which is `HYPERINDEX_URL` and may carry
            // credentials.
            let err_msg = redact_upstream_error(&*e);
            metrics::SCHEMA_REFRESH_ERRORS.inc();
            tracing::error!("Failed to refresh schema: {}", redact_upstream_error(&*e));
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

    /// An operator-supplied `HYPERINDEX_URL` can carry credentials, and
    /// `reqwest::Error`'s `Display` repeats it verbatim.
    #[test]
    fn upstream_errors_do_not_carry_the_request_url() {
        let raw = "error sending request for url (http://user:pass@internal-host:8080/v1/graphql?api_key=secret)";
        let redacted = super::redact_upstream_url(&raw);
        assert!(!redacted.contains("internal-host"), "{redacted}");
        assert!(!redacted.contains("secret"), "{redacted}");
        assert!(!redacted.contains("user:pass"), "{redacted}");
        assert!(redacted.contains("error sending request"), "{redacted}");
    }

    #[test]
    fn redaction_stops_at_the_url_and_keeps_the_rest_of_the_message() {
        // Running to the last `)` in the message swallowed everything after the
        // URL, including the part that says what actually went wrong.
        let raw = "error sending request for url (http://internal-host:8080/v1/graphql): connection refused (os error 61)";
        let redacted = super::redact_upstream_url(&raw);
        assert!(!redacted.contains("internal-host"), "{redacted}");
        assert!(redacted.contains("connection refused (os error 61)"), "{redacted}");
    }

    #[test]
    fn an_unbalanced_url_parenthesis_is_redacted_to_the_end() {
        // Fail closed: no matching `)` means the extent of the URL is unknown,
        // so the tail goes rather than the URL staying.
        let raw = "error sending request for url (http://user:pass@internal-host:8080";
        let redacted = super::redact_upstream_url(&raw);
        assert!(!redacted.contains("internal-host"), "{redacted}");
        assert!(!redacted.contains("user:pass"), "{redacted}");
    }

    /// The real thing: a boxed `reqwest::Error` from a failed request, which is
    /// what `forward_to_hyperindex` and the `schema` module hand back.
    #[tokio::test]
    async fn a_boxed_reqwest_error_loses_its_url() {
        // Port 1 is reserved, so this refuses the connection without leaving the
        // machine. The credentials and query string are the parts that matter.
        let url = "http://user:hunter2@127.0.0.1:1/v1/graphql?api_key=SUPERSECRET";
        let err = reqwest::Client::new().get(url).send().await.unwrap_err();
        // reqwest lifts the userinfo out into an auth header, so the URL it
        // records - and repeats in `Display` - keeps the host and query string
        // but not the credentials.
        let recorded = err.url().map(|u| u.to_string()).unwrap_or_default();
        assert!(
            recorded.contains("SUPERSECRET"),
            "reqwest no longer records the URL ({recorded:?}); the redaction needs rechecking"
        );

        let boxed: Box<dyn std::error::Error + Send + Sync> = Box::new(err);
        let redacted = super::redact_upstream_error(&*boxed);

        for secret in ["hunter2", "SUPERSECRET", "127.0.0.1", "api_key"] {
            assert!(!redacted.contains(secret), "{secret} survived in {redacted}");
        }
        assert!(redacted.contains("<upstream>"), "{redacted}");
    }

    #[test]
    fn an_error_without_a_url_is_left_alone() {
        let raw = "operation timed out";
        assert_eq!(super::redact_upstream_url(&raw), "operation timed out");
    }

    #[test]
    fn the_logged_upstream_address_keeps_the_path_but_drops_secrets() {
        let logged = super::loggable_upstream("http://user:pass@host:8080/v1/graphql?api_key=secret");
        assert!(logged.contains("host:8080"), "{logged}");
        assert!(logged.contains("/v1/graphql"), "{logged}");
        assert!(!logged.contains("secret"), "{logged}");
        assert!(!logged.contains("user"), "{logged}");
        assert!(!logged.contains("pass"), "{logged}");
    }

    #[test]
    fn an_unparseable_upstream_address_is_not_echoed() {
        assert_eq!(
            super::loggable_upstream("not a url"),
            "<unparseable HYPERINDEX_URL>"
        );
    }
    use super::*;
    use serde_json::json;

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
        let out = transform_response_to_subgraph_shape(resp, &empty_map, &[]);
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

        let out = transform_response_to_subgraph_shape(resp, &field_map, &[]);
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
                "_meta": [
                    {"progressBlock": 419538012}
                ]
            }
        });
        let empty_map = std::collections::HashMap::new();
        let out = transform_response_to_subgraph_shape(
            resp,
            &empty_map,
            &[conversion::MetaSelection {
                block_number: true,
                key: "_meta".to_string(),
                ..conversion::MetaSelection::default()
            }],
        );
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

    #[test]
    fn interface_fragment_response_is_reshaped_to_the_subgraph_form() {
        crate::schema::init_test_schema_once();
        let payload = json!({
            "query": "query Q($u: String!) { userTransactions(where: {user: $u}, orderBy: timestamp, orderDirection: desc, first: 3) { id action ... on Supply { amount reserve { symbol } } ... on Borrow { borrowRateMode } } }",
            "variables": {"u": "0xabc"}
        });
        let converted = conversion::convert_subgraph_to_hyperindex(&payload, None).unwrap();

        // What Hasura returns for the converted query.
        let mut hasura = json!({"data": {"UserTransaction": [
            {"id": "a", "action": "Supply", "_on_Supply": {"amount": "5", "reserve": {"symbol": "WSOMI"}}, "_on_Borrow": null},
            {"id": "b", "action": "Borrow", "_on_Supply": null, "_on_Borrow": {"borrowRateMode": 2}},
            {"id": "c", "action": "Supply", "_on_Supply": null, "_on_Borrow": null}
        ]}});
        apply_shape_plans(&mut hasura, &converted.shape_plans);
        let out = transform_response_to_subgraph_shape(
            hasura,
            &converted.field_name_map,
            &converted.meta_selections,
        );

        assert_eq!(
            out,
            json!({"data": {"userTransactions": [
                {"id": "a", "action": "Supply", "amount": "5", "reserve": {"symbol": "WSOMI"}},
                {"id": "b", "action": "Borrow", "borrowRateMode": 2},
                {"id": "c", "action": "Supply"}
            ]}})
        );
    }

    #[test]
    fn aliased_roots_come_back_under_their_alias() {
        let payload = json!({
            "query": "query { Recent: streams(first: 1) { id } top: tokens(first: 1) { id } }"
        });
        let converted = conversion::convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let hasura = json!({"data": {
            "Recent": [{"id": "s1"}],
            "top": [{"id": "t1"}]
        }});
        let out = transform_response_to_subgraph_shape(
            hasura,
            &converted.field_name_map,
            &converted.meta_selections,
        );
        assert_eq!(
            out,
            json!({"data": {"Recent": [{"id": "s1"}], "top": [{"id": "t1"}]}})
        );
    }

    #[test]
    fn number_gte_passes_once_every_chain_has_reached_the_block() {
        let mut resp = json!({"data": {
            "Token": [{"id": "t"}],
            "__converter_meta": [{"progressBlock": 120}, {"progressBlock": 100}]
        }});
        enforce_min_block(&mut resp, Some(100), "b232c6d");
        assert_eq!(resp, json!({"data": {"Token": [{"id": "t"}]}}));
    }

    #[test]
    fn number_gte_fails_while_any_chain_is_behind() {
        let mut resp = json!({"data": {
            "Token": [{"id": "t"}],
            "__converter_meta": [{"progressBlock": 120}, {"progressBlock": 99}]
        }});
        enforce_min_block(&mut resp, Some(100), "b232c6d");
        assert!(resp.get("data").is_none(), "{resp}");
        assert_eq!(
            resp,
            json!({"errors": [{"message": "Failed to decode `block.number_gte` value: `subgraph b232c6d has only indexed up to block number 99 and data for block number 100 is therefore not yet available`"}]})
        );
    }

    #[test]
    fn number_gte_reads_the_chain_metadata_fallback() {
        let mut resp = json!({"data": {
            "Token": [],
            "__converter_meta": [{"latest_processed_block": 50}]
        }});
        enforce_min_block(&mut resp, Some(60), "b232c6d");
        assert!(resp["errors"][0]["message"].as_str().unwrap().contains("block number 50"));
    }

    #[test]
    fn progress_key_is_removed_even_without_a_requirement() {
        let mut resp = json!({"data": {"Token": [], "__converter_meta": [{"progressBlock": 1}]}});
        enforce_min_block(&mut resp, None, "b232c6d");
        assert_eq!(resp, json!({"data": {"Token": []}}));
    }

    #[test]
    fn number_gte_end_to_end_strips_the_progress_root() {
        crate::schema::init_test_schema_once();
        let payload = json!({"query": "{ tokens(first: 1, block: {number_gte: 10}) { id } }"});
        let converted = conversion::convert_subgraph_to_hyperindex(&payload, None).unwrap();
        assert_eq!(converted.min_block, Some(10));
        let mut hasura = json!({"data": {
            "Token": [{"id": "t"}],
            "__converter_meta": [{"progressBlock": 10}]
        }});
        enforce_min_block(&mut hasura, converted.min_block, "b232c6d");
        let out = transform_response_to_subgraph_shape(
            hasura,
            &converted.field_name_map,
            &converted.meta_selections,
        );
        assert_eq!(out, json!({"data": {"tokens": [{"id": "t"}]}}));
    }

    #[test]
    fn meta_response_reports_the_lowest_chain_with_its_block_time() {
        let selection = conversion::MetaSelection {
            block_number: true,
            block_timestamp: true,
            typename: true,
            block_typename: true,
            deployment: false,
            key: "_meta".to_string(),
        };
        let rows = json!([
            {"progressBlock": 442680100, "progressBlockTime": "2026-10-19T15:03:00+00:00"},
            {"progressBlock": 442680053, "progressBlockTime": "2026-10-19T15:02:36+00:00"}
        ]);
        assert_eq!(
            subgraph_meta(&rows, &selection),
            json!({
                "__typename": "_Meta_",
                "block": {"number": 442680053, "timestamp": 1792422156i64, "__typename": "_Block_"}
            })
        );
    }

    #[test]
    fn meta_timestamp_with_fractional_seconds_and_null() {
        let selection = conversion::MetaSelection {
            block_timestamp: true,
            key: "_meta".to_string(),
            ..conversion::MetaSelection::default()
        };
        assert_eq!(
            subgraph_meta(
                &json!([{"progressBlock": 1, "progressBlockTime": "2026-10-19T13:05:36.429+00:00"}]),
                &selection
            ),
            json!({"block": {"timestamp": 1792415136i64}})
        );
        // During backfill Hasura has no block time yet; graph-node's field is nullable.
        assert_eq!(
            subgraph_meta(&json!([{"progressBlock": 1, "progressBlockTime": null}]), &selection),
            json!({"block": {"timestamp": null}})
        );
    }

    #[test]
    fn meta_alongside_entities_keeps_the_rest_of_the_response() {
        crate::schema::init_test_schema_once();
        let payload = json!({"query": "query { m: _meta { block { number } } tokens(first: 1) { id } }"});
        let converted = conversion::convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let hasura = json!({"data": {
            "m": [{"progressBlock": 7}],
            "Token": [{"id": "t"}]
        }});
        let out = transform_response_to_subgraph_shape(
            hasura,
            &converted.field_name_map,
            &converted.meta_selections,
        );
        assert_eq!(
            out,
            json!({"data": {"m": {"block": {"number": 7}}, "tokens": [{"id": "t"}]}})
        );
    }

    #[test]
    fn meta_response_from_chain_metadata_fallback() {
        let selection = conversion::MetaSelection {
            block_number: true,
            key: "_meta".to_string(),
            ..conversion::MetaSelection::default()
        };
        assert_eq!(
            subgraph_meta(&json!([{"latest_processed_block": 99}]), &selection),
            json!({"block": {"number": 99}})
        );
    }

    #[test]
    fn responses_without_plans_are_untouched() {
        let mut resp = json!({"data": {"Stream": [{"id": "1", "_on_Supply": null}]}});
        let before = resp.clone();
        apply_shape_plans(&mut resp, &std::collections::HashMap::new());
        assert_eq!(resp, before);
    }

    #[test]
    fn chain_route_lookup_returns_the_row_or_null() {
        crate::schema::init_test_schema_once();
        let payload = serde_json::json!({
            "query": "{ token(id: \"0xa\") { id } other: token(id: \"0xb\") { id } }"
        });
        let converted = conversion::convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let mut hasura = serde_json::json!({"data": {
            "Token_by_pk": [{"id": "0xa"}],
            "other": []
        }});
        unwrap_single_rows(&mut hasura, &converted.single_row_keys);
        let out = transform_response_to_subgraph_shape(
            hasura,
            &converted.field_name_map,
            &converted.meta_selections,
        );
        assert_eq!(
            out,
            serde_json::json!({"data": {"token": {"id": "0xa"}, "other": null}})
        );
    }


    #[test]
    fn upstream_errors_keep_only_their_messages() {
        let hasura = json!([
            {"message": "field 'foo' not found in type: 'Swap'", "extensions": {"path": "$.selectionSet.Swap.selectionSet.foo", "code": "validation-failed"}},
            {"extensions": {"code": "unexpected"}}
        ]);
        assert_eq!(
            subgraph_errors(&hasura),
            json!({"errors": [
                {"message": "field 'foo' not found in type: 'Swap'"},
                {"message": "{\"extensions\":{\"code\":\"unexpected\"}}"}
            ]})
        );
    }

    #[test]
    fn debug_errors_switch_values() {
        assert!(!debug_errors_setting(None));
        assert!(!debug_errors_setting(Some("off")));
        for on in ["on", "ON", " true ", "1", "yes"] {
            assert!(debug_errors_setting(Some(on)), "{on}");
        }
    }

    #[tokio::test]
    async fn query_the_converter_cannot_run_is_a_graphql_error_with_status_200() {
        crate::schema::init_test_schema_once();
        let payload = json!({"query": "{ _meta { hasIndexingErrors block { number } } }"});
        let (status, Json(body)) = execute_query_with_retry(&payload, None).await;
        assert_eq!(status, StatusCode::OK);
        let errors = body["errors"].as_array().unwrap();
        assert_eq!(errors.len(), 1);
        assert!(errors[0]["message"].as_str().unwrap().contains("hasIndexingErrors"), "{body}");
        // graph-node's shape: no `data`, and none of the converter's diagnostics.
        assert_eq!(body.as_object().unwrap().keys().collect::<Vec<_>>(), vec!["errors"]);
    }

    #[tokio::test]
    async fn request_without_a_query_string_is_a_400() {
        crate::schema::init_test_schema_once();
        for payload in [json!({}), json!({"query": 7})] {
            let (status, Json(body)) = execute_query_with_retry(&payload, None).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{payload}");
            let error = body["error"].as_str().unwrap();
            assert!(error.starts_with("GraphQL server error (client error): "), "{error}");
            assert_eq!(body.as_object().unwrap().len(), 1, "{body}");
        }
    }


    #[test]
    fn meta_deployment_response_is_the_indexer_hash() {
        crate::schema::init_test_schema_once();
        let payload = json!({"query": "{ _meta { deployment block { number } } }"});
        let converted = conversion::convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let hasura = json!({"data": {"_meta": [{"chainId": 5031, "progressBlock": 10}]}});
        let out = transform_response_to_subgraph_shape(
            hasura,
            &converted.field_name_map,
            &converted.meta_selections,
        );
        assert_eq!(
            out,
            json!({"data": {"_meta": {"deployment": "b232c6d", "block": {"number": 10}}}})
        );
    }


    #[test]
    fn number_gte_with_no_progress_rows_says_so() {
        let mut resp = json!({"data": {"Token": [], "__converter_meta": []}});
        enforce_min_block(&mut resp, Some(10), "b232c6d");
        let message = resp["errors"][0]["message"].as_str().unwrap();
        assert!(message.contains("subgraph b232c6d has not indexed any blocks yet"), "{message}");
        assert!(!message.contains("number no"), "{message}");
    }

    #[test]
    fn upstream_errors_are_not_replaced_by_the_block_error() {
        let mut resp = json!({
            "data": {"__converter_meta": [{"progressBlock": 1}]},
            "errors": [{"message": "upstream"}]
        });
        enforce_min_block(&mut resp, Some(10), "b232c6d");
        assert_eq!(resp["errors"], json!([{"message": "upstream"}]));
        assert!(resp["data"].get("__converter_meta").is_none());
    }

    #[test]
    fn several_meta_roots_are_rebuilt() {
        crate::schema::init_test_schema_once();
        let payload = json!({"query": "{ a: _meta { block { number } } b: _meta { deployment } }"});
        let converted = conversion::convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let hasura = json!({"data": {
            "a": [{"progressBlock": 7}],
            "b": [{"progressBlock": 7}]
        }});
        let out = transform_response_to_subgraph_shape(
            hasura,
            &converted.field_name_map,
            &converted.meta_selections,
        );
        assert_eq!(
            out,
            json!({"data": {"a": {"block": {"number": 7}}, "b": {"deployment": "b232c6d"}}})
        );
    }

}
