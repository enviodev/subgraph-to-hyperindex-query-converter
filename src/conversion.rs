use serde_json::Value;
use std::collections::{HashMap, HashSet};
use thiserror::Error;

use crate::fragments::{self, ShapePlan};
use crate::schema;

#[derive(Error, Debug)]
pub enum ConversionError {
    #[error("Invalid GraphQL query format")]
    InvalidQueryFormat,
    #[error("The \"query\" field is not a string")]
    QueryNotString,
    #[error("The \"{0}\" field is missing in request data")]
    MissingField(String),
    #[error("Unsupported filter: {0}")]
    UnsupportedFilter(String),
    #[error("Nested field `{0}` has arguments that cannot be translated: {1}")]
    UnsupportedNestedArgument(String, String),
    #[error("_meta fields with no Hyperindex equivalent: {0}. Only _meta {{ deployment block {{ number timestamp }} }} can be served (block.timestamp needs an indexer with Hasura's _meta, deployment a HYPERINDEX_URL with a deployment hash), and a partial answer would look like a complete one to the caller")]
    ComplexMetaQuery(String),
    #[error("Must provide operation name if query contains multiple operations")]
    MissingOperationName,
    #[error("Unknown operation named \"{0}\"")]
    UnknownOperation(String),
    #[error("Invalid value for `block: {{ number_gte }}`: {0}")]
    InvalidBlockNumber(String),
    #[error("Historical queries are not supported: {0}. Hyperindex serves only the latest indexed state, so a query pinned to a past block cannot be answered")]
    UnsupportedBlockConstraint(String),
}

impl ConversionError {
    /// Whether the request itself is malformed (no usable `query` string), which
    /// graph-node answers with HTTP 400, rather than the query failing, which it
    /// answers with a GraphQL error.
    pub fn is_request_error(&self) -> bool {
        matches!(
            self,
            ConversionError::MissingField(_) | ConversionError::QueryNotString
        )
    }
}

/// Where one half of an `order_by` pair comes from: a GraphQL variable the client
/// sends, or a literal written into the query text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderValueSource {
    Variable(String),
    Literal(String),
}

/// An `order_by` argument whose value has to be assembled at request time.
///
/// Subgraph spells dynamic ordering as two scalar variables
/// (`orderBy: $orderBy, orderDirection: $direction`), but GraphQL has no variable
/// object keys, so `order_by: {$orderBy: $direction}` cannot be written. Instead we
/// retype the client's own `$orderBy` to `[Entity_order_by!]` and rebuild its JSON
/// value as `[{field: direction}]` before forwarding.
#[derive(Debug, Clone)]
pub struct OrderBySpec {
    /// Variable whose JSON value we overwrite — normally the client's own `orderBy`.
    pub target_var: String,
    pub field_source: OrderValueSource,
    pub direction_source: OrderValueSource,
    /// Hyperindex entity being ordered, for logging.
    pub entity: String,
}

/// Result of query conversion, including the converted query and field name mappings
#[derive(Debug)]
pub struct ConversionResult {
    pub query: Value,
    /// Maps Hyperindex field names (e.g., "LpAction") to original query field names (e.g., "lpActions")
    pub field_name_map: HashMap<String, String>,
    /// Maps variable names (without `$`) to the Hyperindex entity name (e.g., "UniswapDayData")
    /// for variables that were declared with a subgraph `*_filter` type and used as a
    /// top-level `where: $var` argument. The caller is expected to translate the JSON
    /// value of each such variable via `translate_subgraph_filter_value` before forwarding
    /// upstream.
    pub filter_variable_entities: HashMap<String, String>,
    /// Order-by variables whose JSON value must be rebuilt into Hasura's
    /// `[{field: direction}]` shape before forwarding upstream.
    /// Which `_meta` sub-fields the caller asked for, when this was a `_meta`
    /// query. The response transform needs it to echo back exactly those fields.
    pub meta_selections: Vec<MetaSelection>,
    pub order_by_variables: Vec<OrderBySpec>,
    /// Variables whose declaration was removed from the operation header because
    /// nothing references them any more. GraphQL rejects a declared-but-unused
    /// variable, so these are stripped from the forwarded payload too.
    pub dropped_variables: Vec<String>,
    /// Per root response key (the Hyperindex field name), how to merge rewritten
    /// interface-link fields back into subgraph-shaped rows. See `crate::fragments`.
    pub shape_plans: HashMap<String, ShapePlan>,
    /// The block a `block: {number_gte: N}` argument requires to be indexed. The
    /// converted query carries the indexer's progress under `BLOCK_CHECK_KEY`,
    /// for the caller to compare against this before answering.
    pub min_block: Option<u64>,
    /// Response keys of single-entity lookups that were read as a one-row list
    /// (on the `/chainId/{id}` route, where `_by_pk` cannot filter by chain).
    /// The caller replaces each list with its row, or null.
    pub single_row_keys: Vec<String>,
}

/// Response key of the hidden `_meta` root that reports indexing progress for a
/// `block: {number_gte: N}` check. Removed from the response before it is returned.
pub const BLOCK_CHECK_KEY: &str = "__converter_meta";

/// Where the `N` of a `block: {number_gte: N}` argument comes from. A variable
/// carries its declared default, used when the request leaves it out.
#[derive(Debug, Clone, PartialEq)]
enum MinBlockSource {
    /// `block: {number_gte: 5}`; `None` when N is negative, which graph-node
    /// treats as always met.
    Literal(Option<u64>),
    /// `block: {number_gte: $n}`
    NumberVariable(String, Option<Value>),
    /// `block: $b`, a whole `Block_height` value.
    BlockVariable(String, Option<Value>),
}

/// A `number_gte` value as graph-node reads it: an `i32` block number (an
/// integer, or a string holding one) that is met once `latest >= N`, so a
/// negative N is always met. Anything else, null included, is an error.
fn min_block_number(value: Option<&Value>, what: &str) -> Result<Option<u64>, ConversionError> {
    let n = match value {
        None | Some(Value::Null) => {
            return Err(ConversionError::InvalidBlockNumber(format!(
                "{} is null or missing",
                what
            )))
        }
        Some(Value::Number(n)) => n.as_i64(),
        Some(Value::String(s)) => s.trim().parse::<i64>().ok(),
        Some(_) => None,
    };
    match n {
        Some(n) if n < 0 => Ok(None),
        Some(n) if n <= i64::from(i32::MAX) => Ok(Some(n as u64)),
        _ => Err(ConversionError::InvalidBlockNumber(format!(
            "{} is not a block number: {}",
            what,
            value.map(Value::to_string).unwrap_or_default()
        ))),
    }
}

pub fn convert_subgraph_to_hyperindex(
    payload: &Value,
    chain_id: Option<&str>,
) -> Result<ConversionResult, ConversionError> {
    // Extract the query from the payload
    let query = payload
        .get("query")
        .ok_or(ConversionError::MissingField("query".to_string()))?
        .as_str()
        .ok_or(ConversionError::QueryNotString)?;

    tracing::info!("Converting query: {}", query);

    // A document may hold several operations; `operationName` says which to run.
    // Hasura is only ever sent that one, with the fragments it uses.
    let selected = select_operation(
        query,
        payload.get("operationName").and_then(|v| v.as_str()),
    )?;
    let query = selected.as_ref().map(|s| s.query.as_str()).unwrap_or(query);

    // Check if this is an introspection query **only by operation name**.
    // We ONLY bypass conversion when the operation name is `IntrospectionQuery`.
    let trimmed = query.trim_start();
    let is_introspection = if trimmed.starts_with("query") {
        // Look at the header before the operation's selection set
        let header_end = operation_body_start(trimmed).unwrap_or(trimmed.len());
        let header = &trimmed[..header_end];
        header.contains("IntrospectionQuery")
    } else {
        false
    };

    if is_introspection {
        tracing::info!("Detected introspection query, passing through unchanged");
        let mut result = serde_json::json!({
            "query": query
        });
        if let Some(variables) = payload.get("variables") {
            result["variables"] = variables.clone();
        }
        return Ok(ConversionResult {
            query: result,
            field_name_map: HashMap::new(),
            meta_selections: Vec::new(),
            filter_variable_entities: HashMap::new(),
            order_by_variables: Vec::new(),
            dropped_variables: Vec::new(),
            shape_plans: HashMap::new(),
            min_block: None,
            single_row_keys: Vec::new(),
        });
    }

    // Check if this is a _meta query before conversion
    // Parse the GraphQL query (simplified parsing for now)
    let MainConversion {
        converted_query,
        field_name_map,
        filter_variable_entities,
        order_by_variables,
        dropped_variables,
        meta_selections,
        shape_plans,
        fragments_override: _,
        min_blocks,
        like_variables,
        chain_filter_variables,
        single_row_keys,
        limit_variables,
        where_defaults,
        required_where_variables,
    } = convert_query_structure(query, chain_id)?;
    let defaults = graph_node_defaults();

    // Variables are read before any are dropped from the forwarded payload: a
    // `$n` that only fed `number_gte` has no place left in the query.
    let min_block = resolve_min_block(&min_blocks, payload.get("variables"))?;

    // Build the result with query and optionally variables
    let mut result = serde_json::json!({
        "query": converted_query
    });

    // Extract variables, translating any subgraph `*_filter`-typed variables that
    // were used as `where: $var`. We rewrite the variable value in-place from
    // subgraph filter shape (flat keys like `date_gt`) into Hasura `bool_exp` shape
    // (`date: {_gt: ...}`). This keeps clients that still send subgraph-shaped
    // filter JSON working transparently after the query-level rewrite.
    // A pattern variable can need a value (from a declared default) even when
    // the request sends no variables at all.
    let variables = payload
        .get("variables")
        .cloned()
        .or_else(|| {
            (!like_variables.is_empty()
                || !chain_filter_variables.is_empty()
                || !limit_variables.is_empty()
                || !where_defaults.is_empty())
                .then(|| serde_json::json!({}))
        });
    if let Some(mut variables) = variables {
        // Values for the operations that were not selected would be rejected as
        // unexpected variables.
        if let (Some(selected), Value::Object(map)) = (&selected, &mut variables) {
            map.retain(|name, _| selected.variables.contains(name));
        }
        // A `where` default left out of the request is sent as the value, so it
        // is translated and chain-filtered like one the client sent.
        if let Value::Object(map) = &mut variables {
            for (var_name, default) in &where_defaults {
                map.entry(var_name.clone()).or_insert_with(|| default.clone());
            }
        }
        if !filter_variable_entities.is_empty() {
            if let Value::Object(map) = &mut variables {
                for (var_name, entity_name) in &filter_variable_entities {
                    if let Some(v) = map.get(var_name).cloned() {
                        let translated = translate_subgraph_filter_value(&v, entity_name);
                        map.insert(var_name.clone(), translated);
                    }
                }
            }
        }
        // graph-node reads `first: null` as its default page; Hasura as no limit.
        if let (Value::Object(map), Some(first)) = (&mut variables, defaults.first) {
            for var in &limit_variables {
                let value = match map.get(&var.name) {
                    Some(value) => Some(value),
                    None if var.non_null => continue,
                    None => var.default.as_ref(),
                };
                if value.map_or(true, Value::is_null) {
                    map.insert(var.name.clone(), Value::from(first));
                }
            }
        }
        // After translation, so the chain condition is added in Hasura's shape.
        if let Value::Object(map) = &mut variables {
            for (var_name, chain_id) in &chain_filter_variables {
                if !map.contains_key(var_name) && required_where_variables.contains(var_name) {
                    continue;
                }
                let filter = map.remove(var_name).unwrap_or(Value::Null);
                map.insert(var_name.clone(), with_chain_condition(filter, chain_id));
            }
        }
        // Rebuild `order_by` variables into Hasura's `[{field: direction}]` shape.
        if !order_by_variables.is_empty() {
            if let Value::Object(map) = &mut variables {
                for spec in &order_by_variables {
                    let field = match &spec.field_source {
                        OrderValueSource::Literal(s) => Some(s.clone()),
                        OrderValueSource::Variable(name) => map
                            .get(name)
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string()),
                    };
                    let field = match field.filter(|f| !f.trim().is_empty()) {
                        Some(f) => f,
                        // Nothing to order by: graph-node's default `id` order,
                        // whatever the direction says. With that default off,
                        // the variable is left unset and Hasura doesn't order.
                        None if defaults.order_by_id => {
                            map.insert(
                                spec.target_var.clone(),
                                Value::Array(vec![order_by_json("id", "asc")]),
                            );
                            continue;
                        }
                        None => continue,
                    };
                    let direction = match &spec.direction_source {
                        OrderValueSource::Literal(s) => s.clone(),
                        OrderValueSource::Variable(name) => map
                            .get(name)
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string())
                            .unwrap_or_else(|| "asc".to_string()),
                    };
                    tracing::debug!(
                        "Rebuilding order_by variable ${} for {} as {} {}",
                        spec.target_var,
                        spec.entity,
                        field,
                        direction
                    );
                    map.insert(
                        spec.target_var.clone(),
                        order_by_json_with_tiebreak(&field, &direction, id_tiebreak_enabled()),
                    );
                }
            }
        }
        // Patterns are built before their source variable is dropped below.
        if let Value::Object(map) = &mut variables {
            for spec in &like_variables {
                if let Some(pattern) = like_variable_value(spec, map) {
                    map.insert(spec.target.clone(), pattern);
                }
            }
        }
        // Declarations we removed from the header must not be forwarded either.
        if !dropped_variables.is_empty() {
            if let Value::Object(map) = &mut variables {
                for name in &dropped_variables {
                    if !order_by_variables.iter().any(|sp| &sp.target_var == name) {
                        map.remove(name);
                    }
                }
            }
        }
        result["variables"] = variables;
    }

    Ok(ConversionResult {
        query: result,
        field_name_map,
        meta_selections,
        filter_variable_entities,
        order_by_variables,
        dropped_variables,
        shape_plans,
        min_block,
        single_row_keys,
    })
}

/// A `where` variable's value restricted to one chain: the chain condition alone
/// when the client sent none (or an empty one), otherwise both under `_and`.
fn with_chain_condition(filter: Value, chain_id: &str) -> Value {
    let chain = serde_json::json!({"chainId": {"_eq": chain_id}});
    match filter {
        Value::Null => chain,
        Value::Object(ref m) if m.is_empty() => chain,
        filter => serde_json::json!({"_and": [filter, chain]}),
    }
}

/// The highest block any `block: {number_gte: N}` argument requires, with
/// variables taken from the request. An unset variable sets no requirement.
fn resolve_min_block(
    sources: &[MinBlockSource],
    variables: Option<&Value>,
) -> Result<Option<u64>, ConversionError> {
    let variable = |name: &str, default: &Option<Value>| {
        variables
            .and_then(|vars| vars.get(name))
            .or(default.as_ref())
            .cloned()
    };
    let mut required: Option<u64> = None;
    for source in sources {
        let block = match source {
            MinBlockSource::Literal(n) => *n,
            MinBlockSource::NumberVariable(name, default) => {
                min_block_number(variable(name, default).as_ref(), &format!("${}", name))?
            }
            // A null `block` is graph-node's default: the latest block.
            MinBlockSource::BlockVariable(name, default) => match variable(name, default) {
                None | Some(Value::Null) => None,
                Some(Value::Object(fields)) => {
                    if fields.is_empty() {
                        return Err(ConversionError::UnsupportedBlockConstraint(
                            "block: {}".to_string(),
                        ));
                    }
                    if let Some(key) = fields.keys().find(|k| k.as_str() != "number_gte") {
                        return Err(ConversionError::UnsupportedBlockConstraint(format!(
                            "block: {{ {}: ... }}",
                            key
                        )));
                    }
                    min_block_number(fields.get("number_gte"), &format!("${}.number_gte", name))?
                }
                Some(other) => {
                    return Err(ConversionError::UnsupportedBlockConstraint(format!(
                        "block: {}",
                        other
                    )))
                }
            },
        };
        if let Some(block) = block {
            required = Some(required.map_or(block, |r: u64| r.max(block)));
        }
    }
    Ok(required)
}

/// The operation `operationName` picked out of a multi-operation document.
struct SelectedOperation {
    /// The operation followed by the fragment definitions it reaches.
    query: String,
    /// Variables the operation declares.
    variables: HashSet<String>,
}

/// One top-level definition of a GraphQL document, exactly as written.
struct Definition {
    text: String,
    /// The fragment's name, for a fragment definition.
    fragment: Option<String>,
    /// The operation's name; `None` for fragments and anonymous operations.
    operation_name: Option<String>,
}

/// Reduce a document holding several operations to the one `operation_name`
/// selects, plus the fragments it uses, as the GraphQL spec's GetOperation does.
///
/// Returns `Ok(None)` when there is at most one operation, so there is nothing to
/// choose, or when the document cannot be split; the caller then carries on with
/// the document unchanged.
fn select_operation(
    query: &str,
    operation_name: Option<&str>,
) -> Result<Option<SelectedOperation>, ConversionError> {
    let definitions = match split_definitions(query) {
        Some(defs) => defs,
        None => return Ok(None),
    };
    let operations: Vec<&Definition> =
        definitions.iter().filter(|d| d.fragment.is_none()).collect();
    if operations.len() < 2 {
        return Ok(None);
    }

    let chosen = match operation_name {
        None => return Err(ConversionError::MissingOperationName),
        Some(name) => operations
            .iter()
            .find(|d| d.operation_name.as_deref() == Some(name))
            .ok_or_else(|| ConversionError::UnknownOperation(name.to_string()))?,
    };

    // Fragments the operation reaches, directly or through other fragments. An
    // unused fragment is a validation error, so the rest are left out.
    let by_name: HashMap<&str, &Definition> = definitions
        .iter()
        .filter_map(|d| d.fragment.as_deref().map(|name| (name, d)))
        .collect();
    let mut reached: Vec<&str> = Vec::new();
    let mut pending = fragment_spreads(&chosen.text);
    while let Some(name) = pending.pop() {
        if reached.iter().any(|r| *r == name) {
            continue;
        }
        if let Some((key, def)) = by_name.get_key_value(name.as_str()) {
            reached.push(key);
            pending.extend(fragment_spreads(&def.text));
        }
    }

    let mut text = chosen.text.clone();
    for def in definitions.iter().filter(|d| {
        d.fragment
            .as_deref()
            .map(|name| reached.contains(&name))
            .unwrap_or(false)
    }) {
        text.push('\n');
        text.push_str(&def.text);
    }

    let header_end = operation_body_start(&chosen.text).unwrap_or(chosen.text.len());
    let variables = parse_variable_declarations(&chosen.text[..header_end])
        .into_keys()
        .collect();
    Ok(Some(SelectedOperation {
        query: text,
        variables,
    }))
}

/// Split a document into its top-level definitions.
///
/// A definition runs until the `}` that closes its selection set brings the
/// nesting depth back to zero; brackets in the header (variable defaults) and
/// string literals do not end it. Returns `None` for unbalanced input.
fn split_definitions(doc: &str) -> Option<Vec<Definition>> {
    let chars: Vec<char> = doc.chars().collect();
    let mut definitions = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() || c == ',' {
            i += 1;
            continue;
        }
        if c == '#' {
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
            }
            continue;
        }

        let start = i;
        let mut depth = 0i32;
        let mut in_str = false;
        let mut end = None;
        while i < chars.len() {
            let c = chars[i];
            if in_str {
                if c == '\\' {
                    i += 1;
                } else if c == '"' {
                    in_str = false;
                }
            } else {
                match c {
                    '"' => in_str = true,
                    '#' => {
                        while i < chars.len() && chars[i] != '\n' {
                            i += 1;
                        }
                        continue;
                    }
                    '(' | '[' | '{' => depth += 1,
                    ')' | ']' => depth -= 1,
                    '}' => {
                        depth -= 1;
                        if depth == 0 {
                            end = Some(i);
                            i += 1;
                            break;
                        }
                    }
                    _ => {}
                }
            }
            i += 1;
        }
        let end = end?;
        let text: String = chars[start..=end].iter().collect();

        let words: Vec<String> = text
            .split(|c: char| !(c.is_alphanumeric() || c == '_'))
            .filter(|w| !w.is_empty())
            .take(2)
            .map(str::to_string)
            .collect();
        let (fragment, operation_name) = match words.first().map(String::as_str) {
            _ if text.starts_with('{') => (None, None),
            Some("fragment") => (Some(words.get(1)?.clone()), None),
            Some("query") | Some("mutation") | Some("subscription") => {
                // `query {` and `query($a: Int)` are anonymous.
                let after_keyword = text[words[0].len()..].trim_start();
                let named = after_keyword
                    .chars()
                    .next()
                    .map(|c| c.is_alphabetic() || c == '_')
                    .unwrap_or(false);
                (None, if named { words.get(1).cloned() } else { None })
            }
            _ => return None,
        };
        definitions.push(Definition {
            text,
            fragment,
            operation_name,
        });
    }
    Some(definitions)
}

/// Names of the fragments spread (`...Name`) anywhere in `text`. Inline
/// fragments (`... on Type`, `... @include`) are not spreads.
fn fragment_spreads(text: &str) -> Vec<String> {
    let mut names = Vec::new();
    let mut rest = text;
    while let Some(at) = rest.find("...") {
        rest = &rest[at + 3..];
        let name: String = rest
            .trim_start()
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        if !name.is_empty() && name != "on" {
            names.push(name);
        }
    }
    names
}

/// Internal aggregate returned by `convert_query_structure` / `convert_main_query`.
struct MainConversion {
    converted_query: String,
    field_name_map: HashMap<String, String>,
    /// Variable name (without `$`) -> entity name (e.g., "UniswapDayData"). Only
    /// populated for variables that appeared as top-level `where: $var` and were
    /// declared with a subgraph-style `*_filter` type in the query header.
    filter_variable_entities: HashMap<String, String>,
    order_by_variables: Vec<OrderBySpec>,
    dropped_variables: Vec<String>,
    /// Which `_meta` sub-fields were requested, for shaping the response.
    meta_selections: Vec<MetaSelection>,
    /// Response reshaping for rewritten interface fragments, by root response key.
    shape_plans: HashMap<String, ShapePlan>,
    /// Replacement named-fragment definitions, when rewriting changed any of them.
    fragments_override: Option<String>,
    /// `block: {number_gte: N}` requirements found on root fields.
    min_blocks: Vec<MinBlockSource>,
    /// Variables carrying the LIKE pattern for a `name_contains: $q`.
    like_variables: Vec<LikeVariable>,
    /// `where: $var` variables that need the route's chain condition added.
    chain_filter_variables: HashMap<String, String>,
    /// Response keys of lookups read as one-row lists; see `ConversionResult`.
    single_row_keys: Vec<String>,
    /// `first: $var` variables to give graph-node's page size when null.
    limit_variables: Vec<LimitVariable>,
    /// Declared defaults of `where: $var` variables, as JSON. The default is
    /// taken off the header (it is in subgraph syntax, which Hasura cannot read)
    /// and sent as the value when the request leaves the variable out.
    where_defaults: HashMap<String, Value>,
    /// `where: $var` variables declared non-null. One the request leaves out
    /// is the client's error, as in graph-node, so nothing is filled in for it.
    required_where_variables: HashSet<String>,
}

/// A `first: $var` variable, which gets graph-node's default page size when the
/// request leaves it null (Hasura would return every row).
#[derive(Debug, Clone)]
struct LimitVariable {
    name: String,
    /// The declared default, which Hasura applies itself when the variable is
    /// left out.
    default: Option<Value>,
    /// A non-null variable left out is the client's error, as in graph-node.
    non_null: bool,
}

fn convert_query_structure(
    query: &str,
    chain_id: Option<&str>,
) -> Result<MainConversion, ConversionError> {
    // Extract fragments and main query
    let (fragments, main_query) = extract_fragments_and_main_query(query)?;

    // Convert the main query
    let main = convert_main_query(&main_query, chain_id, &fragments)?;

    // Combine fragments with converted main query
    let fragments = main.fragments_override.clone().unwrap_or(fragments);
    let mut result = String::new();
    if !fragments.is_empty() {
        result.push_str(&fragments);
        result.push('\n');
    }
    result.push_str(&main.converted_query);

    Ok(MainConversion {
        converted_query: result,
        field_name_map: main.field_name_map,
        filter_variable_entities: main.filter_variable_entities,
        order_by_variables: main.order_by_variables,
        dropped_variables: main.dropped_variables,
        meta_selections: main.meta_selections,
        shape_plans: main.shape_plans,
        fragments_override: None,
        min_blocks: main.min_blocks,
        like_variables: main.like_variables,
        chain_filter_variables: main.chain_filter_variables,
        single_row_keys: main.single_row_keys,
        limit_variables: main.limit_variables,
        where_defaults: main.where_defaults,
        required_where_variables: main.required_where_variables,
    })
}

fn extract_fragments_and_main_query(query: &str) -> Result<(String, String), ConversionError> {
    // Handle both multi-line and single-line queries.
    // Strategy: scan the full string for 'fragment ' blocks and remove them from main.
    let mut fragments = String::new();
    let mut remaining = query.to_string();

    loop {
        if let Some(start_idx) = remaining.find("fragment ") {
            // Find the start of the fragment body '{'
            let after_start = &remaining[start_idx..];
            if let Some(open_idx_rel) = after_start.find('{') {
                let open_idx = start_idx + open_idx_rel;
                // Walk to the matching '}'
                let mut brace_count = 1;
                let mut pos = open_idx + 1;
                let chars: Vec<char> = remaining.chars().collect();
                while pos < chars.len() {
                    match chars[pos] {
                        '{' => brace_count += 1,
                        '}' => {
                            brace_count -= 1;
                            if brace_count == 0 {
                                // Capture the fragment text [start_idx..=pos]
                                let fragment_text: String = chars[start_idx..=pos].iter().collect();
                                if !fragments.is_empty() {
                                    fragments.push('\n');
                                }
                                fragments.push_str(fragment_text.trim());

                                // Remove it from remaining
                                let prefix: String = chars[..start_idx].iter().collect();
                                let suffix: String = chars[pos + 1..].iter().collect();
                                remaining = format!("{}{}", prefix.trim_end(), suffix);
                                break;
                            }
                        }
                        _ => {}
                    }
                    pos += 1;
                }
                // Continue loop to find next fragment in updated 'remaining'
                continue;
            } else {
                // 'fragment ' without body; stop scanning to avoid infinite loop
                break;
            }
        } else {
            break;
        }
    }

    let main_query = remaining.trim().to_string();
    Ok((fragments, main_query))
}

/// Convert variable type definitions from ID/Bytes/BigInt/BigDecimal to the
/// concrete scalar types expected by Hyperindex/Hasura.
/// - ID, Bytes  -> String
/// - BigInt, BigDecimal -> numeric
fn convert_variable_types_in_header(
    header: &str,
    variable_type_overrides: &HashMap<String, String>,
) -> String {
    // Replace ID/Bytes/BigInt/BigDecimal types in variable definitions
    // Also apply enum type overrides for variables used with enum fields
    // Handle patterns like:
    // - $id: ID! -> $id: String!
    // - $id: ID -> $id: String
    // - $bytes: Bytes! -> $bytes: String!
    // - $bytes: Bytes -> $bytes: String
    // - $ids: [ID!]! -> $ids: [String!]!
    // - $ids: [Bytes!] -> $ids: [String!]
    // - $amount: BigInt! -> $amount: numeric!
    // - $amounts: [BigInt!]! -> $amounts: [numeric!]!

    // Normalize whitespace (replace newlines/tabs with spaces) to handle
    // multi-line variable definitions
    let normalized: String = header
        .chars()
        .map(|c| if c.is_whitespace() { ' ' } else { c })
        .collect();

    // Collapse multiple spaces into single spaces
    let mut result = String::new();
    let mut prev_was_space = false;
    for c in normalized.chars() {
        if c == ' ' {
            if !prev_was_space {
                result.push(' ');
            }
            prev_was_space = true;
        } else {
            result.push(c);
            prev_was_space = false;
        }
    }

    // Replace array types first (more specific patterns)
    result = result.replace("[ID!]", "[String!]");
    result = result.replace("[Bytes!]", "[String!]");
    result = result.replace("[ID]", "[String]");
    result = result.replace("[Bytes]", "[String]");
    result = result.replace("[BigInt!]", "[numeric!]");
    result = result.replace("[BigInt]", "[numeric]");
    result = result.replace("[BigDecimal!]", "[numeric!]");
    result = result.replace("[BigDecimal]", "[numeric]");

    // Replace non-nullable types
    result = result.replace(": ID!", ": String!");
    result = result.replace(": Bytes!", ": String!");
    result = result.replace(": BigInt!", ": numeric!");
    result = result.replace(": BigDecimal!", ": numeric!");

    // Replace nullable types using simple word-boundary-ish patterns
    // Use word boundaries by checking for space/comma/paren after the type
    result = result.replace(": ID ", ": String ");
    result = result.replace(": ID,", ": String,");
    result = result.replace(": ID)", ": String)");
    result = result.replace(": Bytes ", ": String ");
    result = result.replace(": Bytes,", ": String,");
    result = result.replace(": Bytes)", ": String)");
    result = result.replace(": BigInt ", ": numeric ");
    result = result.replace(": BigInt,", ": numeric,");
    result = result.replace(": BigInt)", ": numeric)");
    result = result.replace(": BigDecimal ", ": numeric ");
    result = result.replace(": BigDecimal,", ": numeric,");
    result = result.replace(": BigDecimal)", ": numeric)");

    // Handle end of string cases (no trailing delimiter)
    if result.ends_with(": ID") {
        result.truncate(result.len() - 3);
        result.push_str(": String");
    }
    if result.ends_with(": Bytes") {
        result.truncate(result.len() - 6);
        result.push_str(": String");
    }
    if result.ends_with(": BigInt") {
        result.truncate(result.len() - 7);
        result.push_str(": numeric");
    }
    if result.ends_with(": BigDecimal") {
        result.truncate(result.len() - 11);
        result.push_str(": numeric");
    }

    // Apply variable type overrides for enum and numeric types
    // For each variable that needs type conversion, replace its declared type
    for (var_name, expected_type) in variable_type_overrides {
        // Look for patterns like "$operation: String" and replace with "$operation: orderaction"
        // Or "$epoch: String" -> "$epoch: numeric"
        // Handle various type patterns: Type, Type!, [Type], [Type!], etc.
        let var_prefix = format!("${}: ", var_name);
        if let Some(var_start) = result.find(&var_prefix) {
            let type_start = var_start + var_prefix.len();
            // Find the end of the type (next comma, paren, or space)
            let remaining = &result[type_start..];
            let type_end = remaining
                .find(|c: char| c == ',' || c == ')' || c == ' ')
                .unwrap_or(remaining.len());

            let current_type = &remaining[..type_end];

            // Override String to expected type (enum or numeric)
            // Also handle Int -> numeric if needed
            let should_convert = current_type == "String"
                || current_type == "String!"
                || current_type == "Int"
                || current_type == "Int!";

            if should_convert {
                let new_type = if current_type.ends_with('!') {
                    format!("{}!", expected_type)
                } else {
                    expected_type.clone()
                };

                tracing::info!(
                    "Converting variable ${} type from {} to {} (field type mismatch)",
                    var_name,
                    current_type,
                    new_type
                );

                let before = &result[..type_start];
                let after = &result[type_start + type_end..];
                result = format!("{}{}{}", before, new_type, after);
            }
        }
    }

    result
}

/// Where an operation's selection set opens: the first `{` outside its variable
/// definitions, whose default values may contain braces of their own
/// (`query Q($w: Token_filter = {symbol: "A"}) {`).
fn operation_body_start(text: &str) -> Option<usize> {
    let mut depth = 0i32;
    let mut in_str = false;
    let mut escape = false;
    for (i, c) in text.char_indices() {
        if in_str {
            match c {
                _ if escape => escape = false,
                '\\' => escape = true,
                '"' => in_str = false,
                _ => {}
            }
            continue;
        }
        match c {
            '"' => in_str = true,
            '(' => depth += 1,
            ')' => depth -= 1,
            '{' if depth <= 0 => return Some(i),
            _ => {}
        }
    }
    None
}

/// Parse `$name: Type` pairs out of a query header.
///
/// Best-effort lexical scan — the header here is the substring before the
/// first `{` of the operation, e.g. `query Q($a: Foo_filter, $b: Int!)`. The
/// returned type string is the raw text up to the next `,` or `)` (so it
/// includes `!`, list brackets, etc.).
fn parse_variable_declarations(header: &str) -> HashMap<String, String> {
    let mut result = HashMap::new();
    let bytes = header.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] != b'$' {
            i += 1;
            continue;
        }
        i += 1;
        let name_start = i;
        while i < bytes.len() {
            let c = bytes[i] as char;
            if c.is_ascii_alphanumeric() || c == '_' {
                i += 1;
            } else {
                break;
            }
        }
        if name_start == i {
            continue;
        }
        let name = header[name_start..i].to_string();
        while i < bytes.len() && (bytes[i] as char).is_whitespace() {
            i += 1;
        }
        if i >= bytes.len() || bytes[i] != b':' {
            continue;
        }
        i += 1;
        while i < bytes.len() && (bytes[i] as char).is_whitespace() {
            i += 1;
        }
        let type_start = i;
        // A default value (`= {a: 1, b: [2, 3]}`) may hold commas and brackets.
        let mut depth = 0i32;
        let mut in_str = false;
        while i < bytes.len() {
            let b = bytes[i];
            if in_str {
                match b {
                    b'\\' => i += 1,
                    b'"' => in_str = false,
                    _ => {}
                }
            } else {
                match b {
                    b'"' => in_str = true,
                    b'{' | b'[' => depth += 1,
                    b'}' | b']' => depth -= 1,
                    b',' | b')' if depth <= 0 => break,
                    _ => {}
                }
            }
            i += 1;
        }
        let raw_type = header[type_start..i].trim().to_string();
        if !raw_type.is_empty() {
            result.insert(name, raw_type);
        }
    }
    result
}

/// Returns true if `declared_type` is a subgraph filter type — i.e. its
/// non-null/list-stripped name ends in `_filter`. Used to decide whether
/// to translate the variable's JSON value from subgraph filter shape to
/// Hasura `bool_exp` shape.
fn is_subgraph_filter_type(declared_type: &str) -> bool {
    // Strip any default value, non-null markers, list brackets, and whitespace.
    let declared_type = declared_type.split('=').next().unwrap_or_default();
    let cleaned: String = declared_type
        .chars()
        .filter(|c| !c.is_whitespace() && *c != '!' && *c != '[' && *c != ']')
        .collect();
    cleaned.ends_with("_filter") && cleaned.len() > "_filter".len()
}

fn convert_main_query(
    main_query: &str,
    chain_id: Option<&str>,
    fragments_text: &str,
) -> Result<MainConversion, ConversionError> {
    // Extract query header (name and variable definitions) and body separately
    let (query_header, stripped_query) = if main_query.trim().starts_with("query") {
        let content = main_query.trim();
        if let (Some(start_brace), Some(end_brace)) =
            (operation_body_start(content), content.rfind('}'))
        {
            // Extract everything before the first '{' as the header (e.g., "query Trades($pairid: String!)")
            let header = content[..start_brace].trim().to_string();
            // Extract everything inside the braces as the body
            let body = content[start_brace + 1..end_brace].to_string();
            tracing::debug!(
                "Extracted query header: '{}', body length: {}",
                header,
                body.len()
            );
            (Some(header), body)
        } else {
            (None, main_query.to_string())
        }
    } else if main_query.trim().starts_with('{') {
        // Already a selection body, no header. Strip exactly one brace at each
        // end: `trim_start_matches`/`trim_end_matches` remove *every* repeat, so
        // `{reserves{id}}` lost the inner `}` and the selection never closed.
        let trimmed = main_query.trim();
        let body = trimmed
            .strip_prefix('{')
            .map(|rest| rest.strip_suffix('}').unwrap_or(rest))
            .unwrap_or(trimmed);
        (None, body.to_string())
    } else {
        (None, main_query.to_string())
    };

    // Extract multiple entities from the main query body
    let mut roots = extract_multiple_entities(&stripped_query)?;

    let mut converted_entities = Vec::new();
    // Build mapping from Hyperindex field names to original query field names
    let mut field_name_map: HashMap<String, String> = HashMap::new();
    // Parse variable declarations once for the whole query.
    let mut ctx = ArgContext {
        header_var_types: query_header
            .as_deref()
            .map(parse_variable_declarations)
            .unwrap_or_default(),
        defaults: graph_node_defaults(),
        ..ArgContext::default()
    };

    // graph-node-only arguments are taken off before anything reads the rest as
    // filters, or `block` would become a `where` on a column that does not exist.
    let mut min_blocks = Vec::new();
    for root in roots.iter_mut() {
        min_blocks.extend(take_block_arguments(root, &mut ctx)?);
    }

    // `_meta` reads Hyperindex's indexing progress, not an entity.
    let (meta_roots, roots): (Vec<RootField>, Vec<RootField>) =
        roots.into_iter().partition(|root| root.name == "_meta");
    let native_meta = native_meta_available();
    let mut meta_selections: Vec<MetaSelection> = Vec::new();
    let mut meta_lines = Vec::new();
    for meta in meta_roots {
        // graph-node allows `_meta` more than once under different aliases; the
        // same key twice is one field that GraphQL merges, served once.
        let key = meta.alias.clone().unwrap_or_else(|| "_meta".to_string());
        if meta_selections.iter().any(|m| m.key == key) {
            continue;
        }
        let selection = convert_meta_selection(
            &meta.selection,
            native_meta,
            deployment_id().is_some(),
            key.clone(),
        )?;
        // The response key is rebuilt in place and must not be renamed after.
        field_name_map.insert(key.clone(), key.clone());
        let line = progress_root(&key, chain_id, native_meta, selection.block_timestamp);
        // Directives go before the selection set, as on any other root field.
        let line = match meta.directives.is_empty() {
            true => line,
            false => line.replacen(" {\n", &format!(" {} {{\n", meta.directives), 1),
        };
        meta_lines.push(line);
        meta_selections.push(selection);
    }

    // Each root's Hyperindex entity, and whether it is a primary-key lookup.
    let classified: Vec<(String, bool)> = roots
        .iter()
        .map(|root| classify_root_field(&root.name, &root.params))
        .collect();
    // Arguments inside the selections, and inside named fragments, are converted
    // first: the interface rewrite below parses them.
    let mut selections: Vec<String> = Vec::with_capacity(roots.len());
    for (root, (entity_cap, _)) in roots.iter().zip(&classified) {
        let converted = convert_selection_arguments(&root.selection, Some(entity_cap), &mut ctx)?;
        selections.push(format!("{{\n    {}\n  }}", converted));
    }
    let converted_fragments = match split_definitions(fragments_text) {
        Some(definitions) => {
            let mut converted = Vec::with_capacity(definitions.len());
            for definition in definitions {
                converted.push(convert_fragment_arguments(&definition.text, &mut ctx)?);
            }
            converted.join("\n")
        }
        None => fragments_text.to_string(),
    };
    let fragments_text = converted_fragments.as_str();

    // Type-conditioned fragments (`... on Supply { }`) on an interface stand-in are
    // rewritten to its link fields before anything below sees the selection.
    let rewrites = {
        let root_pairs: Vec<(&str, &str)> = classified
            .iter()
            .zip(&selections)
            .map(|((ty, _), selection)| (ty.as_str(), selection.as_str()))
            .collect();
        fragments::rewrite_all(fragments_text, &root_pairs)
    };
    let fragments_override = rewrites
        .fragments
        .or_else(|| (!fragments_text.is_empty()).then(|| fragments_text.to_string()));
    let mut root_rewrites = rewrites.roots.into_iter();
    let mut shape_plans: HashMap<String, ShapePlan> = HashMap::new();
    let mut single_row_keys = Vec::new();

    // Response keys handed out in this operation, each with the caller key it
    // resolves to. Hasura requires response names to be unique, and
    // `field_name_map` has to stay injective or two roots collapse onto one key
    // in the rename pass. Two roots that share a caller key are the deliberate
    // merge case (`{ trades { a } trades { b } }`) and keep the same key.
    let mut used_response_keys: HashMap<String, String> = HashMap::new();

    for ((root, (entity_cap, is_pk)), selection) in roots.into_iter().zip(classified).zip(selections)
    {
        let RootField {
            alias,
            name: entity,
            params,
            directives,
            ..
        } = root;
        let directives = if directives.is_empty() {
            String::new()
        } else {
            format!(" {}", directives)
        };

        // The key the caller expects to read the result under.
        let client_key = alias.clone().unwrap_or_else(|| entity.clone());
        // The key we would naturally emit for it.
        let natural_key = match (&alias, is_pk) {
            (Some(a), _) => a.clone(),
            (None, true) => format!("{}_by_pk", entity_cap),
            (None, false) => entity_cap.clone(),
        };
        // An alias is free-form, so it can collide with another root's entity
        // name (`users: reserves` beside `users`, or `Reserve: users` beside
        // `reserves`). Hasura rejects duplicate response names outright, and a
        // duplicate `field_name_map` key silently drops one result set, so
        // disambiguate before either can happen.
        let response_key = {
            let mut candidate = natural_key.clone();
            let mut n = 2;
            while used_response_keys
                .get(&candidate)
                .is_some_and(|taken| *taken != client_key)
            {
                candidate = format!("{}_{}", natural_key, n);
                n += 1;
            }
            candidate
        };
        used_response_keys.insert(response_key.clone(), client_key.clone());

        let selection = match root_rewrites.next().flatten() {
            Some(rewrite) => {
                // Key by the root field actually emitted below, and merge: the same
                // entity can be selected twice in one operation (GraphQL merges them).
                shape_plans
                    .entry(response_key.clone())
                    .or_default()
                    .merge(rewrite.plan);
                rewrite.selection
            }
            None => selection,
        };

        // Record the mapping: emitted response key -> the key the caller asked
        // for. For an unaliased root those differ ("LpAction" -> "lpActions");
        // for an aliased one the alias maps to itself, and that identity entry
        // also keeps the PascalCase fallback in
        // `transform_response_to_subgraph_shape` from rewriting it.
        field_name_map.insert(response_key.clone(), client_key);

        // Hasura honours aliases, so the root is emitted under the key the
        // response needs.
        let hasura_field = if is_pk {
            format!("{}_by_pk", entity_cap)
        } else {
            entity_cap.clone()
        };
        let alias_prefix = if response_key == hasura_field {
            String::new()
        } else {
            format!("{}: ", response_key)
        };

        // Single-entity by primary key: singular entity, only 'id' param
        if is_pk && chain_id.is_some() {
            // `_by_pk` takes no filter, so it would return the row whatever its
            // chain. A one-row collection read under the same response key
            // keeps the chain filter; the caller unwraps it (`single_row_keys`).
            let mut filters = HashMap::new();
            filters.insert("id".to_string(), params["id"].clone());
            filters.insert(
                "chainId".to_string(),
                format!("\"{}\"", chain_id.unwrap_or_default()),
            );
            let converted = convert_filters_to_where_clause(&filters, &entity_cap)?;
            ctx.variable_type_overrides
                .extend(converted.variable_type_overrides);
            converted_entities.push(format!(
                "  {}: {}(limit: 1, {}){} {}",
                response_key, entity_cap, converted.where_clause, directives, selection
            ));
            single_row_keys.push(response_key);
            continue;
        }
        if is_pk {
            let pk_query = format!(
                "  {}{}(id: {}){} {}",
                alias_prefix,
                hasura_field,
                params.get("id").unwrap(),
                directives,
                selection
            );
            converted_entities.push(pk_query);
            continue;
        }

        let params_vec = convert_collection_args(&params, &entity_cap, chain_id, &mut ctx)?;
        let params_str = if params_vec.is_empty() {
            String::new()
        } else {
            format!("({})", params_vec.join(", "))
        };

        let converted_entity = format!(
            "  {}{}{}{} {}",
            alias_prefix, entity_cap, params_str, directives, selection
        );
        converted_entities.push(converted_entity);
    }

    converted_entities.extend(meta_lines);
    if !min_blocks.is_empty() {
        converted_entities.push(progress_root(
            BLOCK_CHECK_KEY,
            chain_id,
            native_meta_available(),
            false,
        ));
    }

    // Build the body first: whether a declaration is still referenced can only be
    // answered against the emitted arguments.
    let body = converted_entities.join("\n");
    let referenced = referenced_variables(&format!(
        "{}\n{}",
        body,
        fragments_override.as_deref().unwrap_or_default()
    ));
    // `name_contains: $q` now reads `$q__contains`, declared here as a String;
    // `$q` itself goes if nothing else reads it.
    let like_variables = like_variables(&referenced, &ctx.header_var_types);
    for spec in &like_variables {
        let non_null = ctx.header_var_types[&spec.source]
            .split('=')
            .next()
            .unwrap_or_default()
            .trim()
            .ends_with('!');
        let ty = if non_null { "String!" } else { "String" };
        ctx.order_by_retypes.insert(spec.target.clone(), ty.to_string());
        ctx.dead_candidates.push(spec.source.clone());
    }
    let removals: HashSet<String> = std::mem::take(&mut ctx.dead_candidates)
        .into_iter()
        .filter(|name| !referenced.contains(name))
        .collect();
    let mut dropped_variables: Vec<String> = removals.iter().cloned().collect();
    dropped_variables.sort();
    let mut where_defaults = HashMap::new();
    let mut required_where_variables = HashSet::new();
    let where_variables: HashSet<&String> = ctx
        .filter_variable_entities
        .keys()
        .chain(ctx.chain_filter_variables.keys())
        .collect();
    for var in where_variables {
        let Some(declared) = ctx.header_var_types.get(var) else {
            continue;
        };
        let ty = declared.split('=').next().unwrap_or_default().trim();
        if ty.ends_with('!') {
            required_where_variables.insert(var.clone());
        }
        if let Some(default) = declared_default(declared) {
            // A retype rewrites the declaration without its default.
            ctx.order_by_retypes
                .insert(var.clone(), rewrite_filter_type_names(ty));
            where_defaults.insert(var.clone(), default);
        }
    }
    let mut limit_variables: Vec<LimitVariable> = ctx
        .limit_variables
        .iter()
        .filter_map(|name| {
            let declared = ctx.header_var_types.get(name)?;
            Some(LimitVariable {
                name: name.clone(),
                default: declared_default(declared),
                non_null: declared.split('=').next().unwrap_or_default().trim().ends_with('!'),
            })
        })
        .collect();
    limit_variables.sort_by(|a, b| a.name.cmp(&b.name));

    // Reconstruct the query with preserved header (including variable definitions)
    // Convert ID and Bytes types to String in variable definitions
    let query_header_str = if let Some(header) = &query_header {
        // Convert variable type definitions: ID -> String, Bytes -> String, and enum types
        let converted_header =
            convert_variable_types_in_header(header, &ctx.variable_type_overrides);
        // Additionally rewrite subgraph filter type names (e.g., `Foo_filter`)
        // to their Hyperindex/Hasura equivalents (`Foo_bool_exp`). This is a
        // syntactic rename only — see `translate_subgraph_filter_value` for
        // the runtime JSON-value translation that goes with it.
        let converted_header = rewrite_filter_type_names(&converted_header);
        // Order-argument retypes and removals run last, over the now
        // whitespace-normalized single-line header.
        let converted_header =
            rewrite_variable_definitions(&converted_header, &ctx.order_by_retypes, &removals);
        tracing::debug!(
            "Using preserved query header: '{}' (converted to: '{}')",
            header,
            converted_header
        );
        converted_header
    } else {
        // Otherwise, use default "query"
        tracing::debug!("No query header found, using default 'query'");
        "query".to_string()
    };

    let converted_query = format!("{} {{\n{}\n}}", query_header_str, body);
    tracing::debug!("Final converted query: {}", converted_query);
    Ok(MainConversion {
        converted_query,
        field_name_map,
        filter_variable_entities: ctx.filter_variable_entities,
        order_by_variables: ctx.order_by_specs,
        dropped_variables,
        meta_selections,
        shape_plans,
        fragments_override,
        min_blocks,
        like_variables,
        chain_filter_variables: ctx.chain_filter_variables,
        single_row_keys,
        limit_variables,
        where_defaults,
        required_where_variables,
    })
}

/// Take graph-node's `block` and `subgraphError` arguments off a root field.
///
/// `block: {number_gte: N}` means "answer from the latest state, provided block N
/// is indexed"; the requirement is returned so the response can be checked.
/// `block: {number: N}` and `block: {hash: ..}` ask for the state at a past block,
/// which Hyperindex does not keep, so they are refused rather than answered from
/// the latest state. `subgraphError` chooses whether a failed subgraph may still
/// serve data; a Hyperindex indexer stops on a handler error instead, so there is
/// nothing to choose and the argument is dropped.
fn take_block_arguments(
    root: &mut RootField,
    ctx: &mut ArgContext,
) -> Result<Vec<MinBlockSource>, ConversionError> {
    let mut sources = Vec::new();
    if let Some(value) = root.params.remove("subgraphError") {
        if let Some(var) = value.trim().strip_prefix('$') {
            ctx.dead_candidates.push(var.to_string());
        }
    }
    let Some(value) = root.block_argument.take() else {
        return Ok(sources);
    };
    let unsupported = || ConversionError::UnsupportedBlockConstraint(format!("block: {}", value));
    if let Some(var) = value.strip_prefix('$') {
        ctx.dead_candidates.push(var.to_string());
        let default = ctx.header_var_types.get(var).and_then(|d| declared_default(d));
        sources.push(MinBlockSource::BlockVariable(var.to_string(), default));
        return Ok(sources);
    }
    // Only `number_gte` can be answered; anything else (`number`, `hash`, an
    // unknown key, an empty object) could be a stricter constraint than it looks.
    let fields = value
        .strip_prefix('{')
        .and_then(|rest| rest.strip_suffix('}'))
        .map(top_level_arguments)
        .unwrap_or_default();
    if fields.is_empty() {
        return Err(unsupported());
    }
    for field in fields {
        if field.name != "number_gte" {
            return Err(unsupported());
        }
        let n = field.value.trim();
        match n.strip_prefix('$') {
            Some(var) => {
                ctx.dead_candidates.push(var.to_string());
                let default = ctx.header_var_types.get(var).and_then(|d| declared_default(d));
                sources.push(MinBlockSource::NumberVariable(var.to_string(), default));
            }
            None => {
                let literal = graphql_literal_to_json(n);
                let block = min_block_number(literal.as_ref(), "block.number_gte")?;
                sources.push(MinBlockSource::Literal(block));
            }
        }
    }
    Ok(sources)
}

/// Whether the indexer exposes Hasura's native `_meta` root (newer Hyperindex
/// versions). Older ones only have `chain_metadata`.
fn native_meta_available() -> bool {
    schema::get_field_info("_meta", "progressBlock").is_some()
}

/// A root field reporting indexing progress, aliased to `key`: the processed
/// block and, with `with_time`, that block's timestamp. On the chain route it is
/// limited to that chain; otherwise it lists every chain the indexer covers.
fn progress_root(key: &str, chain_id: Option<&str>, native: bool, with_time: bool) -> String {
    let chain_value = |id: &str| {
        if id.chars().all(|c| c.is_ascii_digit()) {
            id.to_string()
        } else {
            format!("\"{}\"", id)
        }
    };
    let (source, chain_column, fields) = if native {
        let fields = if with_time {
            "progressBlock progressBlockTime"
        } else {
            "progressBlock"
        };
        ("_meta", "chainId", fields)
    } else {
        ("chain_metadata", "chain_id", "latest_processed_block")
    };
    let filter = chain_id
        .map(|id| format!("(where: {{{}: {{_eq: {}}}}})", chain_column, chain_value(id)))
        .unwrap_or_default();
    let alias = if key == source {
        String::new()
    } else {
        format!("{}: ", key)
    };
    format!("  {}{}{} {{\n    {}\n  }}", alias, source, filter, fields)
}

/// Per-operation state that argument conversion builds up across every root and
/// nested field: header retypes, variables whose JSON value is rewritten before
/// forwarding, and declarations that may have become unused.
#[derive(Default)]
struct ArgContext {
    /// Declared types from the operation header, by variable name.
    header_var_types: HashMap<String, String>,
    /// Variables declared `String`/`Int` that the field they filter needs typed
    /// otherwise (enums, numeric).
    variable_type_overrides: HashMap<String, String>,
    /// Variables used as `where: $var` with a subgraph `*_filter` type, by the
    /// entity whose filter shape their value must be translated into.
    filter_variable_entities: HashMap<String, String>,
    /// `where: $var` variables on the `/chainId/{id}` route, by that chain id. The
    /// chain condition is added to their value at request time.
    chain_filter_variables: HashMap<String, String>,
    /// `first: $var` variables, which get graph-node's page size when the request
    /// leaves them null.
    limit_variables: HashSet<String>,
    /// Unconditional header retypes, for order arguments and pattern variables.
    /// A name the client never declared gets a declaration of its own. Kept
    /// separate from `variable_type_overrides`, which only ever replaces a
    /// declared `String`/`Int` and so cannot touch `OrderDirection!` or
    /// `Entity_orderBy!`.
    order_by_retypes: HashMap<String, String>,
    order_by_specs: Vec<OrderBySpec>,
    /// Declarations that may have become unused; confirmed against the built body.
    dead_candidates: Vec<String>,
    order_var_owner: HashMap<String, String>,
    synth_counter: usize,
    /// graph-node's page size and sort for collections that omit them.
    defaults: GraphNodeDefaults,
}

/// The Hyperindex entity a root field reads, and whether it is a primary-key
/// lookup (`token(id: "0x..")`) rather than a collection (`tokens(...)`).
///
/// graph-node names the single-entity field after the entity (`swapUser`) and the
/// collection after its plural (`swapUsers`), so the schema settles both. Without
/// a schema match, a name not ending in `s` and carrying only `id` is taken as a
/// lookup.
fn classify_root_field(name: &str, params: &HashMap<String, String>) -> (String, bool) {
    let id_only = params.len() == 1 && params.contains_key("id");
    if let Some(entity) = schema::resolve_entity_for_collection_field(name) {
        let singular = schema::is_singular_field_for(name, &entity).unwrap_or(false);
        return (entity, singular && id_only);
    }
    let entity = singularize_and_capitalize(name);
    (entity, !name.ends_with('s') && id_only)
}

/// Hasura arguments for a collection field from its subgraph arguments: `first`
/// and `skip` become `limit`/`offset`, `orderBy`/`orderDirection` become
/// `order_by`, and everything else is a filter for `where`.
fn convert_collection_args(
    params: &HashMap<String, String>,
    entity_cap: &str,
    chain_id: Option<&str>,
    ctx: &mut ArgContext,
) -> Result<Vec<String>, ConversionError> {
    // Extract limit/offset, preserving GraphQL variables (e.g., $first/$skip)
    let limit = params.get("first").cloned();
    let offset = params.get("skip").cloned();

    let mut converted_params = params.clone();

    // Add chainId to params if provided
    if let Some(chain_id) = chain_id {
        converted_params.insert("chainId".to_string(), format!("\"{}\"", chain_id));
    }

    // Check if where is a variable reference (e.g., where: $where)
    let where_is_variable = params
        .get("where")
        .map(|w| w.trim_start().starts_with('$'))
        .unwrap_or(false);

    // Convert filters to where clause (flattened)
    // If where is a variable, it will be handled separately
    let (where_clause, entity_var_overrides) = if where_is_variable {
        // If where is a variable, pass it through directly
        let clause = if let Some(where_var) = params.get("where") {
            // Only opt the variable into runtime JSON translation when it
            // was declared with a subgraph-style `*_filter` type. Other
            // input-object types pass through verbatim — both for
            // backwards compatibility and because we don't know how to
            // translate an arbitrary user-defined input type.
            let var_name = where_var.trim().trim_start_matches('$').to_string();
            // `where: {_and: [{chainId: ...}, $var]}` is rejected for a nullable
            // `$var`, since `_and` items are non-null.
            if let (Some(chain_id), false) = (chain_id, var_name.is_empty()) {
                ctx.chain_filter_variables
                    .insert(var_name.clone(), chain_id.to_string());
            }
            if !var_name.is_empty() {
                let declared = ctx.header_var_types.get(&var_name);
                if declared
                    .map(|t| is_subgraph_filter_type(t))
                    .unwrap_or(false)
                {
                    ctx.filter_variable_entities
                        .insert(var_name, entity_cap.to_string());
                }
            }
            format!("where: {}", where_var)
        } else {
            String::new()
        };
        (clause, HashMap::new())
    } else {
        let result = convert_filters_to_where_clause(&converted_params, entity_cap)?;
        (result.where_clause, result.variable_type_overrides)
    };

    // Merge variable type overrides
    ctx.variable_type_overrides.extend(entity_var_overrides);

    // Map orderBy/orderDirection to Hasura order_by
    let order_arg = plan_order_by(
        params,
        entity_cap,
        &ctx.header_var_types,
        &mut ctx.order_by_retypes,
        &mut ctx.order_by_specs,
        &mut ctx.dead_candidates,
        &mut ctx.order_var_owner,
        &mut ctx.synth_counter,
    );
    let defaults = ctx.defaults;
    // `first: null` is graph-node's default page too.
    let limit = limit.filter(|l| l.trim() != "null");
    if let (Some(var), Some(_)) = (
        limit.as_deref().and_then(|l| l.trim().strip_prefix('$')),
        defaults.first,
    ) {
        ctx.limit_variables.insert(var.to_string());
    }
    let limit = limit.or_else(|| defaults.first.map(|n| n.to_string()));
    let order_arg = order_arg.or_else(|| {
        defaults
            .order_by_id
            .then(|| format!("order_by: {}", literal_order_by("id", "asc")))
    });

    let mut params_vec = Vec::new();
    if let Some(l) = limit.as_ref() {
        // Include limit whether it's a literal or a variable (e.g., $first)
        params_vec.push(format!("limit: {}", l));
    }
    if let Some(o) = offset.as_ref() {
        // Include offset whether it's a literal or a variable (e.g., $skip)
        params_vec.push(format!("offset: {}", o));
    }
    if let Some(order_arg) = order_arg {
        params_vec.push(order_arg);
    }
    if !where_clause.is_empty() {
        // The where_clause already has the correct format, just use it directly
        params_vec.push(where_clause);
    }
    Ok(params_vec)
}

/// What graph-node does for a collection that omits `first` or `orderBy`: it
/// returns 100 rows, sorted by `id` ascending (any lone `orderDirection` is
/// ignored). Hasura would return every row, in no particular order, so a client
/// paging with `skip` and no `orderBy` could repeat or miss rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GraphNodeDefaults {
    /// Page size when `first` is absent; `None` leaves it unlimited.
    first: Option<u64>,
    /// Sort by `id` ascending when `orderBy` is absent.
    order_by_id: bool,
}

impl Default for GraphNodeDefaults {
    fn default() -> Self {
        Self::GRAPH_NODE
    }
}

impl GraphNodeDefaults {
    const GRAPH_NODE: GraphNodeDefaults = GraphNodeDefaults {
        first: Some(100),
        order_by_id: true,
    };

    /// `CONVERTER_DEFAULT_FIRST` sets the page size (`off`/`0` for none) and
    /// `CONVERTER_DEFAULT_ORDER_ID=off` drops the default sort, e.g. if sorting a
    /// very large table by `id` proves slow.
    fn from_env(var: impl Fn(&str) -> Option<String>) -> Self {
        let off = |v: &str| matches!(v, "0" | "false" | "off" | "no");
        let first = match var("CONVERTER_DEFAULT_FIRST").map(|v| v.trim().to_ascii_lowercase()) {
            Some(v) if off(&v) => None,
            Some(v) => v.parse().ok().or(Self::GRAPH_NODE.first),
            None => Self::GRAPH_NODE.first,
        };
        let order_by_id = !var("CONVERTER_DEFAULT_ORDER_ID")
            .map(|v| off(&v.trim().to_ascii_lowercase()))
            .unwrap_or(false);
        GraphNodeDefaults { first, order_by_id }
    }
}

/// The defaults in force. Tests always see graph-node's, so their expectations
/// do not depend on the environment they run in.
fn graph_node_defaults() -> GraphNodeDefaults {
    if cfg!(test) {
        GraphNodeDefaults::GRAPH_NODE
    } else {
        GraphNodeDefaults::from_env(|name| std::env::var(name).ok())
    }
}

/// Build a literal `order_by` object, expanding subgraph's `parent__child` nested
/// ordering syntax into Hasura's nested object form
/// (`pool__reserveUSD` -> `{pool: {reserveUSD: asc}}`).
fn literal_order_by(field: &str, direction: &str) -> String {
    let segments: Vec<&str> = field.split("__").filter(|s| !s.is_empty()).collect();
    let segments = if segments.is_empty() {
        vec![field]
    } else {
        segments
    };
    let mut out = direction.to_string();
    for seg in segments.iter().rev() {
        out = format!("{{{}: {}}}", seg, out);
    }
    out
}

/// JSON counterpart of `literal_order_by`, for values assembled at request time.
fn order_by_json(field: &str, direction: &str) -> Value {
    let segments: Vec<&str> = field.split("__").filter(|s| !s.is_empty()).collect();
    let segments = if segments.is_empty() {
        vec![field]
    } else {
        segments
    };
    let mut out = Value::String(normalize_order_direction(direction));
    for seg in segments.iter().rev() {
        let mut m = serde_json::Map::new();
        m.insert((*seg).to_string(), out);
        out = Value::Object(m);
    }
    out
}

/// graph-node orders rows that tie on the sort key by `id`, in the same direction as
/// the sort (`ORDER BY timestamp DESC, id DESC`). Hasura leaves ties in whatever order
/// Postgres produces them, so with `first`/`skip` a page boundary inside a run of equal
/// keys can repeat or skip rows, and the rows differ from what the subgraph returned.
/// Appending `id` makes the order total and identical.
///
/// Set `CONVERTER_ORDER_ID_TIEBREAK=off` to emit the sort key alone, e.g. if the extra
/// sort column proves slow on a very large table. Tests always run with it on; the
/// off form is covered through `literal_order_by_with_tiebreak`.
fn id_tiebreak_enabled() -> bool {
    cfg!(test) || tiebreak_setting(std::env::var("CONVERTER_ORDER_ID_TIEBREAK").ok().as_deref())
}

fn tiebreak_setting(value: Option<&str>) -> bool {
    !matches!(
        value.map(|v| v.trim().to_ascii_lowercase()).as_deref(),
        Some("0") | Some("false") | Some("off") | Some("no")
    )
}

/// The `order_by` value for a literal sort: the sort key, then `id` when `tiebreak`.
/// Sorting by `id` already is a total order, so nothing is added.
fn literal_order_by_with_tiebreak(field: &str, direction: &str, tiebreak: bool) -> String {
    let primary = literal_order_by(field, direction);
    if tiebreak && field != "id" {
        format!("[{}, {}]", primary, literal_order_by("id", direction))
    } else {
        primary
    }
}

/// JSON counterpart, for sorts assembled at request time. Always an array, which
/// is the shape the retyped `[Entity_order_by!]` variable expects.
fn order_by_json_with_tiebreak(field: &str, direction: &str, tiebreak: bool) -> Value {
    let mut items = vec![order_by_json(field, direction)];
    if tiebreak && field != "id" {
        items.push(order_by_json("id", direction));
    }
    Value::Array(items)
}

/// Hasura matches its `order_by` enum exactly, while subgraph clients sometimes
/// send `DESC`. Normalize the values we assemble ourselves; anything unrecognized
/// falls back to `asc` rather than failing the request.
fn normalize_order_direction(direction: &str) -> String {
    let d = direction.trim().trim_matches('"').to_lowercase();
    match d.as_str() {
        "asc" | "desc" | "asc_nulls_first" | "asc_nulls_last" | "desc_nulls_first"
        | "desc_nulls_last" => d,
        _ => "asc".to_string(),
    }
}

/// Decide how a root field's `orderBy`/`orderDirection` arguments become Hasura's
/// `order_by`, recording any header retypes, now-dead declarations and runtime
/// value rewrites the choice implies.
///
/// Returns the `order_by: ...` argument text, or `None` when the field has no
/// usable ordering. Previously any variable in either position caused the whole
/// argument to be dropped, which returned unordered rows with a 200 — silently
/// corrupting keyset pagination.
#[allow(clippy::too_many_arguments)]
fn plan_order_by(
    params: &HashMap<String, String>,
    entity_cap: &str,
    header_var_types: &HashMap<String, String>,
    retypes: &mut HashMap<String, String>,
    specs: &mut Vec<OrderBySpec>,
    dead_candidates: &mut Vec<String>,
    order_var_owner: &mut HashMap<String, String>,
    synth_counter: &mut usize,
) -> Option<String> {
    let order_field = params.get("orderBy").map(|s| s.trim());
    let order_dir = params.get("orderDirection").map(|s| s.trim());

    let dir_var = order_dir
        .filter(|d| d.starts_with('$'))
        .map(|d| d.trim_start_matches('$').to_string());

    let field = match order_field {
        // `orderDirection` with no `orderBy` orders nothing. Drop its declaration
        // too, since GraphQL rejects a variable that is declared but never used.
        None => {
            if let Some(dir) = dir_var {
                dead_candidates.push(dir);
            }
            return None;
        }
        Some(f) => f,
    };

    if !field.starts_with('$') {
        // Literal field. A variable is legal as an object *value*, so a variable
        // direction only needs its declared type corrected.
        let dir_text = order_dir.unwrap_or("asc");
        if let Some(dir) = dir_var {
            let non_null = header_var_types
                .get(&dir)
                .map(|t| t.trim().ends_with('!'))
                .unwrap_or(false);
            retypes.insert(
                dir,
                if non_null {
                    "order_by!".to_string()
                } else {
                    "order_by".to_string()
                },
            );
        }
        return Some(format!(
            "order_by: {}",
            literal_order_by_with_tiebreak(field, dir_text, id_tiebreak_enabled())
        ));
    }

    // The field itself is a variable, so the key of the `order_by` object is not
    // known until request time. Repurpose the client's own variable: retype it to
    // `[Entity_order_by!]` and rebuild its value. Repurposing rather than
    // synthesizing keeps the failure loud — if the value rewrite ever fails to
    // fire, Hasura sees a String where an input object is expected and errors,
    // instead of quietly returning unordered rows.
    let var = field.trim_start_matches('$').to_string();

    let direction_source = match (&dir_var, order_dir) {
        (Some(d), _) => OrderValueSource::Variable(d.clone()),
        (None, Some(lit)) => OrderValueSource::Literal(lit.to_string()),
        (None, None) => OrderValueSource::Literal("asc".to_string()),
    };

    // Two roots can share one `orderBy` variable while asking for different
    // directions (`a: reserves(orderBy: $o, orderDirection: $d1)` beside
    // `b: … $d2`). They cannot then share one rewritten variable, or the second
    // root silently inherits the first root's direction.
    let direction_conflict = order_var_owner.get(&var).is_some()
        && specs
            .iter()
            .any(|sp| sp.target_var == var && sp.direction_source != direction_source);

    let target_var = match order_var_owner.get(&var) {
        // One variable cannot be typed as two different entities' order_by, nor
        // carry two different directions.
        Some(prev) if prev != entity_cap || direction_conflict => {
            let mut name;
            loop {
                *synth_counter += 1;
                name = format!("order_by_{}", synth_counter);
                if !header_var_types.contains_key(&name) && !retypes.contains_key(&name) {
                    break;
                }
            }
            name
        }
        _ => {
            order_var_owner.insert(var.clone(), entity_cap.to_string());
            var.clone()
        }
    };

    retypes.insert(target_var.clone(), format!("[{}_order_by!]", entity_cap));
    if let Some(dir) = dir_var.clone() {
        dead_candidates.push(dir);
    }

    if !specs
        .iter()
        .any(|sp| sp.target_var == target_var && sp.direction_source == direction_source)
    {
        specs.push(OrderBySpec {
            target_var: target_var.clone(),
            field_source: OrderValueSource::Variable(var),
            direction_source,
            entity: entity_cap.to_string(),
        });
    }

    Some(format!("order_by: ${}", target_var))
}

/// Rewrite an operation header's variable definitions: replace the declared type of
/// any variable named in `retypes`, and remove any variable named in `removals`.
///
/// Rebuilds the definition list from kept spans rather than splicing in place, so a
/// removal cannot leave a dangling comma and a retype discards a default value that
/// would no longer typecheck. GraphQL treats commas as whitespace, so a declaration
/// runs from its `$` up to the next top-level `$`.
fn rewrite_variable_definitions(
    header: &str,
    retypes: &HashMap<String, String>,
    removals: &HashSet<String>,
) -> String {
    if retypes.is_empty() && removals.is_empty() {
        return header.to_string();
    }

    let chars: Vec<char> = header.chars().collect();
    let open = chars.iter().position(|&c| c == '(');
    let close = open.and_then(|o| match_paren(&chars, o));

    let (open, close) = match (open, close) {
        (Some(o), Some(c)) => (o, c),
        // No definition list yet. A synthesized order_by variable still needs
        // declaring, so build one; otherwise there is nothing to rewrite.
        _ => {
            let mut added: Vec<String> = retypes
                .iter()
                .map(|(name, ty)| format!("${}: {}", name, ty))
                .collect();
            if added.is_empty() {
                return header.to_string();
            }
            added.sort();
            return format!("{}({})", header.trim_end(), added.join(", "));
        }
    };

    let inner = &chars[open + 1..close];

    // Locate the start of each declaration.
    let mut starts = Vec::new();
    let mut depth = 0i32;
    let mut in_str = false;
    let mut i = 0;
    while i < inner.len() {
        let c = inner[i];
        if in_str {
            if c == '\\' {
                i += 2;
                continue;
            }
            if c == '"' {
                in_str = false;
            }
        } else {
            match c {
                '"' => in_str = true,
                '(' | '[' | '{' => depth += 1,
                ')' | ']' | '}' => depth -= 1,
                '$' if depth == 0 => starts.push(i),
                _ => {}
            }
        }
        i += 1;
    }

    let mut kept: Vec<String> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    for (n, &start) in starts.iter().enumerate() {
        let end = starts.get(n + 1).copied().unwrap_or(inner.len());
        let span: String = inner[start..end].iter().collect();
        let name: String = span[1..]
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();

        seen.insert(name.clone());

        if removals.contains(&name) {
            continue;
        }
        match retypes.get(&name) {
            Some(new_type) => kept.push(format!("${}: {}", name, new_type)),
            None => kept.push(span.trim().trim_end_matches(',').trim().to_string()),
        }
    }

    // A retype target the client never declared is one we synthesized (two root
    // fields ordering by the same variable against different entities), so it
    // needs a declaration of its own.
    let mut added: Vec<String> = retypes
        .iter()
        .filter(|(name, _)| !seen.contains(*name))
        .map(|(name, ty)| format!("${}: {}", name, ty))
        .collect();
    added.sort();
    kept.extend(added);

    let before: String = chars[..open].iter().collect();
    let after: String = chars[close + 1..].iter().collect();

    if kept.is_empty() {
        // `query Q()` is a syntax error, so drop the whole list.
        return format!("{}{}", before.trim_end(), after);
    }
    format!("{}({}){}", before, kept.join(", "), after)
}

/// Index of the `)` matching the `(` at `open`, skipping bracketed and quoted text.
fn match_paren(chars: &[char], open: usize) -> Option<usize> {
    let mut depth = 0i32;
    let mut in_str = false;
    let mut i = open;
    while i < chars.len() {
        let c = chars[i];
        if in_str {
            if c == '\\' {
                i += 2;
                continue;
            }
            if c == '"' {
                in_str = false;
            }
        } else {
            match c {
                '"' => in_str = true,
                // A `#` comment runs to the end of the line and may hold a `)`.
                '#' => {
                    while i < chars.len() && chars[i] != '\n' && chars[i] != '\r' {
                        i += 1;
                    }
                    continue;
                }
                '(' | '[' | '{' => depth += 1,
                ')' | ']' | '}' => {
                    depth -= 1;
                    if depth == 0 && c == ')' {
                        return Some(i);
                    }
                }
                _ => {}
            }
        }
        i += 1;
    }
    None
}

/// Every `$name` token referenced in the converted operation body.
///
/// The scan covers the whole body, root and nested arguments and directive
/// arguments alike, which makes it a complete answer to "is this declaration
/// still used?".
fn referenced_variables(body: &str) -> HashSet<String> {
    let mut out = HashSet::new();
    let chars: Vec<char> = body.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '$' {
            let name: String = chars[i + 1..]
                .iter()
                .take_while(|c| c.is_alphanumeric() || **c == '_')
                .collect();
            i += 1 + name.chars().count();
            if !name.is_empty() {
                out.insert(name);
            }
            continue;
        }
        i += 1;
    }
    out
}

/// Rewrite subgraph filter type names in a query header. Subgraph generates
/// `EntityName_filter` for the where-clause input type; Hyperindex/Hasura
/// generates `EntityName_bool_exp` for the equivalent argument. Anywhere a
/// variable is declared with a `*_filter` type (including list and non-null
/// forms), replace the suffix with `_bool_exp`.
///
/// Examples (input → output):
/// - `$where: Foo_filter`           → `$where: Foo_bool_exp`
/// - `$where: Foo_filter!`          → `$where: Foo_bool_exp!`
/// - `$where: [Foo_filter!]!`       → `$where: [Foo_bool_exp!]!`
fn rewrite_filter_type_names(header: &str) -> String {
    // Walk character-by-character looking for identifier tokens that end with
    // `_filter` and whose suffix character indicates a type position
    // (i.e. not the middle of a longer identifier).
    let bytes = header.as_bytes();
    let mut out = String::with_capacity(header.len());
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i] as char;
        // Identifier start: ASCII letter or underscore.
        let is_ident_start = c.is_ascii_alphabetic() || c == '_';
        if !is_ident_start {
            out.push(c);
            i += 1;
            continue;
        }
        // Walk the identifier.
        let start = i;
        while i < bytes.len() {
            let cc = bytes[i] as char;
            if cc.is_ascii_alphanumeric() || cc == '_' {
                i += 1;
            } else {
                break;
            }
        }
        let ident = &header[start..i];
        // Only rewrite identifiers shaped like `Something_filter` (PascalCase
        // entity name + `_filter`). This avoids accidentally rewriting words
        // like `query_filter_helper` or variable names that contain `_filter`.
        if let Some(prefix) = ident.strip_suffix("_filter") {
            let is_pascal_entity = prefix
                .chars()
                .next()
                .map(|first| first.is_ascii_uppercase())
                .unwrap_or(false)
                && !prefix.is_empty();
            if is_pascal_entity {
                out.push_str(prefix);
                out.push_str("_bool_exp");
                continue;
            }
        }
        out.push_str(ident);
    }
    out
}

struct RootField {
    /// Response key the client asked for (`recent` in `recent: swaps`).
    alias: Option<String>,
    /// The subgraph field name (`swaps`).
    name: String,
    params: HashMap<String, String>,
    /// The raw value of a top-level `block:` argument (`{ number_gte: 5 }`,
    /// `$block`), taken out of the argument text before it is parsed.
    ///
    /// Read from the raw text rather than from `params`, because
    /// `parse_single_param` hoists the contents of `where: {...}` into the same
    /// map unprefixed. By then a `block` key is ambiguous: it is equally the
    /// block argument and a filter on an entity that has a `block` field.
    block_argument: Option<String>,
    /// Directives on the field (`@include(if: $x)`), as written.
    directives: String,
    /// The selection set's contents, without the enclosing braces.
    selection: String,
}

fn extract_multiple_entities(query: &str) -> Result<Vec<RootField>, ConversionError> {
    let mut entities = Vec::new();
    let query_chars: Vec<char> = query.chars().collect();
    let mut current_pos = 0;


    // Skip opening brace if present
    while current_pos < query_chars.len() && query_chars[current_pos].is_whitespace() {
        current_pos += 1;
    }
    if current_pos < query_chars.len() && query_chars[current_pos] == '{' {
        current_pos += 1;
    }

    while current_pos < query_chars.len() {
        // Skip whitespace and newlines
        while current_pos < query_chars.len() && query_chars[current_pos].is_whitespace() {
            current_pos += 1;
        }

        if current_pos >= query_chars.len() {
            break;
        }


        // Look for entity name (word characters) - only at top level.
        // Identifier chars are alphanumerics plus underscore, so prefixed
        // entity names like `V2_Factory_Swaps` are kept as a single token
        // rather than being chopped into `V2` / `Factory` / `Swaps`.
        let entity_start = current_pos;
        while current_pos < query_chars.len()
            && (query_chars[current_pos].is_alphanumeric() || query_chars[current_pos] == '_')
        {
            current_pos += 1;
        }

        if current_pos == entity_start {
            current_pos += 1;
            continue;
        }

        let mut token_end = current_pos;
        let mut entity_name = query_chars[entity_start..current_pos]
            .iter()
            .collect::<String>();

        // `alias: field` - GraphQL lets any root field be renamed, and the
        // response must use the alias. This has to run before the validity
        // guard below, which would otherwise discard a short alias like `a`.
        let mut alias: Option<String> = None;
        {
            let mut probe = current_pos;
            while probe < query_chars.len() && query_chars[probe].is_whitespace() {
                probe += 1;
            }
            if probe < query_chars.len() && query_chars[probe] == ':' {
                probe += 1;
                while probe < query_chars.len() && query_chars[probe].is_whitespace() {
                    probe += 1;
                }
                let field_start = probe;
                while probe < query_chars.len()
                    && (query_chars[probe].is_alphanumeric() || query_chars[probe] == '_')
                {
                    probe += 1;
                }
                if probe > field_start {
                    alias = Some(entity_name.clone());
                    entity_name = query_chars[field_start..probe].iter().collect::<String>();
                    current_pos = probe;
                    token_end = probe;
                }
            }
        }

        // Skip if this is not a valid entity name (too short or common words)
        if entity_name.len() < 2
            || [
                "id", "in", "on", "to", "of", "at", "by", "is", "it", "as", "or", "an", "if", "up",
                "do", "go", "no", "so", "we", "he", "me", "be", "my", "am", "us", "hi", "lo", "ok",
                "hi", "lo", "ok",
            ]
            .contains(&entity_name.as_str())
        {
            current_pos += 1;
            continue;
        }

        // Look for opening parenthesis or brace after entity name (with optional whitespace)
        while current_pos < query_chars.len() && query_chars[current_pos].is_whitespace() {
            current_pos += 1;
        }

        let mut params = HashMap::new();
        let mut block_argument = None;

        if current_pos < query_chars.len() && query_chars[current_pos] == '(' {

            // Found an entity definition with parameters, extract parameters
            let params_start = current_pos + 1;
            let mut paren_count = 1; // We're already inside the first parenthesis

            // Walk to the matching `)`, ignoring parentheses inside string
            // literals and `#` comments. A `)` in either closed the argument
            // list early, and the rest of the query then parsed as stray text:
            // `streams(first: 1 # note )` collapsed to an empty operation,
            // which is a 200 with no rows rather than an error.
            let mut in_string = false;
            let mut escape = false;
            let mut in_comment = false;
            while current_pos < query_chars.len() {
                current_pos += 1;
                if current_pos >= query_chars.len() {
                    break;
                }
                let ch = query_chars[current_pos];

                if escape {
                    escape = false;
                    continue;
                }
                if in_string {
                    match ch {
                        '\\' => escape = true,
                        '"' => in_string = false,
                        _ => {}
                    }
                    continue;
                }
                if in_comment {
                    if ch == '\n' || ch == '\r' {
                        in_comment = false;
                    }
                    continue;
                }

                match ch {
                    '"' => in_string = true,
                    '#' => in_comment = true,
                    '(' => paren_count += 1,
                    ')' => {
                        paren_count -= 1;
                        if paren_count == 0 {
                            break;
                        }
                    }
                    _ => {}
                }
            }

            if current_pos >= query_chars.len() {
                break;
            }

            let mut params_str = strip_line_comments(
                &query_chars[params_start..current_pos].iter().collect::<String>(),
            );

            // Take `block:` from the raw argument text, before
            // `parse_graphql_params` flattens `block: {number: N}` into a
            // `block.number` key that is indistinguishable from a `where` filter
            // on a `block` field. Excise the text rather than dropping parsed
            // keys, which could take a real filter with it.
            let block = top_level_arguments(&params_str)
                .into_iter()
                .find(|arg| arg.name == "block")
                .map(|arg| (arg.value.trim().to_string(), arg.span));
            if let Some((value, span)) = block {
                params_str.replace_range(span, "");
                block_argument = Some(value);
            }

            parse_graphql_params(&params_str, &mut params)?;

            // Advance past the closing parenthesis
            current_pos += 1;
        } else if current_pos < query_chars.len()
            && (query_chars[current_pos] == '{' || query_chars[current_pos] == '@')
        {
            // Entity without parameters, continue to its directives or selection set
        } else {
            // Not an entity definition. Resume at the end of the token just
            // read: `current_pos` has been advanced past trailing whitespace and
            // is sitting inside the next identifier, so stepping on from here
            // would read that identifier from its second character.
            current_pos = token_end;
            if current_pos <= entity_start {
                current_pos = entity_start + 1;
            }
            continue;
        }

        // Directives (`@include(if: $x)`) come between the arguments and the
        // selection set. Hasura supports `@include` and `@skip`, so they are kept
        // as written; read as a field, `@include(if: $x)` became an `Include`
        // query filtered on `if`.
        let mut directives = String::new();
        loop {
            while current_pos < query_chars.len() && query_chars[current_pos].is_whitespace() {
                current_pos += 1;
            }
            if current_pos >= query_chars.len() || query_chars[current_pos] != '@' {
                break;
            }
            let start = current_pos;
            current_pos += 1;
            while current_pos < query_chars.len()
                && (query_chars[current_pos].is_alphanumeric() || query_chars[current_pos] == '_')
            {
                current_pos += 1;
            }
            let mut end = current_pos;
            while end < query_chars.len() && query_chars[end].is_whitespace() {
                end += 1;
            }
            if end < query_chars.len() && query_chars[end] == '(' {
                if let Some(close) = match_paren(&query_chars, end) {
                    current_pos = close + 1;
                }
            }
            if !directives.is_empty() {
                directives.push(' ');
            }
            directives.extend(&query_chars[start..current_pos]);
        }

        // Look for opening brace for selection set
        while current_pos < query_chars.len() && query_chars[current_pos].is_whitespace() {
            current_pos += 1;
        }


        if current_pos >= query_chars.len() || query_chars[current_pos] != '{' {
            // No selection set, skip this entity
            current_pos += 1;
            continue;
        }


        // Extract selection set
        let selection_start = current_pos + 1;
        let mut brace_count = 1; // We're already inside the first brace

        while current_pos < query_chars.len() {
            current_pos += 1;
            if current_pos >= query_chars.len() {
                break;
            }

            match query_chars[current_pos] {
                '{' => brace_count += 1,
                '}' => {
                    brace_count -= 1;
                    if brace_count == 0 {
                        break;
                    }
                }
                _ => {}
            }
        }

        if current_pos >= query_chars.len() {
            break;
        }

        let raw_selection: String = query_chars[selection_start..current_pos]
            .iter()
            .collect::<String>()
            .trim()
            .to_string();


        entities.push(RootField {
            alias,
            name: entity_name,
            params,
            block_argument,
            directives,
            selection: raw_selection,
        });
    }

    tracing::debug!(
        "Found {} root fields: {:?}",
        entities.len(),
        entities.iter().map(|root| &root.name).collect::<Vec<_>>()
    );
    Ok(entities)
}

/// The most recent field in a selection walk whose arguments or sub-selection
/// may still follow.
struct PendingField {
    name: String,
    /// The field as the schema knows it, when the enclosing type is known.
    info: Option<schema::FieldInfo>,
    /// Output position just past the field name, where default arguments go.
    insert_at: usize,
    had_args: bool,
}

impl PendingField {
    /// The entity the field's arguments filter and order.
    fn entity(&self) -> String {
        self.info
            .as_ref()
            .and_then(|info| info.nested_type_name.clone())
            .unwrap_or_else(|| singularize_and_capitalize(&self.name))
    }
}

/// Convert the arguments inside a selection set: a nested collection's `first`,
/// `skip`, `orderBy`/`orderDirection` and filters become Hasura's `limit`,
/// `offset`, `order_by` and `where`, exactly as on a root field, and directive
/// arguments (`@include(if: $x)`) are kept as written. Everything else is copied
/// through untouched.
///
/// `parent` is the type the selection belongs to. The walk tracks the type of
/// every nested selection through the schema, so a nested `where` is converted
/// against the right entity. Arguments on a field of unknown type are still
/// converted, against an entity named after the field.
fn convert_selection_arguments(
    selection: &str,
    parent: Option<&str>,
    ctx: &mut ArgContext,
) -> Result<String, ConversionError> {
    let chars: Vec<char> = selection.chars().collect();
    let mut out = String::with_capacity(selection.len());
    // Type of each open selection set, innermost last.
    let mut types: Vec<Option<String>> = vec![parent.map(str::to_string)];
    let mut field: Option<PendingField> = None;
    // Set by `...`; the next identifier is `on` or a fragment name.
    let mut after_spread = false;
    // The type an inline fragment's `{` opens, once `...` has been seen.
    let mut fragment_type: Option<Option<String>> = None;
    // The last identifier was a directive name, so a `(` holds its arguments.
    let mut after_directive = false;

    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];

        if c == '"' {
            let start = i;
            i += 1;
            while i < chars.len() && chars[i] != '"' {
                if chars[i] == '\\' {
                    i += 1;
                }
                i += 1;
            }
            let end = i.min(chars.len() - 1);
            out.extend(&chars[start..=end]);
            i = end + 1;
            continue;
        }

        // A comment is copied through as written: Hasura accepts it, and the
        // line break that ends it survives. Read as syntax, a `(` in it would
        // start an argument scan that ate the rest of the selection set.
        if c == '#' {
            while i < chars.len() && chars[i] != '\n' && chars[i] != '\r' {
                out.push(chars[i]);
                i += 1;
            }
            continue;
        }

        if c == '.' && chars[i..].starts_with(&['.', '.', '.']) {
            out.push_str("...");
            i += 3;
            after_spread = true;
            after_directive = false;
            field = None;
            // `... {` and `... @include(if: $x) {` stay on the enclosing type.
            fragment_type = Some(types.last().cloned().flatten());
            continue;
        }

        if c.is_alphabetic() || c == '_' || c == '@' {
            let start = i;
            i += 1;
            while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
                i += 1;
            }
            let word: String = chars[start..i].iter().collect();
            out.push_str(&word);

            if c == '@' {
                // `... @include(if: $x) {`: an untyped inline fragment, so the
                // next identifier is a field, not a fragment name.
                after_spread = false;
                after_directive = true;
                continue;
            }
            after_directive = false;

            if after_spread {
                after_spread = false;
                if word == "on" {
                    while i < chars.len() && chars[i].is_whitespace() {
                        out.push(chars[i]);
                        i += 1;
                    }
                    let type_start = i;
                    while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
                        i += 1;
                    }
                    let type_name: String = chars[type_start..i].iter().collect();
                    out.push_str(&type_name);
                    fragment_type = Some(Some(type_name));
                } else {
                    // `...Name` spreads a named fragment; there is no `{` to type.
                    fragment_type = None;
                }
                continue;
            }

            // An alias label: the field it names comes after the colon.
            let mut j = i;
            while j < chars.len() && chars[j].is_whitespace() {
                j += 1;
            }
            if j < chars.len() && chars[j] == ':' {
                out.extend(&chars[i..=j]);
                i = j + 1;
                continue;
            }

            let info = types
                .last()
                .cloned()
                .flatten()
                .and_then(|ty| schema::get_field_info(&ty, &word));
            field = Some(PendingField {
                name: word,
                info,
                insert_at: out.len(),
                had_args: false,
            });
            fragment_type = None;
            continue;
        }

        if c == '(' {
            let close = match_paren(&chars, i).unwrap_or(chars.len() - 1);
            if after_directive {
                out.extend(&chars[i..=close]);
                after_directive = false;
            } else if let Some(pending) = field.as_mut() {
                // Arguments are re-emitted on one line, so a comment kept here
                // would comment out everything after it, the closing paren too.
                let inner = strip_line_comments(&chars[i + 1..close].iter().collect::<String>());
                check_nested_arguments(&pending.name, &inner)?;
                let mut params = HashMap::new();
                parse_graphql_params(&inner, &mut params)?;
                let args = convert_collection_args(&params, &pending.entity(), None, ctx)?;
                if !args.is_empty() {
                    out.push_str(&format!("({})", args.join(", ")));
                }
                pending.had_args = true;
            }
            i = close + 1;
            continue;
        }

        if c == '{' {
            let opened = if let Some(ty) = fragment_type.take() {
                ty
            } else if let Some(pending) = field.take() {
                let is_list = pending.info.as_ref().map(|info| info.is_list).unwrap_or(false);
                if is_list && !pending.had_args {
                    // A list with no arguments still gets graph-node's defaults.
                    let args =
                        convert_collection_args(&HashMap::new(), &pending.entity(), None, ctx)?;
                    if !args.is_empty() {
                        out.insert_str(pending.insert_at, &format!("({})", args.join(", ")));
                    }
                }
                pending.info.and_then(|info| info.nested_type_name)
            } else {
                None
            };
            types.push(opened);
            out.push('{');
            after_spread = false;
            after_directive = false;
            i += 1;
            continue;
        }

        if c == '}' {
            types.pop();
            field = None;
            fragment_type = None;
            after_directive = false;
        }
        out.push(c);
        i += 1;
    }

    Ok(out)
}

/// graph-node takes exactly `first`, `skip`, `orderBy`, `orderDirection` and
/// `where` on a nested list. Anything else (a nested `block:`, a bare filter
/// key) is a mistake or a constraint we cannot honour, and an argument list that
/// reads as nothing is not an empty one; either way, converting it would answer
/// a question that was not asked.
fn check_nested_arguments(field: &str, inner: &str) -> Result<(), ConversionError> {
    let args = top_level_arguments(inner);
    if args.is_empty() && !inner.trim().is_empty() {
        return Err(ConversionError::UnsupportedNestedArgument(
            field.to_string(),
            "unparseable arguments".to_string(),
        ));
    }
    let unsupported: Vec<&str> = args
        .iter()
        .map(|arg| arg.name)
        .filter(|name| !matches!(*name, "first" | "skip" | "orderBy" | "orderDirection" | "where"))
        .collect();
    if !unsupported.is_empty() {
        return Err(ConversionError::UnsupportedNestedArgument(
            field.to_string(),
            unsupported.join(", "),
        ));
    }
    Ok(())
}

/// `convert_selection_arguments` for a named fragment definition, whose type
/// condition (`fragment F on Swap`) is the selection's type.
fn convert_fragment_arguments(
    fragment_text: &str,
    ctx: &mut ArgContext,
) -> Result<String, ConversionError> {
    let chars: Vec<char> = fragment_text.chars().collect();
    let Some(open_idx) = chars.iter().position(|c| *c == '{') else {
        return Ok(fragment_text.to_string());
    };
    // Find matching closing brace
    let mut brace_count = 1i32;
    let mut pos = open_idx + 1;
    while pos < chars.len() {
        match chars[pos] {
            '{' => brace_count += 1,
            '}' => {
                brace_count -= 1;
                if brace_count == 0 {
                    break;
                }
            }
            _ => {}
        }
        pos += 1;
    }
    if pos >= chars.len() {
        return Ok(fragment_text.to_string());
    }
    let header: String = chars[..open_idx + 1].iter().collect();
    let body: String = chars[open_idx + 1..pos].iter().collect();
    let tail: String = chars[pos..].iter().collect();
    let type_condition = header
        .split_whitespace()
        .skip_while(|word| *word != "on")
        .nth(1)
        .map(|ty| ty.trim_end_matches('{').to_string());
    let converted = convert_selection_arguments(body.trim(), type_condition.as_deref(), ctx)?;
    Ok(format!("{}{}{}", header, converted, tail))
}

// Removed unused selection set helpers

/// Which parts of a `_meta` selection the caller asked for, and where its
/// answer goes in the response.
///
/// `block { number timestamp }` maps onto Hyperindex's indexing progress
/// (`_meta.progressBlock` / `progressBlockTime`, or `chain_metadata` on indexers
/// without Hasura's `_meta`), and `__typename` is answered locally.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MetaSelection {
    pub block_number: bool,
    pub block_timestamp: bool,
    pub typename: bool,
    pub block_typename: bool,
    pub deployment: bool,
    /// Response key: the client's alias, or `_meta`.
    pub key: String,
}

/// The deployment hash of the configured Hyperindex endpoint, which stands in
/// for graph-node's deployment id in `_meta.deployment` and in errors.
pub fn deployment_id() -> Option<String> {
    if cfg!(test) {
        return Some("b232c6d".to_string());
    }
    std::env::var("HYPERINDEX_URL")
        .ok()
        .and_then(|url| deployment_id_from_url(&url))
}

/// The deployment hash in a Hyperindex GraphQL URL: the path segment before
/// `/v1/graphql` (`https://indexer.hyperindex.xyz/b232c6d/v1/graphql`), or the
/// subdomain of a dedicated host (`https://0xostium-7cfbfa6.dedicated.hyperindex.xyz/v1/graphql`).
pub fn deployment_id_from_url(url: &str) -> Option<String> {
    let rest = url.split_once("://")?.1;
    let (host, path) = rest.split_once('/').unwrap_or((rest, ""));
    let host = host.split(':').next().unwrap_or(host);
    match path.trim_end_matches('/').split('/').collect::<Vec<_>>().as_slice() {
        [hash, "v1", "graphql"] if !hash.is_empty() => Some(hash.to_string()),
        ["v1", "graphql"] => host
            .strip_suffix(".dedicated.hyperindex.xyz")
            .filter(|label| !label.is_empty() && !label.contains('.'))
            .map(str::to_string),
        _ => None,
    }
}

/// Read a `_meta` selection, refusing it whole if any field cannot be served.
///
/// Serving the rest would hand back an object that looks complete but silently
/// omits fields, and a missing `hasIndexingErrors` reads as "no indexing errors".
/// `block.timestamp` needs Hasura's native `_meta` (`native`); `chain_metadata`
/// has no block time.
fn convert_meta_selection(
    selection: &str,
    native: bool,
    deployment_known: bool,
    key: String,
) -> Result<MetaSelection, ConversionError> {
    let requested = selection_leaf_paths(selection);
    if requested.is_empty() {
        return Err(ConversionError::InvalidQueryFormat);
    }

    let servable = |path: &str| match path {
        "block.number" | "__typename" | "block.__typename" => true,
        "block.timestamp" => native,
        "deployment" => deployment_known,
        _ => false,
    };
    let unsupported: Vec<String> = requested
        .iter()
        .filter(|path| !servable(path))
        .cloned()
        .collect();
    if !unsupported.is_empty() {
        return Err(ConversionError::ComplexMetaQuery(unsupported.join(", ")));
    }

    Ok(MetaSelection {
        block_number: requested.iter().any(|p| p == "block.number"),
        block_timestamp: requested.iter().any(|p| p == "block.timestamp"),
        typename: requested.iter().any(|p| p == "__typename"),
        block_typename: requested.iter().any(|p| p == "block.__typename"),
        deployment: requested.iter().any(|p| p == "deployment"),
        key,
    })
}

/// Dotted leaf paths of a selection set: `block { number } deployment` yields
/// `["block.number", "deployment"]`.
fn selection_leaf_paths(selection: &str) -> Vec<String> {
    let chars: Vec<char> = selection.chars().collect();
    let mut paths = Vec::new();
    let mut stack: Vec<String> = Vec::new();
    let mut pending: Option<String> = None;

    let join = |stack: &[String], leaf: &str| -> String {
        if stack.is_empty() {
            leaf.to_string()
        } else {
            format!("{}.{}", stack.join("."), leaf)
        }
    };

    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c.is_alphabetic() || c == '_' {
            let name: String = chars[i..]
                .iter()
                .take_while(|c| c.is_alphanumeric() || **c == '_')
                .collect();
            i += name.chars().count();
            // The previous identifier had no sub-selection, so it was a leaf.
            if let Some(prev) = pending.replace(name) {
                paths.push(join(&stack, &prev));
            }
            continue;
        }
        match c {
            '{' => {
                if let Some(parent) = pending.take() {
                    stack.push(parent);
                }
                i += 1;
            }
            '}' => {
                if let Some(prev) = pending.take() {
                    paths.push(join(&stack, &prev));
                }
                stack.pop();
                i += 1;
            }
            _ => i += 1,
        }
    }
    if let Some(prev) = pending.take() {
        paths.push(join(&stack, &prev));
    }
    paths
}

fn flatten_where_map(mut map: HashMap<String, String>) -> HashMap<String, String> {
    let mut flat = HashMap::new();
    for (k, v) in map.drain() {
        if k == "where" {
            // Recursively parse and flatten
            if let Ok(nested) = parse_nested_where_clause(&v) {
                for (nk, nv) in flatten_where_map(nested) {
                    flat.insert(nk, nv);
                }
            }
        } else {
            flat.insert(k, v);
        }
    }
    flat
}

fn process_nested_filters_recursive(
    parent: &str,
    child_filters: HashMap<String, String>,
    parent_entity_name: &str,
) -> Result<String, ConversionError> {
    let mut child_conditions = Vec::new();
    let mut child_and_conditions = Vec::new();

    // Subgraph's strict nested-entity filter carries a trailing underscore
    // (`launch_: { quoteAsset: "USDC" }`). Strip it for both schema lookup and
    // emission, matching what `translate_leaf_filter` already does on the
    // JSON-variable path.
    let parent = parent
        .strip_suffix('_')
        .filter(|p| !p.is_empty())
        .unwrap_or(parent);

    // Check if parent itself is a nested path (e.g., "pair.token")
    // If so, recursively process the first part with the rest as a nested filter
    if parent.contains('.') {
        if let Some(dot_idx) = parent.find('.') {
            let first_part = &parent[..dot_idx];
            let first_part = first_part
                .strip_suffix('_')
                .filter(|p| !p.is_empty())
                .unwrap_or(first_part);
            let rest = &parent[dot_idx + 1..];

            // Get the nested entity type for first_part
            let nested_entity_type = schema::get_field_info(parent_entity_name, first_part)
                .and_then(|info| info.nested_type_name)
                .unwrap_or_else(|| first_part.to_string());

            // Process "rest" with child_filters to get the nested condition for "rest"
            // This returns something like "token: {amount: {_eq: "0"}}"
            let rest_condition =
                process_nested_filters_recursive(rest, child_filters, &nested_entity_type)?;

            // Extract the inner condition part (the part after "rest: ")
            // rest_condition is "rest: {...}", we want just "{...}"
            let inner_condition = if let Some(colon_idx) = rest_condition.find(':') {
                rest_condition[colon_idx + 1..].trim().to_string()
            } else {
                format!("{{{}}}", rest_condition)
            };

            // Now wrap this under first_part: first_part: {rest: {inner_condition}}
            // The inner_condition already has the braces, so we just need to wrap it
            return Ok(format!("{}: {{{}: {}}}", first_part, rest, inner_condition));
        }
    }

    // Base case: parent is a simple field name (e.g., "pair")
    // Get the nested entity type for this parent field
    let nested_entity_type = schema::get_field_info(parent_entity_name, parent)
        .and_then(|info| info.nested_type_name)
        .unwrap_or_else(|| parent.to_string());

    // Group child filters by field name to handle duplicates, in a fixed order.
    let mut child_filters: Vec<(String, String)> = child_filters.into_iter().collect();
    child_filters.sort();
    let mut grouped_child_filters: std::collections::BTreeMap<String, Vec<(String, String)>> =
        std::collections::BTreeMap::new();
    for (child_key, child_value) in child_filters {
        let field_name = if child_key.contains('_') {
            if let Some(underscore_idx) = child_key.find('_') {
                &child_key[..underscore_idx]
            } else {
                &child_key
            }
        } else {
            &child_key
        };

        grouped_child_filters
            .entry(field_name.to_string())
            .or_insert_with(Vec::new)
            .push((child_key, child_value));
    }

    for (_field_name, conditions) in grouped_child_filters {
        if conditions.len() == 1 {
            // Single condition for this field
            let (k, v) = &conditions[0];
            // Use schema to determine if child fields are nested entities
            let condition = convert_basic_filter_to_hasura_condition(&k, &v, &nested_entity_type)?;
            child_conditions.push(condition);
        } else {
            // Multiple conditions for the same field - wrap in _and
            for (k, v) in conditions {
                // Use schema to determine if child fields are nested entities
                let condition =
                    convert_basic_filter_to_hasura_condition(&k, &v, &nested_entity_type)?;
                child_and_conditions.push(format!("{{{}}}", condition));
            }
        }
    }

    let mut child_conditions = move_repeated_keys(child_conditions, &mut child_and_conditions);
    if !child_and_conditions.is_empty() {
        child_conditions.push(format!("_and: [{}]", child_and_conditions.join(", ")));
    }

    Ok(format!("{}: {{{}}}", parent, child_conditions.join(", ")))
}

/// Result of filter conversion, including the where clause and variable type overrides
struct FilterConversionResult {
    where_clause: String,
    /// Maps variable names to their expected types (for enum fields)
    /// e.g., "operation" -> "orderaction"
    variable_type_overrides: HashMap<String, String>,
}

fn convert_filters_to_where_clause(
    params: &HashMap<String, String>,
    entity_name: &str,
) -> Result<FilterConversionResult, ConversionError> {
    // Recursively flatten the entire params map
    let mut flat_filters = flatten_where_map(params.clone());

    // Remove pagination/order keys
    flat_filters.remove("first");
    flat_filters.remove("skip");
    flat_filters.remove("orderBy");
    flat_filters.remove("orderDirection");
    flat_filters.remove("where");
    // `where: {first: 2}` filters on a field named `first`.
    let mut flat_filters: HashMap<String, String> = flat_filters
        .into_iter()
        .map(|(key, value)| match key.strip_prefix(WHERE_FIELD_MARKER) {
            Some(field) => (field.to_string(), value),
            None => (key, value),
        })
        .collect();

    // Inline `and: [{...}, {...}]` / `or: [...]`. Each item is a filter of its own.
    let logical_keys: Vec<String> = flat_filters
        .iter()
        .filter(|(key, value)| is_logical_list(key, value))
        .map(|(key, _)| key.clone())
        .collect();
    let mut logical: Vec<(String, String)> = logical_keys
        .into_iter()
        .filter_map(|key| flat_filters.remove(&key).map(|value| (key, value)))
        .collect();
    logical.sort();

    // Group filters by parent object to avoid duplicates
    let mut grouped_filters: HashMap<String, HashMap<String, String>> = HashMap::new();
    let mut basic_filters: HashMap<String, Vec<(String, String)>> = HashMap::new();

    // Sorted, so conditions on one field come out in the same order every time.
    let mut flat_filters: Vec<(String, String)> = flat_filters.into_iter().collect();
    flat_filters.sort();
    for (key, value) in flat_filters {
        if key.contains('.') {
            // This is a nested filter (e.g., "user.name_starts_with")
            if let Some(dot_idx) = key.rfind('.') {
                let parent = &key[..dot_idx];
                // `launch_: {...}` and `launch: {...}` name the same relation.
                let parent = parent
                    .strip_suffix('_')
                    .filter(|p| !p.is_empty())
                    .unwrap_or(parent);
                let child_key = &key[dot_idx + 1..];

                grouped_filters
                    .entry(parent.to_string())
                    .or_insert_with(HashMap::new)
                    .insert(child_key.to_string(), value);
            }
        } else {
            // This is a basic filter - group by field name
            let field_name = if key.contains('_') {
                // Extract the base field name (e.g., "alias" from "alias_contains")
                if let Some(underscore_idx) = key.find('_') {
                    &key[..underscore_idx]
                } else {
                    &key
                }
            } else {
                &key
            };

            basic_filters
                .entry(field_name.to_string())
                .or_insert_with(Vec::new)
                .push((key, value));
        }
    }

    // Sort keys to ensure consistent order, with chainId first
    let mut sorted_keys: Vec<_> = basic_filters.keys().collect();
    sorted_keys.sort_by(|a, b| {
        if *a == "chainId" {
            std::cmp::Ordering::Less
        } else if *b == "chainId" {
            std::cmp::Ordering::Greater
        } else {
            a.cmp(b)
        }
    });

    let mut where_conditions = Vec::new();
    let mut variable_type_overrides: HashMap<String, String> = HashMap::new();

    // Add basic filters
    let mut and_conditions = Vec::new();
    for key in sorted_keys {
        let conditions = basic_filters.get(key).unwrap();
        if conditions.len() == 1 {
            // Single condition for this field
            let (k, v) = &conditions[0];
            let (condition, var_override) =
                convert_basic_filter_to_hasura_condition_with_type(&k, &v, entity_name)?;
            where_conditions.push(condition);
            if let Some((var_name, var_type)) = var_override {
                variable_type_overrides.insert(var_name, var_type);
            }
        } else {
            // Multiple conditions for the same field - wrap in _and
            for (k, v) in conditions {
                let (condition, var_override) =
                    convert_basic_filter_to_hasura_condition_with_type(&k, &v, entity_name)?;
                and_conditions.push(format!("{{{}}}", condition));
                if let Some((var_name, var_type)) = var_override {
                    variable_type_overrides.insert(var_name, var_type);
                }
            }
        }
    }
    // `and` items join the field-level `_and`, since an object cannot carry two
    // `_and` keys; `or` items become one `_or`.
    for (key, value) in logical {
        let (items, overrides) = convert_logical_items(&value, entity_name)?;
        variable_type_overrides.extend(overrides);
        if key.trim_start_matches('_') == "and" {
            and_conditions.extend(items);
        } else {
            where_conditions.push(format!("_or: [{}]", items.join(", ")));
        }
    }

    // Add grouped nested filters (recursively handle arbitrary depth)
    //
    // A relation can be constrained both by id (`launch_not_in: $ids`, emitted
    // above as a basic filter) and by its own fields (`launch_: {quoteAsset: $q}`).
    // Both render as `launch: {...}`, and a GraphQL object cannot carry the same
    // key twice, so the nested half moves into `_and` — same meaning, valid syntax.
    let mut sorted_parents: Vec<_> = grouped_filters.keys().cloned().collect();
    sorted_parents.sort();
    for parent in sorted_parents {
        let child_filters = grouped_filters.remove(&parent).unwrap();
        let nested_condition =
            process_nested_filters_recursive(&parent, child_filters, entity_name)?;
        if basic_filters.contains_key(&parent) {
            and_conditions.push(format!("{{{}}}", nested_condition));
        } else {
            where_conditions.push(nested_condition);
        }
    }

    let mut where_conditions = move_repeated_keys(where_conditions, &mut and_conditions);
    if !and_conditions.is_empty() {
        where_conditions.push(format!("_and: [{}]", and_conditions.join(", ")));
    }

    if where_conditions.is_empty() {
        return Ok(FilterConversionResult {
            where_clause: String::new(),
            variable_type_overrides,
        });
    }

    Ok(FilterConversionResult {
        where_clause: format!("where: {{{}}}", where_conditions.join(", ")),
        variable_type_overrides,
    })
}

/// Keep the first condition for each key and move any repeat into `_and`. A
/// GraphQL object cannot carry a key twice, and two `_not_*` filters on
/// different fields both render as `_not: {...}`.
fn move_repeated_keys(conditions: Vec<String>, and_conditions: &mut Vec<String>) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut kept = Vec::new();
    for condition in conditions {
        let key = condition.split(':').next().unwrap_or_default().trim().to_string();
        if seen.insert(key) {
            kept.push(condition);
        } else {
            and_conditions.push(format!("{{{}}}", condition));
        }
    }
    kept
}

/// Whether `key: value` is an inline logical filter: `and`/`or` (or Hasura's
/// `_and`/`_or`) over a list literal.
fn is_logical_list(key: &str, value: &str) -> bool {
    matches!(key, "and" | "or" | "_and" | "_or") && value.trim_start().starts_with('[')
}

/// Convert each item of an inline `and`/`or` list into a Hasura condition object,
/// with the same rules as a top-level `where`. An item that is not an object
/// literal (a `$variable`) is passed through as written. Returns the items and
/// any variable type overrides they imply.
fn convert_logical_items(
    list: &str,
    entity_name: &str,
) -> Result<(Vec<String>, HashMap<String, String>), ConversionError> {
    let mut items = Vec::new();
    let mut overrides = HashMap::new();
    for item in split_list_items(list) {
        let item = item.trim();
        let Some(content) = item.strip_prefix('{').and_then(|i| i.strip_suffix('}')) else {
            items.push(item.to_string());
            continue;
        };
        let mut params = HashMap::new();
        parse_graphql_params(content, &mut params)?;
        let converted = convert_filters_to_where_clause(&params, entity_name)?;
        overrides.extend(converted.variable_type_overrides);
        let condition = converted
            .where_clause
            .strip_prefix("where: ")
            .unwrap_or("{}")
            .to_string();
        items.push(if condition.is_empty() {
            "{}".to_string()
        } else {
            condition
        });
    }
    Ok((items, overrides))
}

/// The items of a list literal (`[{a: 1}, {b: 2}]`), split at its top-level
/// commas; brackets, braces and strings inside an item are respected.
fn split_list_items(list: &str) -> Vec<String> {
    let inner = list
        .trim()
        .strip_prefix('[')
        .and_then(|l| l.strip_suffix(']'))
        .unwrap_or(list);
    let mut items = Vec::new();
    let mut current = String::new();
    let mut depth = 0i32;
    let mut in_str = false;
    let mut escape = false;
    for c in inner.chars() {
        if in_str {
            current.push(c);
            if escape {
                escape = false;
            } else if c == '\\' {
                escape = true;
            } else if c == '"' {
                in_str = false;
            }
            continue;
        }
        match c {
            '"' => {
                in_str = true;
                current.push(c);
            }
            '{' | '[' | '(' => {
                // Commas are optional in GraphQL: `[{a: 1} {b: 2}]`.
                if depth == 0 && !current.trim().is_empty() {
                    items.push(current.trim().to_string());
                    current.clear();
                }
                depth += 1;
                current.push(c);
            }
            '}' | ']' | ')' => {
                depth -= 1;
                current.push(c);
            }
            ',' if depth == 0 => {
                if !current.trim().is_empty() {
                    items.push(current.trim().to_string());
                }
                current.clear();
            }
            _ => current.push(c),
        }
    }
    if !current.trim().is_empty() {
        items.push(current.trim().to_string());
    }
    items
}

fn parse_nested_where_clause(
    where_value: &str,
) -> Result<HashMap<String, String>, ConversionError> {
    let mut nested_params = HashMap::new();

    // Remove outer braces if present
    let content = where_value
        .trim()
        .trim_start_matches('{')
        .trim_end_matches('}');

    // Parse the nested where clause using the same logic as parse_graphql_params
    parse_graphql_params(content, &mut nested_params)?;
    Ok(nested_params)
}

fn convert_basic_filter_to_hasura_condition(
    key: &str,
    value: &str,
    entity_name: &str,
) -> Result<String, ConversionError> {
    if key == "where" {
        // Should never emit a 'where' key at this stage
        return Ok(String::new());
    }

    // `and`/`or` inside a relation filter (`pair_: {or: [...]}`).
    if is_logical_list(key, value) {
        let (items, _) = convert_logical_items(value, entity_name)?;
        let op = if key.trim_start_matches('_') == "and" {
            "_and"
        } else {
            "_or"
        };
        return Ok(format!("{}: [{}]", op, items.join(", ")));
    }

    // Handle different filter patterns - check longer suffixes first
    if let Some((field, filter)) = string_match_filter(key) {
        let value = value.trim();
        // A pattern cannot be spelled around a variable in the query text, so
        // `$q` is swapped for a variable holding the finished pattern, filled in
        // from `$q` at request time (see `like_variables`).
        let pattern = match value.strip_prefix('$') {
            Some(var) => format!("${}", like_variable_name(var, filter.shape)),
            None => format!("\"{}\"", like_pattern(value.trim_matches('"'), filter.shape)),
        };
        let condition = format!("{}: {{{}: {}}}", field, filter.operator, pattern);
        return Ok(if filter.negated {
            format!("_not: {{{}}}", condition)
        } else {
            condition
        });
    }

    if key.ends_with("_not_in") {
        let field = &key[..key.len() - 7];
        return Ok(nested_aware_condition(entity_name, field, "_nin", value));
    }

    if key.ends_with("_gte") {
        let field = &key[..key.len() - 4];
        return Ok(nested_aware_condition(entity_name, field, "_gte", value));
    }

    if key.ends_with("_lte") {
        let field = &key[..key.len() - 4];
        return Ok(nested_aware_condition(entity_name, field, "_lte", value));
    }

    if key.ends_with("_not") {
        let field = &key[..key.len() - 4];
        return Ok(nested_aware_condition(entity_name, field, "_neq", value));
    }

    if key.ends_with("_gt") {
        let field = &key[..key.len() - 3];
        return Ok(nested_aware_condition(entity_name, field, "_gt", value));
    }

    if key.ends_with("_lt") {
        let field = &key[..key.len() - 3];
        return Ok(nested_aware_condition(entity_name, field, "_lt", value));
    }

    if key.ends_with("_in") {
        let field = &key[..key.len() - 3];
        return Ok(nested_aware_condition(entity_name, field, "_in", value));
    }

    // Handle unsupported filters
    if key.ends_with("_containsAny") || key.ends_with("_containsAll") {
        return Err(ConversionError::UnsupportedFilter(key.to_string()));
    }

    // Check if this is a nested entity reference
    // A nested entity reference is when:
    // 1. The field is a nested entity according to the schema
    // 2. The value is a simple scalar (string/number, not an object/array)
    // 3. The field doesn't have an operator suffix (already handled above)
    // 4. The field is not a system field like "chainId" (added programmatically)

    // Special case: chainId is always a primitive field, never a nested entity
    if key == "chainId" {
        // chainId is always a primitive, use default equality filter
        let result = format!("{}: {{_eq: {}}}", key, value);
        return Ok(result);
    }

    // Check if value is a simple scalar (not an object/array)
    let trimmed_value = value.trim();
    let is_variable = trimmed_value.trim_start().starts_with('$');
    let is_simple_scalar =
        !trimmed_value.starts_with('{') && !trimmed_value.starts_with('[') && !is_variable;

    // Use schema to check if this field is a nested entity
    let is_nested_entity = schema::is_nested_entity(entity_name, key);

    if is_nested_entity && (is_simple_scalar || is_variable) {
        // This is a nested entity reference with a scalar value (literal or variable)
        // In subgraph: pair: "0" or pair: $pairid means "where pair id equals value"
        // In Envio/Hyperindex: this becomes pair: {id: {_eq: "0"}} or pair: {id: {_eq: $pairid}}
        return Ok(format!("{}: {{id: {{_eq: {}}}}}", key, value));
    }

    // Default case: treat as equality filter (for regular fields, with or without variables)
    let result = format!("{}: {{_eq: {}}}", key, value);
    Ok(result)
}

// ─────────────────────────────────────────────────────────────────────────────
// JSON-shaped filter translator
//
// Mirrors `convert_basic_filter_to_hasura_condition` for the case where a
// caller passes the entire `where` clause as a GraphQL variable, e.g.
//   query ($where: UniswapDayData_filter) { uniswapDayDatas(where: $where) ... }
// In that situation the converter cannot rewrite the operator suffixes in the
// query string itself (the value never appears literally in the query); it has
// to rewrite the JSON value at request time, which is what these helpers do.
//
// Behaviour parity with the inline path is enforced by tests in
// `tests::filter_value_translation` (every operator covered by the inline
// tests has a matching JSON test).
// ─────────────────────────────────────────────────────────────────────────────

/// Translate a subgraph-shaped filter value into the Hasura `bool_exp`-shaped
/// value expected by Hyperindex. Non-object inputs (`null`, scalars, arrays)
/// are returned unchanged.
///
/// `entity_name` is the Hyperindex entity the filter applies to (e.g. `"Trade"`).
/// It is used to detect nested-entity references via the schema cache.
pub fn translate_subgraph_filter_value(value: &Value, entity_name: &str) -> Value {
    let obj = match value {
        Value::Object(m) => m,
        _ => return value.clone(),
    };

    let mut out = serde_json::Map::new();
    for (key, val) in obj {
        match key.as_str() {
            "and" | "_and" => {
                for item in translate_filter_array(val, entity_name) {
                    push_and(&mut out, item);
                }
            }
            "or" | "_or" => {
                let arr = translate_filter_array(val, entity_name);
                insert_condition(&mut out, "_or", Value::Array(arr));
            }
            "_not" | "not" => {
                let inner = translate_subgraph_filter_value(val, entity_name);
                insert_condition(&mut out, "_not", inner);
            }
            _ => translate_leaf_filter(key, val, entity_name, &mut out),
        }
    }
    Value::Object(out)
}

/// Add `key: condition` to a translated filter object. JSON keys are unique, so
/// when the key is already taken (`amount_gt` and `amount_lt` both become
/// `amount`, two `_not_*` filters both become `_not`) the condition joins `_and`
/// rather than overwriting the first one.
fn insert_condition(out: &mut serde_json::Map<String, Value>, key: &str, condition: Value) {
    if out.contains_key(key) {
        let mut item = serde_json::Map::new();
        item.insert(key.to_string(), condition);
        push_and(out, Value::Object(item));
    } else {
        out.insert(key.to_string(), condition);
    }
}

fn push_and(out: &mut serde_json::Map<String, Value>, item: Value) {
    match out
        .entry("_and")
        .or_insert_with(|| Value::Array(Vec::new()))
    {
        Value::Array(items) => items.push(item),
        other => {
            let first = other.take();
            *other = Value::Array(vec![first, item]);
        }
    }
}

fn translate_filter_array(val: &Value, entity_name: &str) -> Vec<Value> {
    val.as_array()
        .map(|a| {
            a.iter()
                .map(|v| translate_subgraph_filter_value(v, entity_name))
                .collect()
        })
        .unwrap_or_default()
}

/// Translate a single non-connective filter entry, writing into `out`.
fn translate_leaf_filter(
    key: &str,
    value: &Value,
    entity_name: &str,
    out: &mut serde_json::Map<String, Value>,
) {
    // ORDER MATTERS — longest suffixes must be checked first, mirroring the
    // ordering in `convert_basic_filter_to_hasura_condition`. Changes here
    // must be applied to both functions.

    if let Some((field, filter)) = string_match_filter(key) {
        let condition = like_value(value, filter);
        if filter.negated {
            // Each negation is its own `_not`: `_not: {a, b}` would mean
            // "not (a and b)", where graph-node means "not a and not b".
            let mut negated = serde_json::Map::new();
            negated.insert(field.to_string(), condition);
            insert_condition(out, "_not", Value::Object(negated));
        } else {
            insert_condition(out, field, condition);
        }
        return;
    }
    if let Some(field) = key.strip_suffix("_not_in") {
        insert_condition(
            out,
            field,
            nested_aware_value(entity_name, field, "_nin", value),
        );
        return;
    }
    if let Some(field) = key.strip_suffix("_gte") {
        insert_condition(
            out,
            field,
            nested_aware_value(entity_name, field, "_gte", value),
        );
        return;
    }
    if let Some(field) = key.strip_suffix("_lte") {
        insert_condition(
            out,
            field,
            nested_aware_value(entity_name, field, "_lte", value),
        );
        return;
    }
    if let Some(field) = key.strip_suffix("_not") {
        insert_condition(
            out,
            field,
            nested_aware_value(entity_name, field, "_neq", value),
        );
        return;
    }
    if let Some(field) = key.strip_suffix("_gt") {
        insert_condition(
            out,
            field,
            nested_aware_value(entity_name, field, "_gt", value),
        );
        return;
    }
    if let Some(field) = key.strip_suffix("_lt") {
        insert_condition(
            out,
            field,
            nested_aware_value(entity_name, field, "_lt", value),
        );
        return;
    }
    if let Some(field) = key.strip_suffix("_in") {
        insert_condition(
            out,
            field,
            nested_aware_value(entity_name, field, "_in", value),
        );
        return;
    }

    // Unsupported in the inline path; pass through here so Hasura surfaces a
    // useful error instead of silently dropping the filter.
    if key.ends_with("_containsAny") || key.ends_with("_containsAll") {
        insert_condition(out, key, value.clone());
        return;
    }

    // No operator suffix — direct equality, connective (already handled), or
    // a nested-entity reference.

    if key == "chainId" {
        insert_condition(out, key, op_wrap("_eq", value.clone()));
        return;
    }

    // Subgraph's strict nested-entity filter uses a trailing underscore
    // (`pair_: { name: "X" }`); strip it for lookup and emission alike.
    let (lookup_key, target_key) = match key.strip_suffix('_') {
        Some(stripped) if !stripped.is_empty() => (stripped, stripped),
        _ => (key, key),
    };

    let is_nested = schema::is_nested_entity(entity_name, lookup_key);

    if value.is_object() {
        if is_nested {
            let nested_type = schema::get_field_info(entity_name, lookup_key)
                .and_then(|info| info.nested_type_name)
                .unwrap_or_else(|| lookup_key.to_string());
            let translated = translate_subgraph_filter_value(value, &nested_type);
            insert_condition(out, target_key, translated);
        } else {
            // Object value on a non-nested field — most commonly an already
            // Hasura-shaped sub-expression (e.g. `{_eq: 1}`). Pass through.
            insert_condition(out, target_key, value.clone());
        }
        return;
    }

    if is_nested {
        // Scalar against a nested entity → ID equality.
        let mut id_wrapper = serde_json::Map::new();
        id_wrapper.insert("id".to_string(), op_wrap("_eq", value.clone()));
        insert_condition(out, target_key, Value::Object(id_wrapper));
        return;
    }

    insert_condition(out, target_key, op_wrap("_eq", value.clone()));
}

/// Build an operator condition for `field`, routing through `id` when `field`
/// is an entity reference rather than a scalar column.
///
/// Subgraph lets you filter an entity reference by its id directly
/// (`launch_in: [$ids]`); Hasura requires that hop to be spelled out
/// (`launch: {id: {_in: [$ids]}}`) and rejects a bare operator on a
/// `*_bool_exp`. The unsuffixed form is already handled further down
/// `convert_basic_filter_to_hasura_condition`; this keeps the suffixed
/// operators consistent with it.
fn nested_aware_condition(entity_name: &str, field: &str, op: &str, value: &str) -> String {
    if schema::is_nested_entity(entity_name, field) {
        format!("{}: {{id: {{{}: {}}}}}", field, op, value)
    } else {
        format!("{}: {{{}: {}}}", field, op, value)
    }
}

/// JSON-path counterpart of `nested_aware_condition`, for filters that arrive
/// as a variable value rather than inline in the query text.
fn nested_aware_value(entity_name: &str, field: &str, op: &str, value: &Value) -> Value {
    let condition = op_wrap(op, value.clone());
    if schema::is_nested_entity(entity_name, field) {
        let mut id_wrapper = serde_json::Map::new();
        id_wrapper.insert("id".to_string(), condition);
        Value::Object(id_wrapper)
    } else {
        condition
    }
}

fn op_wrap(op: &str, value: Value) -> Value {
    let mut m = serde_json::Map::new();
    m.insert(op.to_string(), value);
    Value::Object(m)
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum LikeShape {
    Contains,
    StartsWith,
    EndsWith,
}

/// A graph-node string-matching filter suffix and the Hasura comparison it maps to.
#[derive(Clone, Copy)]
struct StringMatchFilter {
    suffix: &'static str,
    /// `_like` (case-sensitive, like graph-node's plain forms) or `_ilike`
    /// (the `_nocase` forms).
    operator: &'static str,
    shape: LikeShape,
    /// The `_not_` forms, which Hasura spells `_not: {field: {...}}`.
    negated: bool,
}

/// Longest suffix first, so `_not_contains_nocase` is not read as `_contains`.
const STRING_MATCH_FILTERS: [StringMatchFilter; 12] = {
    use LikeShape::*;
    const fn f(
        suffix: &'static str,
        operator: &'static str,
        shape: LikeShape,
        negated: bool,
    ) -> StringMatchFilter {
        StringMatchFilter {
            suffix,
            operator,
            shape,
            negated,
        }
    }
    [
        f("_not_starts_with_nocase", "_ilike", StartsWith, true),
        f("_not_ends_with_nocase", "_ilike", EndsWith, true),
        f("_not_contains_nocase", "_ilike", Contains, true),
        f("_starts_with_nocase", "_ilike", StartsWith, false),
        f("_ends_with_nocase", "_ilike", EndsWith, false),
        f("_contains_nocase", "_ilike", Contains, false),
        f("_not_starts_with", "_like", StartsWith, true),
        f("_not_ends_with", "_like", EndsWith, true),
        f("_not_contains", "_like", Contains, true),
        f("_starts_with", "_like", StartsWith, false),
        f("_ends_with", "_like", EndsWith, false),
        f("_contains", "_like", Contains, false),
    ]
};

/// The field and filter of a string-matching key (`name_contains_nocase`).
fn string_match_filter(key: &str) -> Option<(&str, StringMatchFilter)> {
    STRING_MATCH_FILTERS
        .iter()
        .find_map(|filter| key.strip_suffix(filter.suffix).map(|field| (field, *filter)))
}

/// The LIKE pattern graph-node builds for a value. It does not escape `%` or
/// `_`, so they act as wildcards there too, and `_contains` leaves a value that
/// already starts or ends with `%` as it is.
fn like_pattern(value: &str, shape: LikeShape) -> String {
    match shape {
        LikeShape::Contains if value.starts_with('%') || value.ends_with('%') => value.to_string(),
        LikeShape::Contains => format!("%{}%", value),
        LikeShape::StartsWith => format!("{}%", value),
        LikeShape::EndsWith => format!("%{}", value),
    }
}

fn like_value(value: &Value, filter: StringMatchFilter) -> Value {
    let s = match value {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        // Object/array/null aren't valid LIKE patterns; forward the raw value
        // so the upstream returns a meaningful type error.
        _ => return op_wrap(filter.operator, value.clone()),
    };
    op_wrap(filter.operator, Value::String(like_pattern(&s, filter.shape)))
}

impl LikeShape {
    const ALL: [LikeShape; 3] = [LikeShape::Contains, LikeShape::StartsWith, LikeShape::EndsWith];

    fn name(self) -> &'static str {
        match self {
            LikeShape::Contains => "contains",
            LikeShape::StartsWith => "starts_with",
            LikeShape::EndsWith => "ends_with",
        }
    }
}

/// The variable that stands in for `$var` under a string-matching filter and
/// carries its LIKE pattern: `q__contains` for `name_contains: $q`.
fn like_variable_name(var: &str, shape: LikeShape) -> String {
    format!("{}__{}", var, shape.name())
}

/// A variable holding the LIKE pattern built from another variable's value.
#[derive(Debug, Clone)]
struct LikeVariable {
    /// The variable the converted query reads (`q__contains`).
    target: String,
    /// The client's variable whose value the pattern is built from (`q`).
    source: String,
    shape: LikeShape,
    /// The source's default value from its declaration, used when the request
    /// leaves it out.
    default: Option<Value>,
}

/// The pattern variables a converted operation references, from the names in
/// its text. Only names whose source the client declared, and which the
/// client did not declare themselves, count.
fn like_variables(
    referenced: &HashSet<String>,
    header_var_types: &HashMap<String, String>,
) -> Vec<LikeVariable> {
    let mut found: Vec<LikeVariable> = referenced
        .iter()
        .filter(|name| !header_var_types.contains_key(*name))
        .filter_map(|name| {
            LikeShape::ALL.iter().find_map(|&shape| {
                let source = name.strip_suffix(&format!("__{}", shape.name()))?;
                let declared = header_var_types.get(source)?;
                Some(LikeVariable {
                    target: name.clone(),
                    source: source.to_string(),
                    shape,
                    default: declared_default(declared),
                })
            })
        })
        .collect();
    found.sort_by(|a, b| a.target.cmp(&b.target));
    found
}

/// The default value in a declared variable type (`String = "abc"`), when it is
/// a plain scalar literal.
fn declared_default(declared: &str) -> Option<Value> {
    let (_, default) = declared.split_once('=')?;
    graphql_literal_to_json(default.trim())
}

/// A GraphQL value literal (`{symbol: "A", decimals_gt: 6}`, `[1, 2]`, `"x"`) as
/// JSON, the form a variable's value takes. `None` if it does not parse or
/// mentions a variable, which a default value cannot.
fn graphql_literal_to_json(literal: &str) -> Option<Value> {
    use graphql_parser::query::{Definition, OperationDefinition, Selection};
    let source = format!("{{ f(a: {}) }}", literal);
    let doc = graphql_parser::parse_query::<String>(&source).ok()?;
    let Some(Definition::Operation(OperationDefinition::SelectionSet(set))) =
        doc.definitions.into_iter().next()
    else {
        return None;
    };
    let Some(Selection::Field(field)) = set.items.into_iter().next() else {
        return None;
    };
    let (_, value) = field.arguments.into_iter().next()?;
    graphql_value_to_json(value)
}

fn graphql_value_to_json(value: graphql_parser::query::Value<'_, String>) -> Option<Value> {
    use graphql_parser::query::Value as Gql;
    Some(match value {
        Gql::Variable(_) => return None,
        Gql::Int(n) => Value::from(n.as_i64()?),
        Gql::Float(f) => serde_json::Number::from_f64(f).map(Value::Number)?,
        Gql::String(s) => Value::String(s),
        Gql::Boolean(b) => Value::Bool(b),
        Gql::Null => Value::Null,
        Gql::Enum(e) => Value::String(e),
        Gql::List(items) => Value::Array(
            items
                .into_iter()
                .map(graphql_value_to_json)
                .collect::<Option<Vec<_>>>()?,
        ),
        Gql::Object(fields) => Value::Object(
            fields
                .into_iter()
                .map(|(k, v)| graphql_value_to_json(v).map(|v| (k, v)))
                .collect::<Option<serde_json::Map<_, _>>>()?,
        ),
    })
}

/// The value of a pattern variable for a request, or `None` when the source is
/// unset (and has no default) or null, leaving the pattern unset too.
fn like_variable_value(spec: &LikeVariable, variables: &serde_json::Map<String, Value>) -> Option<Value> {
    let source = variables.get(&spec.source).or(spec.default.as_ref())?;
    let text = match source {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        _ => return None,
    };
    Some(Value::String(like_pattern(&text, spec.shape)))
}

/// Same as convert_basic_filter_to_hasura_condition but also returns variable type override info
/// Returns: (condition_string, Option<(variable_name, expected_type)>)
fn convert_basic_filter_to_hasura_condition_with_type(
    key: &str,
    value: &str,
    entity_name: &str,
) -> Result<(String, Option<(String, String)>), ConversionError> {
    let condition = convert_basic_filter_to_hasura_condition(key, value, entity_name)?;

    // Check if value is a variable and if the field type needs conversion
    let trimmed_value = value.trim();
    if trimmed_value.starts_with('$') {
        // Extract variable name (remove $ prefix)
        let var_name = trimmed_value.trim_start_matches('$').to_string();

        // Get the base field name (remove operator suffix like _gte, _contains, etc.)
        let base_field = if let Some(underscore_idx) = key.find('_') {
            &key[..underscore_idx]
        } else {
            key
        };

        // Look up the field info in the schema
        if let Some(field_info) = schema::get_field_info(entity_name, base_field) {
            // Skip if this is a nested entity (object type) - those use String for ID references
            if field_info.is_nested_entity {
                return Ok((condition, None));
            }

            // Check if field expects numeric - users often declare String but field is numeric
            // This handles cases like withdrawUnlockEpoch where subgraph accepts String but Hyperindex needs numeric
            if field_info.field_type == "numeric"
                || field_info.field_type == "Int"
                || field_info.field_type == "Float"
            {
                tracing::debug!(
                    "Variable ${} used with field {}.{} expects type {} (numeric field)",
                    var_name,
                    entity_name,
                    base_field,
                    field_info.field_type
                );
                return Ok((condition, Some((var_name, field_info.field_type.clone()))));
            }

            // Check if it's an enum type (not a standard scalar)
            if !schema::is_standard_scalar(&field_info.field_type) {
                tracing::debug!(
                    "Variable ${} used with field {}.{} expects type {} (likely enum)",
                    var_name,
                    entity_name,
                    base_field,
                    field_info.field_type
                );
                return Ok((condition, Some((var_name, field_info.field_type))));
            }
        }
    }

    Ok((condition, None))
}

// Removed unused nested filter helper

// Removed unused entity/params extractor

fn parse_graphql_params(
    params_str: &str,
    params: &mut HashMap<String, String>,
) -> Result<(), ConversionError> {
    let mut current_param = String::new();
    let mut brace_count = 0;
    let mut bracket_count = 0;
    let mut in_string = false;
    let mut escape_next = false;

    for (byte_idx, ch) in params_str.char_indices() {
        if escape_next {
            current_param.push(ch);
            escape_next = false;
            continue;
        }

        if ch == '\\' {
            escape_next = true;
            current_param.push(ch);
            continue;
        }

        if ch == '"' {
            in_string = !in_string;
            current_param.push(ch);
            continue;
        }

        if !in_string {
            match ch {
                '{' => {
                    brace_count += 1;
                    current_param.push(ch);
                }
                '}' => {
                    brace_count -= 1;
                    current_param.push(ch);
                }
                '[' => {
                    bracket_count += 1;
                    current_param.push(ch);
                }
                ']' => {
                    bracket_count -= 1;
                    current_param.push(ch);
                }
                ',' => {
                    if brace_count == 0 && bracket_count == 0 {
                        parse_single_param(&current_param, params)?;
                        current_param.clear();
                    } else {
                        current_param.push(ch);
                    }
                }
                // GraphQL separates arguments with any whitespace, so a space
                // or tab ends the current argument too, once it has a value
                // (`first: 100 skip: 0`).
                ' ' | '\t'
                    if brace_count == 0
                        && bracket_count == 0
                        && current_param
                            .split_once(':')
                            .map(|(_, value)| !value.trim().is_empty())
                            .unwrap_or(false) =>
                {
                    let next_byte_idx = byte_idx + ch.len_utf8();
                    if starts_with_argument_name(&params_str[next_byte_idx..]) {
                        parse_single_param(&current_param, params)?;
                        current_param.clear();
                    } else {
                        current_param.push(ch);
                    }
                }
                '\n' | '\r' => {
                    // Handle newlines as parameter separators when at top level
                    if brace_count == 0 && bracket_count == 0 {
                        // Look ahead to see if next non-whitespace content is a parameter name (identifier:)
                        // Use byte_idx to slice the string correctly (char_indices gives us byte positions)
                        let next_byte_idx = byte_idx + ch.len_utf8();
                        let remaining = &params_str[next_byte_idx..];
                        let trimmed = remaining.trim_start();

                        // Check if trimmed starts with identifier pattern followed by colon
                        // Pattern: [a-zA-Z_][a-zA-Z0-9_]*\s*:
                        let mut chars_iter = trimmed.chars();
                        if let Some(first) = chars_iter.next() {
                            if first.is_alphabetic() || first == '_' {
                                // Continue reading identifier
                                let mut is_param = true;
                                let mut found_colon = false;
                                for c in chars_iter {
                                    if c == ':' {
                                        found_colon = true;
                                        break;
                                    } else if c.is_alphanumeric() || c == '_' {
                                        continue;
                                    } else if c.is_whitespace() {
                                        continue;
                                    } else {
                                        is_param = false;
                                        break;
                                    }
                                }

                                if is_param && found_colon {
                                    // This is a new parameter, finish current one
                                    if !current_param.trim().is_empty() {
                                        parse_single_param(&current_param, params)?;
                                        current_param.clear();
                                    }
                                    // Skip the newline, don't add it to current_param
                                    continue;
                                }
                            }
                        }
                        // Not a new parameter, preserve newline in value
                        current_param.push(ch);
                    } else {
                        // Inside braces/brackets, preserve newline
                        current_param.push(ch);
                    }
                }
                _ => current_param.push(ch),
            }
        } else {
            current_param.push(ch);
        }
    }

    if !current_param.trim().is_empty() {
        parse_single_param(&current_param, params)?;
    }

    Ok(())
}

/// Whether `text`, after leading whitespace, begins with `name:`, the start of
/// another argument.
fn starts_with_argument_name(text: &str) -> bool {
    let trimmed = text.trim_start();
    let mut chars = trimmed.chars();
    match chars.next() {
        Some(first) if first.is_alphabetic() || first == '_' => {}
        _ => return false,
    }
    let rest: String = chars.collect();
    let after_name = rest.trim_start_matches(|c: char| c.is_alphanumeric() || c == '_');
    after_name.trim_start().starts_with(':')
}

/// Argument names a hoisted `where` field must not be mistaken for.
const ARGUMENT_NAMES: [&str; 7] = [
    "first",
    "skip",
    "orderBy",
    "orderDirection",
    "where",
    "block",
    "subgraphError",
];

/// Prefix on a hoisted `where` field named like an argument. `:` cannot occur
/// in a GraphQL name, so the marked key cannot clash with a real one.
const WHERE_FIELD_MARKER: &str = "where:";

fn parse_single_param(
    param_str: &str,
    params: &mut HashMap<String, String>,
) -> Result<(), ConversionError> {
    let trimmed = param_str.trim();
    if let Some(idx) = trimmed.find(':') {
        let key = trimmed[..idx].trim();
        let value = trimmed[idx + 1..].trim();

        // Special handling for 'where' clause - don't flatten it
        if key == "where" && value.starts_with('{') && value.ends_with('}') {
            // Parse the nested object but don't flatten the keys
            let nested_content = &value[1..value.len() - 1];
            let mut nested_params = HashMap::new();
            parse_graphql_params(nested_content, &mut nested_params)?;

            // Add nested params directly without flattening. A filter field
            // that shares a name with an argument (`where: {first: 2}`) is
            // marked, so it can neither become pagination nor overwrite the
            // real `first`; `convert_filters_to_where_clause` unmarks it.
            for (nested_key, nested_value) in nested_params {
                let head = nested_key.split('.').next().unwrap_or_default();
                let key = if ARGUMENT_NAMES.contains(&head) {
                    format!("{}{}", WHERE_FIELD_MARKER, nested_key)
                } else {
                    nested_key
                };
                params.insert(key, nested_value);
            }
        } else if value.starts_with('{') && value.ends_with('}') {
            // Parse the nested object
            let nested_content = &value[1..value.len() - 1];
            let mut nested_params = HashMap::new();
            parse_graphql_params(nested_content, &mut nested_params)?;

            // Convert nested params to flattened keys
            for (nested_key, nested_value) in nested_params {
                let flattened_key = format!("{}.{}", key, nested_key);
                params.insert(flattened_key, nested_value);
            }
        } else {
            params.insert(key.to_string(), value.to_string());
        }
    }
    Ok(())
}

// Removed unused brace matching helper

/// Strip `#` comments out of an argument list, keeping the line breaks.
///
/// GraphQL allows a comment anywhere whitespace is allowed, including between
/// arguments, and it runs to the end of the line. The converter re-emits
/// arguments on a single line, so a comment left in place comments out
/// everything after it - the remaining arguments and the closing paren
/// included. It also makes `block:` appear in a note look like a real
/// time-travel argument. A `#` inside a string literal is not a comment.
fn strip_line_comments(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars();
    let mut in_string = false;
    let mut escape = false;

    while let Some(ch) = chars.next() {
        if escape {
            out.push(ch);
            escape = false;
            continue;
        }
        if in_string {
            match ch {
                '\\' => escape = true,
                '"' => in_string = false,
                _ => {}
            }
            out.push(ch);
            continue;
        }
        match ch {
            '"' => {
                in_string = true;
                out.push(ch);
            }
            '#' => {
                // Keep the line break. Commas are optional in GraphQL, so it can
                // be the only thing separating this argument from the next.
                for cc in chars.by_ref() {
                    if cc == '\n' || cc == '\r' {
                        out.push(cc);
                        break;
                    }
                }
            }
            _ => out.push(ch),
        }
    }

    out
}

/// One top-level `name: value` pair from an argument list, as raw text.
struct TopLevelArg<'a> {
    /// Byte range of the whole pair, for excising it from the argument text.
    span: std::ops::Range<usize>,
    name: &'a str,
    value: &'a str,
}

/// Split an argument list into its *top-level* pairs, leaving values as raw text.
///
/// `parse_single_param` hoists `where: {...}` contents into the same map without
/// a prefix, so by the time arguments are parsed a key like `block` is
/// ambiguous: it is equally a time-travel argument and a filter on an entity
/// that has a `block` field. This reads the raw argument text, where the nesting
/// is still visible.
///
/// Commas are insignificant in GraphQL, so `first: 1 block: {…}` is the same
/// list as `first: 1, block: {…}` - and the comma-less form is how prettier
/// prints a multi-line query. An unexpected token is skipped rather than ending
/// the scan, so one malformed argument cannot hide the arguments after it.
fn top_level_arguments(params_str: &str) -> Vec<TopLevelArg<'_>> {
    let bytes = params_str.as_bytes();
    let mut args = Vec::new();
    let mut i = 0usize;

    while i < bytes.len() {
        if bytes[i].is_ascii_whitespace() || bytes[i] == b',' {
            i += 1;
            continue;
        }

        // A GraphQL name: /[_A-Za-z][_0-9A-Za-z]*/.
        let name_start = i;
        while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
            i += 1;
        }
        if i == name_start {
            i += 1; // Not a name at all; resynchronize.
            continue;
        }
        let name_end = i;

        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= bytes.len() || bytes[i] != b':' {
            continue; // Not an argument. The name was consumed, so this advances.
        }
        i += 1;
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }

        let value_start = i;
        let value_end = value_end_at(bytes, value_start);
        if value_end == value_start {
            continue; // No value to take. The name was consumed, so this advances.
        }
        i = value_end;

        args.push(TopLevelArg {
            span: name_start..value_end,
            name: params_str.get(name_start..name_end).unwrap_or_default(),
            value: params_str.get(value_start..value_end).unwrap_or_default(),
        });
    }

    args
}

/// Where the single GraphQL value starting at `from` ends.
///
/// Objects and lists are followed to their matching close, counting both kinds
/// together so a list of objects (or an object holding a list) ends in the right
/// place. Everything else - a number, an enum, `$variable`, `true` - runs to the
/// next separator.
fn value_end_at(bytes: &[u8], from: usize) -> usize {
    let mut i = from;
    match bytes.get(i) {
        Some(b'{') | Some(b'[') => {
            let mut depth = 0usize;
            let mut in_string = false;
            let mut escape = false;
            while i < bytes.len() {
                let c = bytes[i];
                i += 1;
                if escape {
                    escape = false;
                    continue;
                }
                if in_string {
                    match c {
                        b'\\' => escape = true,
                        b'"' => in_string = false,
                        _ => {}
                    }
                    continue;
                }
                match c {
                    b'"' => in_string = true,
                    b'{' | b'[' => depth += 1,
                    b'}' | b']' => {
                        depth = depth.saturating_sub(1);
                        if depth == 0 {
                            return i;
                        }
                    }
                    _ => {}
                }
            }
            i
        }
        Some(b'"') => {
            i += 1;
            let mut escape = false;
            while i < bytes.len() {
                let c = bytes[i];
                i += 1;
                if escape {
                    escape = false;
                    continue;
                }
                match c {
                    b'\\' => escape = true,
                    b'"' => return i,
                    _ => {}
                }
            }
            i
        }
        Some(_) => {
            while i < bytes.len()
                && !bytes[i].is_ascii_whitespace()
                && !matches!(bytes[i], b',' | b')' | b'}' | b']')
            {
                i += 1;
            }
            i
        }
        None => i,
    }
}

fn singularize_and_capitalize(s: &str) -> String {
    // Prefer an entity the schema actually declares. Word rules cannot recover
    // `TxTransfers` from `txTransferses`, and a wrong guess queries a table that
    // does not exist — which surfaces as an empty list, not an error.
    if let Some(entity) = schema::resolve_entity_for_collection_field(s) {
        return entity;
    }

    // Improved singularization to cover common English plural forms used in schema entity names
    // First, handle irregulars explicitly
    let lower = s.to_lowercase();
    let irregulars: &[(&str, &str)] = &[("tranches", "tranche")];
    if let Some((_, singular_irregular)) = irregulars.iter().find(|(pl, _)| *pl == &lower) {
        let mut c = singular_irregular.chars();
        return match c.next() {
            None => String::new(),
            Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
        };
    }

    let singular: String = if s.ends_with("ies") && s.len() > 3 {
        // companies -> company
        format!("{}y", &s[..s.len() - 3])
    } else if s.ends_with("ches")
        || s.ends_with("shes")
        || s.ends_with("xes")
        || s.ends_with("zes")
        || s.ends_with("sses")
        || s.ends_with("oes")
    {
        // batches -> batch, boxes -> box, addresses -> address, heroes -> hero, users -> user (via 'ses')
        s[..s.len() - 2].to_string()
    } else if s.ends_with('s') && s.len() > 1 {
        // Default: drop trailing 's'
        s[..s.len() - 1].to_string()
    } else {
        s.to_string()
    };

    let mut c = singular.chars();
    match c.next() {
        None => String::new(),
        Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn create_test_payload(query: &str) -> Value {
        json!({
            "query": query
        })
    }

    // Initialize test schema for unit tests.
    //
    // The previous "always reset" policy was racy under cargo's default parallel
    // scheduler: `init_test_schema` does `clear()` followed by per-entity inserts,
    // and a concurrent reader could land on a half-populated cache. Since unit
    // tests never mutate the schema after init, we use `std::sync::Once` so that
    // the populate runs exactly once, atomically — concurrent callers block
    // until the first call completes.
    fn init_test_schema_if_needed() {
        schema::init_test_schema_once();
    }

    #[test]
    fn test_basic_collection_query() {
        let payload = create_test_payload("query { streams(first: 10, skip: 0) { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(limit: 10, offset: 0, order_by: {id: asc}, where: {chainId: {_eq: \"1\"}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    // Regression: entity names with underscored prefixes (e.g. contract-namespaced
    // entities like `V2_Factory_Swaps`) must be preserved as a single identifier
    // and singularized at the suffix only. Before the fix, the entity tokenizer
    // walked only alphanumeric chars and chopped the name into `V2` / `Factory` /
    // `Swaps`, leaving just `Swap` in the converted query.
    #[test]
    fn test_prefixed_entity_name_preserved() {
        let payload = create_test_payload(
            "query { V2_Factory_Swaps(first: 1, orderBy: timestamp, orderDirection: desc) { id timestamp amountUSD } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  V2_Factory_Swap(limit: 1, order_by: [{timestamp: desc}, {id: desc}], where: {chainId: {_eq: \"1\"}}) {\n    id timestamp amountUSD\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_single_entity_query() {
        let payload = create_test_payload("query { stream(id: \"123\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let expected = json!({
            "query": "query {\n  Stream_by_pk(id: \"123\") {\n    id name\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_meta_query_simple() {
        init_test_schema_if_needed();
        let payload = create_test_payload("query { _meta { block { number } } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  _meta(where: {chainId: {_eq: 1}}) {\n    progressBlock\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_meta_query_simple_multiline() {
        init_test_schema_if_needed();
        // Test the multiline version that was failing before
        let payload =
            create_test_payload("query {\n  _meta {\n    block {\n      number\n    }\n  }\n}");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  _meta(where: {chainId: {_eq: 1}}) {\n    progressBlock\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_meta_query_with_tabs_and_mixed_whitespace() {
        init_test_schema_if_needed();
        // Test with tabs and mixed whitespace
        let payload =
            create_test_payload("query {\n\t_meta {\n\t\tblock {\n\t\t\tnumber\n\t\t}\n\t}\n}");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  _meta(where: {chainId: {_eq: 1}}) {\n    progressBlock\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_meta_query_with_extra_spaces() {
        init_test_schema_if_needed();
        // Test with extra spaces between tokens
        let payload = create_test_payload("query  {   _meta   {   block   {   number   }   }   }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  _meta(where: {chainId: {_eq: 1}}) {\n    progressBlock\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    /// A `_meta` selection is answered in full or not at all.
    ///
    /// Serving `block { number }` and quietly omitting `hash` would hand back an
    /// object the caller cannot tell from a complete one.
    #[test]
    fn test_meta_query_rejects_any_unservable_field() {
        init_test_schema_if_needed();
        for (query, expected) in [
            ("query { _meta { block { hash number } } }", "block.hash"),
            (
                "query {\n  _meta {\n    block {\n      hash\n      number\n    }\n  }\n}",
                "block.hash",
            ),
            (
                "query { _meta { block { number } hasIndexingErrors } }",
                "hasIndexingErrors",
            ),
            (
                "query { _meta { block { number } deployment hasIndexingErrors } }",
                "hasIndexingErrors",
            ),
        ] {
            let payload = create_test_payload(query);
            match convert_subgraph_to_hyperindex(&payload, Some("1")) {
                Err(ConversionError::ComplexMetaQuery(fields)) => assert_eq!(
                    fields, expected,
                    "the error should name the unservable field(s) for {}",
                    query
                ),
                other => panic!(
                    "expected ComplexMetaQuery for {}, got {:?}",
                    query,
                    other.map(|r| r.query)
                ),
            }
        }
    }

    /// Argus `Meta`, verbatim. `hasIndexingErrors` has no Hyperindex source, so
    /// the query is rejected rather than answered partially.
    #[test]
    fn test_meta_query_argus_verbatim_is_rejected() {
        init_test_schema_if_needed();
        let payload = create_test_payload(
            "query Meta { _meta { block { number timestamp } hasIndexingErrors } }",
        );
        match convert_subgraph_to_hyperindex(&payload, Some("5042")) {
            Err(ConversionError::ComplexMetaQuery(fields)) => {
                assert_eq!(fields, "hasIndexingErrors")
            }
            other => panic!("expected ComplexMetaQuery, got {:?}", other.map(|r| r.query)),
        }
    }

    /// Reducing that query to what Hyperindex can serve makes it work.
    #[test]
    fn test_meta_query_argus_reduced_to_block_number_succeeds() {
        init_test_schema_if_needed();
        let payload = create_test_payload("query Meta { _meta { block { number } } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("5042")).unwrap();
        assert_eq!(
            result.query["query"].as_str().unwrap(),
            "query Meta {\n  _meta(where: {chainId: {_eq: 5042}}) {\n    progressBlock\n  }\n}"
        );
        assert!(result.meta_selections.first().cloned().is_some());
        assert_eq!(
            result.meta_selections.first().cloned(),
            Some(MetaSelection {
                block_number: true,
                key: "_meta".to_string(),
                ..MetaSelection::default()
            })
        );
    }

    /// `__typename` is answered locally, so it does not make a query unservable.
    /// Clients that inject it automatically keep working.
    #[test]
    fn test_meta_query_allows_typename() {
        init_test_schema_if_needed();
        let payload =
            create_test_payload("query { _meta { __typename block { number __typename } } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        assert_eq!(
            result.meta_selections.first().cloned(),
            Some(MetaSelection {
                block_number: true,
                typename: true,
                block_typename: true,
                key: "_meta".to_string(),
                ..MetaSelection::default()
            })
        );
    }

    #[test]
    fn selection_leaf_paths_walks_nested_selections() {
        assert_eq!(
            selection_leaf_paths("block { number timestamp } hasIndexingErrors"),
            vec![
                "block.number".to_string(),
                "block.timestamp".to_string(),
                "hasIndexingErrors".to_string()
            ]
        );
    }

    #[test]
    fn test_meta_query_block_number_token_is_not_matched_by_prefix() {
        init_test_schema_if_needed();
        // `blockNumber` must not be mistaken for `block { number }`; nothing here
        // is servable, so this still fails loudly.
        let payload = create_test_payload("query { _meta { block { blockNumber } } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1"));
        match result {
            Err(ConversionError::ComplexMetaQuery(_)) => {}
            other => panic!(
                "Expected ComplexMetaQuery error, got {:?}",
                other.map(|r| r.query)
            ),
        }
    }

    #[test]
    fn test_meta_query_with_deployment() {
        init_test_schema_if_needed();
        // deployment is answered from the Hyperindex endpoint's deployment hash.
        let payload = create_test_payload("query { _meta { deployment } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        assert!(result.meta_selections.first().cloned().unwrap().deployment);
    }

    #[test]
    fn test_meta_query_with_has_indexing_errors() {
        init_test_schema_if_needed();
        // hasIndexingErrors field should trigger ComplexMetaQuery error
        let payload = create_test_payload("query { _meta { hasIndexingErrors } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1"));
        assert!(result.is_err());
        match result {
            Err(ConversionError::ComplexMetaQuery(_)) => {}
            _ => panic!("Expected ComplexMetaQuery error"),
        }
    }

    #[test]
    fn test_meta_query_with_timestamp_multiline() {
        // Hasura's `_meta` carries the processed block's time, so timestamp is
        // served; `chain_metadata` does not, so without it timestamp is refused.
        init_test_schema_if_needed();
        let payload =
            create_test_payload("query {\n  _meta {\n    block {\n      timestamp\n    }\n  }\n}");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        assert_eq!(
            result.query["query"].as_str().unwrap(),
            "query {\n  _meta(where: {chainId: {_eq: 1}}) {\n    progressBlock progressBlockTime\n  }\n}"
        );
        assert!(result.meta_selections.first().cloned().unwrap().block_timestamp);
        match convert_meta_selection("block { timestamp }", false, true, "_meta".to_string()) {
            Err(ConversionError::ComplexMetaQuery(fields)) => assert_eq!(fields, "block.timestamp"),
            other => panic!("expected ComplexMetaQuery, got {:?}", other),
        }
    }

    // Filter tests
    #[test]
    fn test_equality_filter() {
        let payload = create_test_payload("query { streams(name: \"test\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(limit: 100, order_by: {id: asc}, where: {chainId: {_eq: \"1\"}, name: {_eq: \"test\"}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_not_filter() {
        let payload = create_test_payload("query { streams(name_not: \"test\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(limit: 100, order_by: {id: asc}, where: {chainId: {_eq: \"1\"}, name: {_neq: \"test\"}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_greater_than_filter() {
        let payload = create_test_payload("query { streams(amount_gt: 100) { id amount } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(limit: 100, order_by: {id: asc}, where: {chainId: {_eq: \"1\"}, amount: {_gt: 100}}) {\n    id amount\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_greater_than_or_equal_filter() {
        let payload = create_test_payload("query { streams(amount_gte: 100) { id amount } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(limit: 100, order_by: {id: asc}, where: {chainId: {_eq: \"1\"}, amount: {_gte: 100}}) {\n    id amount\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_less_than_filter() {
        let payload = create_test_payload("query { streams(amount_lt: 100) { id amount } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(limit: 100, order_by: {id: asc}, where: {chainId: {_eq: \"1\"}, amount: {_lt: 100}}) {\n    id amount\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_less_than_or_equal_filter() {
        let payload = create_test_payload("query { streams(amount_lte: 100) { id amount } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(limit: 100, order_by: {id: asc}, where: {chainId: {_eq: \"1\"}, amount: {_lte: 100}}) {\n    id amount\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_in_filter() {
        let payload =
            create_test_payload("query { streams(id_in: [\"1\", \"2\", \"3\"]) { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(limit: 100, order_by: {id: asc}, where: {chainId: {_eq: \"1\"}, id: {_in: [\"1\", \"2\", \"3\"]}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_not_in_filter() {
        let payload =
            create_test_payload("query { streams(id_not_in: [\"1\", \"2\", \"3\"]) { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(limit: 100, order_by: {id: asc}, where: {chainId: {_eq: \"1\"}, id: {_nin: [\"1\", \"2\", \"3\"]}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_contains_filter() {
        let payload = create_test_payload("query { streams(name_contains: \"test\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(limit: 100, order_by: {id: asc}, where: {chainId: {_eq: \"1\"}, name: {_like: \"%test%\"}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_not_contains_filter() {
        let payload =
            create_test_payload("query { streams(name_not_contains: \"test\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(limit: 100, order_by: {id: asc}, where: {chainId: {_eq: \"1\"}, _not: {name: {_like: \"%test%\"}}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_starts_with_filter() {
        let payload =
            create_test_payload("query { streams(name_starts_with: \"test\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(limit: 100, order_by: {id: asc}, where: {chainId: {_eq: \"1\"}, name: {_like: \"test%\"}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_ends_with_filter() {
        let payload =
            create_test_payload("query { streams(name_ends_with: \"test\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(limit: 100, order_by: {id: asc}, where: {chainId: {_eq: \"1\"}, name: {_like: \"%test\"}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_not_starts_with_filter() {
        let payload =
            create_test_payload("query { streams(name_not_starts_with: \"test\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(limit: 100, order_by: {id: asc}, where: {chainId: {_eq: \"1\"}, _not: {name: {_like: \"test%\"}}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_not_ends_with_filter() {
        let payload =
            create_test_payload("query { streams(name_not_ends_with: \"test\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(limit: 100, order_by: {id: asc}, where: {chainId: {_eq: \"1\"}, _not: {name: {_like: \"%test\"}}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_contains_nocase_filter() {
        let payload =
            create_test_payload("query { streams(name_contains_nocase: \"test\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(limit: 100, order_by: {id: asc}, where: {chainId: {_eq: \"1\"}, name: {_ilike: \"%test%\"}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_not_contains_nocase_filter() {
        let payload = create_test_payload(
            "query { streams(name_not_contains_nocase: \"test\") { id name } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(limit: 100, order_by: {id: asc}, where: {chainId: {_eq: \"1\"}, _not: {name: {_ilike: \"%test%\"}}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_starts_with_nocase_filter() {
        let payload =
            create_test_payload("query { streams(name_starts_with_nocase: \"test\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(limit: 100, order_by: {id: asc}, where: {chainId: {_eq: \"1\"}, name: {_ilike: \"test%\"}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_ends_with_nocase_filter() {
        let payload =
            create_test_payload("query { streams(name_ends_with_nocase: \"test\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(limit: 100, order_by: {id: asc}, where: {chainId: {_eq: \"1\"}, name: {_ilike: \"%test\"}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_not_starts_with_nocase_filter() {
        let payload = create_test_payload(
            "query { streams(name_not_starts_with_nocase: \"test\") { id name } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(limit: 100, order_by: {id: asc}, where: {chainId: {_eq: \"1\"}, _not: {name: {_ilike: \"test%\"}}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_not_ends_with_nocase_filter() {
        let payload = create_test_payload(
            "query { streams(name_not_ends_with_nocase: \"test\") { id name } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(limit: 100, order_by: {id: asc}, where: {chainId: {_eq: \"1\"}, _not: {name: {_ilike: \"%test\"}}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_unsupported_contains_any_filter() {
        let payload = create_test_payload(
            "query { streams(tags_containsAny: [\"tag1\", \"tag2\"]) { id name } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, Some("1"));
        assert!(result.is_err());
        match result {
            Err(ConversionError::UnsupportedFilter(filter)) => {
                assert_eq!(filter, "tags_containsAny");
            }
            _ => panic!("Expected UnsupportedFilter error"),
        }
    }

    #[test]
    fn test_unsupported_contains_all_filter() {
        let payload = create_test_payload(
            "query { streams(tags_containsAll: [\"tag1\", \"tag2\"]) { id name } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, Some("1"));
        assert!(result.is_err());
        match result {
            Err(ConversionError::UnsupportedFilter(filter)) => {
                assert_eq!(filter, "tags_containsAll");
            }
            _ => panic!("Expected UnsupportedFilter error"),
        }
    }

    #[test]
    fn test_multiple_filters() {
        let payload = create_test_payload(
            "query { streams(name_contains: \"test\", amount_gt: 100, status: \"active\") { id name amount status } }"
        );
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let query = result.query["query"].as_str().unwrap();
        // Check for all filter fragments regardless of order
        assert!(query.contains("chainId: {_eq: \"1\"}"));
        assert!(query.contains("name: {_like: \"%test%\"}"));
        assert!(query.contains("amount: {_gt: 100}"));
        assert!(query.contains("status: {_eq: \"active\"}"));
        // Also check the selection set
        assert!(query.contains("id name amount status"));
        // And the entity name
        assert!(query.contains("Stream"));
    }

    #[test]
    fn test_non_stream_entity() {
        let payload = create_test_payload("query { users(name_contains: \"john\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  User(limit: 100, order_by: {id: asc}, where: {chainId: {_eq: \"1\"}, name: {_like: \"%john%\"}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_pagination_parameters() {
        let payload = create_test_payload("query { streams(first: 5, skip: 10) { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(limit: 5, offset: 10, order_by: {id: asc}, where: {chainId: {_eq: \"1\"}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_order_parameters() {
        let payload = create_test_payload(
            "query { streams(orderBy: name, orderDirection: desc) { id name } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(limit: 100, order_by: [{name: desc}, {id: desc}], where: {chainId: {_eq: \"1\"}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_order_by_with_skip_and_where() {
        let payload = create_test_payload(
            "query { streams(orderBy: alias, skip: 10, where: {alias_contains: \"113\"}) { alias asset { address } } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(limit: 100, offset: 10, order_by: [{alias: asc}, {id: asc}], where: {chainId: {_eq: \"1\"}, alias: {_like: \"%113%\"}}) {\n    alias asset { address }\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_complex_selection_set() {
        let payload =
            create_test_payload("query { streams { id name amount status { id name } } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(limit: 100, order_by: {id: asc}, where: {chainId: {_eq: \"1\"}}) {\n    id name amount status { id name }\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_missing_query_field() {
        let payload = json!({});
        let result = convert_subgraph_to_hyperindex(&payload, Some("1"));
        assert!(result.is_err());
        match result {
            Err(ConversionError::MissingField(field)) => {
                assert_eq!(field, "query");
            }
            _ => panic!("Expected MissingField error"),
        }
    }

    #[test]
    fn test_invalid_query_format() {
        let payload = json!({
            "query": 123
        });
        let result = convert_subgraph_to_hyperindex(&payload, Some("1"));
        assert!(result.is_err());
        match result {
            Err(ConversionError::QueryNotString) => {}
            _ => panic!("Expected QueryNotString error"),
        }
    }

    #[test]
    fn a_where_filter_on_a_block_field_is_not_mistaken_for_time_travel() {
        init_test_schema_if_needed();
        // `parse_single_param` hoists `where: {...}` into the same param map
        // unprefixed, so the guard cannot read the flattened keys.
        let payload = create_test_payload(r#"query { trades(where: { block: 123 }, first: 1) { id } }"#);
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let converted = result.query["query"].as_str().unwrap();
        assert!(converted.contains("where: {block: {_eq: 123}}"), "{converted}");
    }

    #[test]
    fn a_nested_where_filter_named_block_is_not_mistaken_for_time_travel() {
        init_test_schema_if_needed();
        let payload =
            create_test_payload(r#"query { trades(where: { block: { number: 5 } }, first: 1) { id } }"#);
        assert!(convert_subgraph_to_hyperindex(&payload, None).is_ok());
    }

    #[test]
    fn an_alias_colliding_with_another_roots_entity_name_gets_a_distinct_hasura_key() {
        init_test_schema_if_needed();
        // `users:` is the alias of one root; `User` is the Hasura name of the
        // other. Emitting both under the same response name makes Hasura reject
        // the whole operation, so the keys must differ.
        //
        // (The two roots here share the caller-facing name `users`, which is
        // itself invalid GraphQL - graph-node rejects two root fields with the
        // same response name. The converter only has to avoid turning it into a
        // 502 from Hasura.)
        let payload = create_test_payload("query { users: trades(first: 1) { id } users(first: 1) { id } }");
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let keys: std::collections::HashSet<&String> = result.field_name_map.keys().collect();
        assert_eq!(keys.len(), 2, "map: {:?}", result.field_name_map);
    }

    #[test]
    fn an_alias_equal_to_an_entity_name_does_not_duplicate_a_response_name() {
        init_test_schema_if_needed();
        let payload = create_test_payload("query { Trade: users(first: 1) { id } trades(first: 1) { id } }");
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let converted = result.query["query"].as_str().unwrap();
        // Whatever the emitted keys are, they must be distinct or Hasura rejects
        // the operation with "specify the same response name".
        let emitted: Vec<&String> = result.field_name_map.keys().collect();
        assert_eq!(
            emitted.iter().collect::<std::collections::HashSet<_>>().len(),
            emitted.len(),
            "{converted}"
        );
        assert!(result.field_name_map.values().any(|v| v == "Trade"), "{:?}", result.field_name_map);
        assert!(result.field_name_map.values().any(|v| v == "trades"), "{:?}", result.field_name_map);
    }

    #[test]
    fn two_roots_sharing_an_order_by_variable_keep_their_own_directions() {
        init_test_schema_if_needed();
        let payload = create_test_payload(
            "query Q($o: Trade_orderBy!, $d1: OrderDirection!, $d2: OrderDirection!) { \
             a: trades(first: 2, orderBy: $o, orderDirection: $d1) { id } \
             b: trades(first: 2, orderBy: $o, orderDirection: $d2) { id } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        // One spec per direction, or the second root silently inherits the first.
        assert_eq!(
            result.order_by_variables.len(),
            2,
            "specs: {:?}",
            result.order_by_variables
        );
        let targets: std::collections::HashSet<&String> = result
            .order_by_variables
            .iter()
            .map(|sp| &sp.target_var)
            .collect();
        assert_eq!(targets.len(), 2, "specs: {:?}", result.order_by_variables);
    }

    #[test]
    fn by_id_lookup_on_an_entity_whose_name_ends_in_s_returns_an_object() {
        init_test_schema_if_needed();
        // `TxTransfers` is a real entity name; `txTransfers` is its singular
        // graph-node spelling, so this is a primary-key lookup, not a list.
        let payload = create_test_payload(r#"query { txTransfers(id: "1") { id } }"#);
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let converted = result.query["query"].as_str().unwrap();
        assert!(converted.contains("TxTransfers_by_pk(id:"), "{converted}");
    }

    #[test]
    fn a_root_typename_does_not_corrupt_the_next_fields_alias() {
        init_test_schema_if_needed();
        // Apollo injects `__typename` into every selection set, root included.
        // The scanner used to resume one character into the next identifier.
        let payload = create_test_payload("query { __typename recent: trades(first: 1) { id } }");
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        assert!(
            result.field_name_map.values().any(|v| v == "recent"),
            "map: {:?}",
            result.field_name_map
        );
    }

    #[test]
    fn an_aliased_meta_keeps_its_alias() {
        init_test_schema_if_needed();
        let payload = create_test_payload("query { m: _meta { block { number } } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        assert_eq!(
            result.meta_selections.first().cloned().as_ref().map(|m| m.key.clone()),
            Some("m".to_string())
        );
    }

    #[test]
    fn acronym_entity_names_use_graph_nodes_spelling() {
        init_test_schema_if_needed();
        // graph-node lowercases the leading run of capitals and rejects any other
        // spelling, so these are the only names a real subgraph client can send.
        assert_eq!(
            singularize_and_capitalize("atokenBalanceHistoryItems"),
            "ATokenBalanceHistoryItem"
        );
        assert_eq!(
            singularize_and_capitalize("atokenBalanceHistoryItem"),
            "ATokenBalanceHistoryItem"
        );
        assert_eq!(singularize_and_capitalize("emodeCategories"), "EModeCategory");
        assert_eq!(singularize_and_capitalize("emodeCategory"), "EModeCategory");
        // The lowercase-first-letter spelling stays accepted.
        assert_eq!(
            singularize_and_capitalize("aTokenBalanceHistoryItems"),
            "ATokenBalanceHistoryItem"
        );
        assert_eq!(singularize_and_capitalize("eModeCategories"), "EModeCategory");
    }

    #[test]
    fn root_field_alias_is_preserved() {
        init_test_schema_if_needed();
        let payload = create_test_payload("query { a: streams(first: 1) { id } }");
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let converted = result.query["query"].as_str().unwrap();
        assert!(
            converted.contains("a: Stream(limit: 1, order_by: {id: asc})"),
            "alias missing from converted query: {converted}"
        );
        // The response already arrives keyed by the alias, so it must map to itself.
        assert_eq!(result.field_name_map.get("a"), Some(&"a".to_string()));
    }

    #[test]
    fn primary_key_alias_is_preserved() {
        init_test_schema_if_needed();
        let payload = create_test_payload(r#"query { s: stream(id: "1") { id } }"#);
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let converted = result.query["query"].as_str().unwrap();
        assert!(
            converted.contains(r#"s: Stream_by_pk(id: "1")"#),
            "alias missing from converted pk query: {converted}"
        );
    }

    #[test]
    fn two_aliases_of_one_entity_both_survive() {
        init_test_schema_if_needed();
        let payload =
            create_test_payload("query { first: streams(first: 1) { id } second: streams(first: 2) { id } }");
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let converted = result.query["query"].as_str().unwrap();
        assert!(converted.contains("first: Stream(limit: 1, order_by: {id: asc})"), "{converted}");
        assert!(converted.contains("second: Stream(limit: 2, order_by: {id: asc})"), "{converted}");
    }

    #[test]
    fn selection_set_survives_a_body_with_no_spaces() {
        init_test_schema_if_needed();
        // `trim_end_matches('}')` used to eat both closing braces, leaving the
        // selection set unterminated and the query empty.
        let payload = create_test_payload("{streams{id}}");
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let converted = result.query["query"].as_str().unwrap();
        assert!(converted.contains("Stream"), "{converted}");
        assert!(converted.contains("id"), "{converted}");
    }

    #[test]
    fn block_argument_is_rejected_rather_than_turned_into_a_filter() {
        init_test_schema_if_needed();
        let payload = create_test_payload("query { streams(first: 1, block: { number: 100 }) { id } }");
        match convert_subgraph_to_hyperindex(&payload, None) {
            Err(ConversionError::UnsupportedBlockConstraint(_)) => {}
            other => panic!("expected UnsupportedBlockConstraint, got {other:?}"),
        }
    }

    #[test]
    fn comments_in_a_root_argument_list_are_stripped() {
        init_test_schema_if_needed();
        // A comment runs to the end of the line, and the arguments are re-emitted
        // on one line, so a comment left in place commented out the rest of them
        // and the closing paren with it. A `)` inside one was worse: it closed the
        // argument list early and the whole operation came out empty - a 200 with
        // no rows rather than an error.
        for query in [
            "{\n  streams(\n    first: 1   # a plain note\n  ) { id }\n}",
            "{\n  streams(first: 1 # trailing ) note\n  ) { id }\n}",
            "{\n  streams(\n    first: 1   # note with ( and \" inside\n    skip: 2\n  ) { id }\n}",
        ] {
            let result = convert_subgraph_to_hyperindex(&create_test_payload(query), None)
                .unwrap_or_else(|e| panic!("expected {query:?} to convert, got {e:?}"));
            let converted = result.query["query"].as_str().unwrap();
            assert!(converted.contains("Stream(limit: 1"), "{converted}");
            assert!(converted.contains("id"), "selection set lost: {converted}");
            assert!(!converted.contains('#'), "comment survived: {converted}");
        }
    }

    #[test]
    fn a_block_argument_mentioned_in_a_comment_is_not_time_travel() {
        init_test_schema_if_needed();
        // The guard reads the raw argument text, so without comment stripping a
        // note mentioning `block:` got a valid query rejected.
        let query = "{\n  streams(\n    first: 1   # block: 5 is the cutoff\n  ) { id }\n}";
        let result = convert_subgraph_to_hyperindex(&create_test_payload(query), None).unwrap();
        let converted = result.query["query"].as_str().unwrap();
        assert!(converted.contains("Stream(limit: 1, order_by: {id: asc})"), "{converted}");
    }

    #[test]
    fn a_paren_or_hash_inside_a_string_argument_is_not_syntax() {
        init_test_schema_if_needed();
        // Neither ends the argument list: one used to close it early, the other
        // would start a comment that swallowed the rest.
        let result = convert_subgraph_to_hyperindex(
            &create_test_payload("{ streams(where: { name: \"a)b#c\" }) { id } }"),
            None,
        )
        .unwrap();
        let converted = result.query["query"].as_str().unwrap();
        assert!(
            converted.contains("where: {name: {_eq: \"a)b#c\"}}"),
            "{converted}"
        );
    }

    #[test]
    fn strip_line_comments_keeps_strings_and_separators() {
        assert_eq!(
            strip_line_comments("first: 1 # note\n  skip: 2"),
            "first: 1 \n  skip: 2"
        );
        assert_eq!(strip_line_comments("name: \"a#b\""), "name: \"a#b\"");
        // An escaped quote does not end the string, so the `#` after it is still
        // inside one.
        assert_eq!(
            strip_line_comments("name: \"a\\\"#b\""),
            "name: \"a\\\"#b\""
        );
        // A comment with no trailing newline runs to the end.
        assert_eq!(strip_line_comments("first: 1 # the end"), "first: 1 ");
    }

    #[test]
    fn block_argument_is_rejected_when_the_arguments_have_no_commas() {
        init_test_schema_if_needed();
        // Commas are insignificant in GraphQL, and leaving them out of a
        // multi-line argument list is how prettier prints a query. The scanner
        // this replaced accumulated identifier characters across the whitespace,
        // so the candidate reaching the colon was "1block" and the guard never
        // fired - the exact silent wrong-filter case it exists to stop.
        for query in [
            "query { streams(first: 1\n    block: { number: 100 }) { id } }",
            "query { streams(first: 1 block: { number: 100 }) { id } }",
            "query {\n  streams(\n    first: 1\n    block: { number: 100 }\n  ) { id }\n}",
        ] {
            let payload = create_test_payload(query);
            match convert_subgraph_to_hyperindex(&payload, None) {
                Err(ConversionError::UnsupportedBlockConstraint(_)) => {}
                other => panic!("expected UnsupportedBlockConstraint for {query:?}, got {other:?}"),
            }
        }
    }

    #[test]
    fn block_argument_is_rejected_when_it_follows_a_variable() {
        init_test_schema_if_needed();
        // `$f` is not an identifier character, so the old scanner restarted on
        // `f` and compared "fblock".
        let payload = create_test_payload(
            "query ($f: Int) { streams(first: $f\n    block: { number: 100 }) { id } }",
        );
        match convert_subgraph_to_hyperindex(&payload, None) {
            Err(ConversionError::UnsupportedBlockConstraint(_)) => {}
            other => panic!("expected UnsupportedBlockConstraint, got {other:?}"),
        }
    }

    #[test]
    fn block_hash_is_rejected_like_block_number() {
        init_test_schema_if_needed();
        for query in [
            "query { streams(block: { hash: \"0xabc\" }) { id } }",
            // Mixed with a freshness guard it is still pinned to one block.
            "query { streams(block: { number: 100, number_gte: 5 }) { id } }",
            // An unrecognized key could be a stricter constraint than it looks.
            "query { streams(block: { number_lte: 100 }) { id } }",
            "query { streams(block: {}) { id } }",
        ] {
            let payload = create_test_payload(query);
            match convert_subgraph_to_hyperindex(&payload, None) {
                Err(ConversionError::UnsupportedBlockConstraint(_)) => {}
                other => panic!("expected UnsupportedBlockConstraint for {query:?}, got {other:?}"),
            }
        }
    }

    #[test]
    fn block_number_gte_is_served_at_the_latest_block() {
        init_test_schema_if_needed();
        // `number_gte` is graph-node's "answer at a block at least N" freshness
        // guard, which clients send for read-after-write - not time travel. It is
        // answered from the latest state, once the indexer has reached N.
        for query in [
            "query { streams(first: 1, block: { number_gte: 100 }) { id } }",
            "query { streams(block: { number_gte: 100 }, first: 1) { id } }",
            "query { streams(first: 1\n    block: { number_gte: 100 }) { id } }",
        ] {
            let payload = create_test_payload(query);
            let result = convert_subgraph_to_hyperindex(&payload, None)
                .unwrap_or_else(|e| panic!("expected {query:?} to convert, got {e:?}"));
            let converted = result.query["query"].as_str().unwrap();
            assert!(converted.contains("Stream(limit: 1, order_by: {id: asc})"), "{converted}");
            // The argument must not survive as a filter on a `block` column.
            assert!(!converted.contains("block:"), "{converted}");
            assert_eq!(result.min_block, Some(100));
        }
    }

    #[test]
    fn a_where_filter_on_a_block_field_is_left_alone() {
        init_test_schema_if_needed();
        // `where: {block: {...}}` is a filter on the entity's own `block` field,
        // not time travel. `parse_single_param` hoists it to the same
        // `block.number` key the time-travel argument produces, which is why the
        // guard reads the raw argument text and why a dropped `number_gte` is
        // excised by span rather than by key.
        let payload =
            create_test_payload("query { streams(where: { block: { number: 100 } }) { id } }");
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let converted = result.query["query"].as_str().unwrap();
        assert!(
            converted.contains("where: {block: {number: {_eq: 100}}}"),
            "{converted}"
        );

        // Both at once: the guard goes, the filter stays.
        let payload = create_test_payload(
            "query { streams(block: { number_gte: 7 }\n  where: { block: { number: 100 } }) { id } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let converted = result.query["query"].as_str().unwrap();
        assert!(
            converted.contains("where: {block: {number: {_eq: 100}}}"),
            "{converted}"
        );
    }

    #[test]
    fn top_level_arguments_reads_one_value_at_a_time() {
        // A value has to be consumed whole, or the scan resumes inside it and a
        // later argument name is read out of a nested object.
        let args = top_level_arguments(
            "first: 10 where: { or: [{ a: [1, 2] }, { b: \"}, block: 1)\" }] } block: { number: 3 }",
        );
        let names: Vec<&str> = args.iter().map(|a| a.name).collect();
        assert_eq!(names, vec!["first", "where", "block"]);
        assert_eq!(args[0].value, "10");
        assert_eq!(args[2].value, "{ number: 3 }");

        // A string value can hold anything, including an unbalanced brace.
        let args = top_level_arguments("id: \"a: {b\" block: { number: 1 }");
        assert_eq!(
            args.iter().map(|a| a.name).collect::<Vec<_>>(),
            vec!["id", "block"]
        );

        // Escaped quotes do not end the string early.
        let args = top_level_arguments("id: \"a\\\"b\" first: 2");
        assert_eq!(
            args.iter().map(|a| a.name).collect::<Vec<_>>(),
            vec!["id", "first"]
        );

        // An unexpected token is skipped, not treated as the end of the list.
        let args = top_level_arguments("@ 7 block: { number: 1 }");
        assert_eq!(args.iter().map(|a| a.name).collect::<Vec<_>>(), vec!["block"]);

        // A truncated value cannot loop forever.
        assert_eq!(
            top_level_arguments("block: { number: 1")
                .iter()
                .map(|a| a.name)
                .collect::<Vec<_>>(),
            vec!["block"]
        );
    }

    #[test]
    fn block_in_a_nested_object_is_not_a_top_level_argument() {
        // The whole point of reading raw text: `block` nested inside another
        // argument's value is not the block argument.
        let block_of = |args: &str| {
            let roots = extract_multiple_entities(&format!("{{ streams({}) {{ id }} }}", args)).unwrap();
            roots.into_iter().next().unwrap().block_argument
        };
        assert_eq!(block_of("first: 1"), None);
        assert_eq!(block_of("where: { block: { number: 1 } }"), None);
        assert_eq!(block_of("where: { block_gt: 1 } blocking: true"), None);
        assert_eq!(block_of("block: { number: 1 }").as_deref(), Some("{ number: 1 }"));
        assert_eq!(block_of("first: 2 block: $b").as_deref(), Some("$b"));
    }

    #[test]
    fn entity_resolution_prefers_the_most_specific_spelling() {
        // The four ranks the single pass collects, in order: exact plural, exact
        // singular, then both case-insensitively. graph-node lowercases the whole
        // leading run of capitals, so `ATokenBalanceHistoryItem` is queried as
        // `atokenBalanceHistoryItems`.
        init_test_schema_if_needed();
        for (field, entity) in [
            ("reserves", "Reserve"),
            ("reserve", "Reserve"),
            ("Reserves", "Reserve"),
            ("RESERVE", "Reserve"),
            ("atokenBalanceHistoryItems", "ATokenBalanceHistoryItem"),
            ("atokenBalanceHistoryItem", "ATokenBalanceHistoryItem"),
            ("emodeCategories", "EModeCategory"),
            ("userTransactions", "UserTransaction"),
        ] {
            assert_eq!(
                schema::resolve_entity_for_collection_field(field).as_deref(),
                Some(entity),
                "{field}"
            );
        }
        assert_eq!(
            schema::resolve_entity_for_collection_field("widgets"),
            None,
            "an unknown field must fall through to the lexical fallback"
        );
    }

    #[test]
    fn test_singularize_and_capitalize() {
        assert_eq!(singularize_and_capitalize("streams"), "Stream");
        assert_eq!(singularize_and_capitalize("users"), "User");
        assert_eq!(singularize_and_capitalize("stream"), "Stream");
        assert_eq!(singularize_and_capitalize("user"), "User");
        assert_eq!(singularize_and_capitalize("purchases"), "Purchase");
        assert_eq!(singularize_and_capitalize("addresses"), "Address");
    }

    #[test]
    fn test_basic_collection_query_no_chain_id() {
        let payload = create_test_payload("query { streams(first: 10, skip: 0) { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(limit: 10, offset: 0, order_by: {id: asc}) {\n    id name\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_single_entity_query_no_chain_id() {
        let payload = create_test_payload("query { stream(id: \"123\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let expected = json!({
            "query": "query {\n  Stream_by_pk(id: \"123\") {\n    id name\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_equality_filter_no_chain_id() {
        let payload = create_test_payload("query { streams(name: \"test\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(limit: 100, order_by: {id: asc}, where: {name: {_eq: \"test\"}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_non_stream_entity_no_chain_id() {
        let payload = create_test_payload("query { users(name_contains: \"john\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let expected = json!({
            "query": "query {\n  User(limit: 100, order_by: {id: asc}, where: {name: {_like: \"%john%\"}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_different_chain_id() {
        let payload = create_test_payload("query { streams(name: \"test\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("5")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(limit: 100, order_by: {id: asc}, where: {chainId: {_eq: \"5\"}, name: {_eq: \"test\"}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_where_clause_with_multiple_filters() {
        let payload = create_test_payload(
            "query { streams(where: {alias_contains: \"113\", chainId: \"1\"}) { id alias } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let query = result.query["query"].as_str().unwrap();
        println!("Converted query: {}", query);

        // Check that both filters are included
        assert!(query.contains("alias: {_like: \"%113%\"}"));
        assert!(query.contains("chainId: {_eq: \"1\"}"));
        assert!(query.contains("Stream"));
    }

    #[test]
    fn test_where_clause_single_filter() {
        let payload =
            create_test_payload("query { streams(where: {alias_contains: \"113\"}) { id alias } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let query = result.query["query"].as_str().unwrap();
        println!("Converted query: {}", query);

        // Check that the filter is included
        assert!(query.contains("alias: {_like: \"%113%\"}"));
        assert!(query.contains("chainId: {_eq: \"1\"}"));
        assert!(query.contains("Stream"));
    }

    #[test]
    fn test_named_query_with_fragments_after_operation() {
        let payload = create_test_payload(
            "query GetActions { actions { ...ActionFragment } }\nfragment ContractFragment on Contract { id address category version }\nfragment ActionFragment on Action { id chainId stream { id } category hash block timestamp from addressA addressB amountA amountB contract { ...ContractFragment } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let query = result.query["query"].as_str().unwrap();
        // Fragments should be preserved and appear in the final query
        assert!(query.contains("fragment ContractFragment on Contract"));
        assert!(query.contains("fragment ActionFragment on Action"));
        // The converted main query should target Action with chainId filter
        assert!(query.contains("Action("));
        assert!(query.contains("where: {chainId: {_eq: \"1\"}}"));
        // The selection should still reference the fragment
        assert!(query.contains("...ActionFragment"));
    }

    #[test]
    fn test_single_line_query_with_fragments() {
        let payload = create_test_payload(
            "query GetActions { actions { ...ActionFragment } } fragment ContractFragment on Contract { id address category version } fragment ActionFragment on Action { id chainId stream { id } category hash block timestamp from addressA addressB amountA amountB contract { ...ContractFragment } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(query.contains("fragment ContractFragment on Contract"));
        assert!(query.contains("fragment ActionFragment on Action"));
        assert!(query.contains("Action("));
        assert!(query.contains("where: {chainId: {_eq: \"1\"}}"));
        assert!(query.contains("...ActionFragment"));
    }

    #[test]
    fn test_batches_pluralization_with_fragment() {
        let payload = create_test_payload(
            "query GetBatches { batches { ...BatchFragment } } fragment BatchFragment on Batch { id label size }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let query = result.query["query"].as_str().unwrap();
        // Should singularize to Batch and include chainId where
        assert!(query.contains("fragment BatchFragment on Batch"));
        assert!(query.contains("Batch("));
        assert!(query.contains("where: {chainId: {_eq: \"1\"}}"));
        assert!(query.contains("...BatchFragment"));
    }

    #[test]
    fn test_tranches_pluralization_with_fragment() {
        let payload = create_test_payload(
            "query GetTranches { tranches { ...TrancheFragment } } fragment TrancheFragment on Tranche { id position amount timestamp endTime startTime startAmount endAmount }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let query = result.query["query"].as_str().unwrap();
        // Should singularize to Tranche and include chainId where
        assert!(query.contains("fragment TrancheFragment on Tranche"));
        assert!(query.contains("Tranche("));
        assert!(query.contains("where: {chainId: {_eq: \"1\"}}"));
        assert!(query.contains("...TrancheFragment"));
    }

    #[test]
    fn test_boolean_filter_in_where_clause() {
        // Test case for boolean filters in where clause (e.g., isOpen: true)
        // This should be converted to isOpen: { _eq: true } format
        let payload = create_test_payload(
            "query Trades { trades(first: 10000, where: { isOpen: true }) { id trader isOpen } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let query = result.query["query"].as_str().unwrap();
        println!("Converted query: {}", query);

        // Check that the boolean filter is properly converted to Hasura format
        assert!(
            query.contains("isOpen: {_eq: true}"),
            "Expected isOpen: {{_eq: true}} in converted query, got: {}",
            query
        );
        // Check that Trade entity is used (singularized from trades)
        assert!(query.contains("Trade("));
        // Check that chainId is added when provided
        assert!(query.contains("chainId: {_eq: \"1\"}"));
    }

    #[test]
    fn test_boolean_filter_multiline_query_format() {
        // Test case matching the exact failing query format with multiline structure
        // This test reproduces the bug where boolean filters in where clauses are not
        // properly converted to Hasura format when parameters are separated by newlines.
        // Expected error: "expected an object for type 'Boolean_comparison_exp', but found a boolean"
        //
        // Note: This bug specifically affects the DEFAULT case (no suffix) which should use _eq.
        // Boolean operators with explicit suffixes already work correctly:
        // - _neq (via _not suffix): isOpen_not: false → isOpen: {_neq: false} ✓ Works
        // - _in: isOpen_in: [true, false] → isOpen: {_in: [true, false]} ✓ Works
        // - _nin: isOpen_not_in: [true] → isOpen: {_nin: [true]} ✓ Works
        // - _eq (default, no suffix): isOpen: true → isOpen: {_eq: true} ✗ BUG: Affected
        //
        // Note: Operators like _gt, _lt, _gte, _lte, _ilike, _contains don't apply to booleans
        // in Hasura (they're for numeric/string fields). For booleans, only _eq, _neq, _in, _nin are valid.
        let query = r#"query Trades {
                                        trades(
                                            first: 10000
                                            where: {
                                            isOpen: true
                                            }
                                        ) {
                                            id
                                            trader
                                            isOpen
                                        }
                                        }"#;
        let payload = create_test_payload(query);
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let converted_query = result.query["query"].as_str().unwrap();
        println!("Converted query: {}", converted_query);

        // Check that the boolean filter is properly converted to Hasura format
        // The incorrect format "isOpen: true" would cause Hyperindex to reject the query
        assert!(
            converted_query.contains("isOpen: {_eq: true}"),
            "Expected isOpen: {{_eq: true}} in converted query.\n\
             The incorrect format 'isOpen: true' would cause Hyperindex error:\n\
             'expected an object for type Boolean_comparison_exp, but found a boolean'.\n\
             Converted query: {}",
            converted_query
        );
        // Check that Trade entity is used (singularized from trades)
        assert!(converted_query.contains("Trade("));
    }

    #[test]
    fn test_boolean_filter_not_operator_multiline() {
        // Test boolean _neq operator (via _not suffix) in multiline format
        let query = r#"query {
  trades(
    where: {
      isOpen_not: false
    }
  ) {
    id
    isOpen
  }
}"#;
        let payload = create_test_payload(query);
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let converted_query = result.query["query"].as_str().unwrap();
        println!("Converted query: {}", converted_query);

        // Check that _neq is properly formatted (this should work since it has a suffix)
        assert!(
            converted_query.contains("isOpen: {_neq: false}"),
            "Expected isOpen: {{_neq: false}} in converted query, got: {}",
            converted_query
        );
    }

    #[test]
    fn test_boolean_filter_in_operator_multiline() {
        // Test boolean _in operator in multiline format
        let query = r#"query {
                                trades(
                                    where: {
                                    isOpen_in: [true, false]
                                    }
                                ) {
                                    id
                                    isOpen
                                }
                                }"#;
        let payload = create_test_payload(query);
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let converted_query = result.query["query"].as_str().unwrap();
        println!("Converted query: {}", converted_query);

        // Check that _in is properly formatted (this should work since it has a suffix)
        assert!(
            converted_query.contains("isOpen: {_in: [true, false]}"),
            "Expected isOpen: {{_in: [true, false]}} in converted query, got: {}",
            converted_query
        );
    }

    #[test]
    fn test_boolean_filter_false_in_where_clause() {
        // Test case for boolean false filters in where clause
        let payload =
            create_test_payload("query { streams(where: { isOpen: false }) { id isOpen } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let query = result.query["query"].as_str().unwrap();
        println!("Converted query: {}", query);

        // Check that the boolean filter is properly converted to Hasura format
        assert!(
            query.contains("isOpen: {_eq: false}"),
            "Expected isOpen: {{_eq: false}} in converted query, got: {}",
            query
        );
    }

    #[test]
    fn test_boolean_filter_with_other_filters() {
        // Test case for boolean filter combined with other filters
        let payload = create_test_payload(
            "query { trades(where: { isOpen: true, trader: \"0x123\" }) { id trader isOpen } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let query = result.query["query"].as_str().unwrap();
        println!("Converted query: {}", query);

        // Check that both filters are properly converted
        assert!(
            query.contains("isOpen: {_eq: true}"),
            "Expected isOpen: {{_eq: true}} in converted query"
        );
        assert!(
            query.contains("trader: {_eq: \"0x123\"}"),
            "Expected trader: {{_eq: \"0x123\"}} in converted query"
        );
    }

    #[test]
    fn test_numeric_operators_multiline_format() {
        // Test that numeric operators (_gt, _gte, _lt, _lte) work in multiline format
        // This verifies that operators with suffixes are handled correctly
        let query = r#"query {
  streams(
    where: {
      amount_gt: 100
      amount_lte: 1000
    }
  ) {
    id
    amount
  }
}"#;
        let payload = create_test_payload(query);
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let converted_query = result.query["query"].as_str().unwrap();
        println!("Converted query: {}", converted_query);

        // Check that both operators are properly converted
        assert!(
            converted_query.contains("amount: {_gt: 100}"),
            "Expected amount: {{_gt: 100}} in converted query, got: {}",
            converted_query
        );
        assert!(
            converted_query.contains("amount: {_lte: 1000}"),
            "Expected amount: {{_lte: 1000}} in converted query, got: {}",
            converted_query
        );
    }

    #[test]
    fn test_nested_entity_reference_in_where_clause() {
        init_test_schema_if_needed();
        // Test case for nested entity references in where clauses
        // In subgraph format, you can reference a nested entity directly: where: { pair: "0" }
        // In Envio/Hyperindex format, this must be converted to: where: { pair: {id: {_eq: "0"}} }
        //
        // This approach (nested structure) is better than pair_id because:
        // 1. It matches the error message: "field '_eq' not found in type: 'Pair_bool_exp'"
        //    (suggesting pair expects a Pair_bool_exp object, not a direct value)
        // 2. It's more flexible - can handle filtering on other fields: pair: {name: {_eq: "ETH"}}
        // 3. It can handle multiple conditions: pair: {id: {_eq: "0"}, name: {_contains: "ETH"}}
        // 4. It matches the GraphQL/Hasura pattern for nested entity filters
        //
        // The challenge is detecting when a field is a nested entity reference vs a regular field
        // This test matches the actual failing query where 'pair' is NOT in the selection set
        let query = r#"query Trades {
  trades(
    first: 10000
    where: {
      pair: "0"
    }
  ) {
    id
    trader
    index
    tradeID
    tradeType
    openPrice
    closePrice
    takeProfitPrice
    stopLossPrice
    collateral
    notional
    tradeNotional
    highestLeverage
    leverage
    isBuy
    isOpen
    closeInitiated
    funding
    rollover
    timestamp
  }
}"#;
        let payload = create_test_payload(query);
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let converted_query = result.query["query"].as_str().unwrap();
        println!("Converted query: {}", converted_query);

        // Check that the nested entity reference is converted to nested structure
        // The incorrect format "pair: {_eq: \"0\"}" would cause Hyperindex error:
        // "field '_eq' not found in type: 'Pair_bool_exp'"
        // The correct format should be: "pair: {id: {_eq: \"0\"}}"
        assert!(
            converted_query.contains("pair: {id: {_eq: \"0\"}}"),
            "Expected pair: {{id: {{_eq: \"0\"}}}} in converted query.\n\
             The incorrect format 'pair: {{_eq: \"0\"}}' would cause Hyperindex error:\n\
             'field '_eq' not found in type: 'Pair_bool_exp''.\n\
             Converted query: {}",
            converted_query
        );

        // Should NOT contain the incorrect format (direct _eq on pair)
        let incorrect_pattern = "pair: {_eq:";
        assert!(
            !converted_query.contains(incorrect_pattern),
            "Converted query should not contain 'pair: {{_eq:' in where clause.\n\
             It should be 'pair: {{id: {{_eq:' instead.\n\
             Converted query: {}",
            converted_query
        );
    }

    #[test]
    fn test_nested_entity_reference_with_other_filters() {
        init_test_schema_if_needed();
        // Test nested entity reference combined with other filters
        // Uses schema to detect that pair is a nested entity
        let query = r#"query {
  trades(
    where: {
      pair: "0"
      isOpen: true
    }
  ) {
    id
    isOpen
    pair {
      id
    }
  }
}"#;
        let payload = create_test_payload(query);
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let converted_query = result.query["query"].as_str().unwrap();
        println!("Converted query: {}", converted_query);

        // Both filters should be properly converted
        assert!(
            converted_query.contains("pair: {id: {_eq: \"0\"}}"),
            "Expected pair: {{id: {{_eq: \"0\"}}}} in converted query, got: {}",
            converted_query
        );
        assert!(
            converted_query.contains("isOpen: {_eq: true}"),
            "Expected isOpen: {{_eq: true}} in converted query, got: {}",
            converted_query
        );
    }

    #[test]
    fn test_nested_entity_reference_with_operators() {
        init_test_schema_if_needed();
        // Test nested entity reference with comparison operators
        // Uses schema to detect that pair is a nested entity
        let query = r#"query {
  trades(
    where: {
      pair: "0"
      amount_gt: 100
    }
  ) {
    id
    amount
    pair {
      id
    }
  }
}"#;
        let payload = create_test_payload(query);
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let converted_query = result.query["query"].as_str().unwrap();
        println!("Converted query: {}", converted_query);

        // Nested entity reference should use nested structure
        assert!(
            converted_query.contains("pair: {id: {_eq: \"0\"}}"),
            "Expected pair: {{id: {{_eq: \"0\"}}}} in converted query, got: {}",
            converted_query
        );
        // Regular field with operator should work normally
        assert!(
            converted_query.contains("amount: {_gt: 100}"),
            "Expected amount: {{_gt: 100}} in converted query, got: {}",
            converted_query
        );
    }

    #[test]
    fn test_nested_entity_reference_with_nested_field_filter() {
        init_test_schema_if_needed();
        // Test that the nested structure approach allows filtering on other fields of the nested entity
        // This demonstrates why the nested structure is more flexible than _id suffix
        // Example: pair: {name: {_eq: "ETH"}} or pair: {symbol: {_contains: "USD"}}
        let query = r#"query {
  trades(
    where: {
      pair: {
        name: "ETH"
      }
    }
  ) {
    id
    pair {
      name
    }
  }
}"#;
        let payload = create_test_payload(query);
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let converted_query = result.query["query"].as_str().unwrap();
        println!("Converted query: {}", converted_query);

        // When the subgraph already uses nested structure, it should be preserved/converted correctly
        // pair: {name: "ETH"} should become pair: {name: {_eq: "ETH"}}
        assert!(
            converted_query.contains("pair: {name: {_eq: \"ETH\"}}"),
            "Expected pair: {{name: {{_eq: \"ETH\"}}}} in converted query, got: {}",
            converted_query
        );
    }

    #[test]
    fn test_deeply_nested_entity_reference() {
        init_test_schema_if_needed();
        // Test deeply nested entity reference: pair.token: "0"
        // where token is a nested entity within pair
        // Should convert to: pair: {token: {id: {_eq: "0"}}}
        let query = r#"query {
  trades(
    where: {
      pair: {
        token: "0"
      }
    }
  ) {
    id
    pair {
      token {
        id
      }
    }
  }
}"#;
        let payload = create_test_payload(query);
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let converted_query = result.query["query"].as_str().unwrap();
        println!("Converted query: {}", converted_query);

        // Deeply nested entity reference should use nested structure
        assert!(
            converted_query.contains("pair: {token: {id: {_eq: \"0\"}}}"),
            "Expected pair: {{token: {{id: {{_eq: \"0\"}}}}}} in converted query, got: {}",
            converted_query
        );
    }

    #[test]
    fn test_deeply_nested_regular_field() {
        init_test_schema_if_needed();
        // Test deeply nested regular field: pair.token.amount: "0"
        // where token is a nested entity within pair, and amount is a regular field within token
        // Should convert to: pair: {token: {amount: {_eq: "0"}}}
        let query = r#"query {
  trades(
    where: {
      pair: {
        token: {
          amount: "0"
        }
      }
    }
  ) {
    id
    pair {
      token {
        id
        amount
      }
    }
  }
}"#;
        let payload = create_test_payload(query);
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let converted_query = result.query["query"].as_str().unwrap();
        println!("Converted query: {}", converted_query);

        // Deeply nested regular field should use nested structure without id wrapper
        assert!(
            converted_query.contains("pair: {token: {amount: {_eq: \"0\"}}}"),
            "Expected pair: {{token: {{amount: {{_eq: \"0\"}}}}}} in converted query, got: {}",
            converted_query
        );
        // Should NOT have id wrapper for regular fields
        assert!(
            !converted_query.contains("pair: {token: {amount: {id: {_eq: \"0\"}}}}"),
            "Should NOT have id wrapper for regular field 'amount', got: {}",
            converted_query
        );
    }

    #[test]
    fn test_nested_entity_with_non_id_field() {
        init_test_schema_if_needed();
        // Test case: pair: {token: {name: "ETH"}}
        // where token is a nested entity, but we're filtering by 'name' (not the default 'id')
        // Should convert to: pair: {token: {name: {_eq: "ETH"}}}
        // NOT: pair: {token: {name: {id: {_eq: "ETH"}}}} (wrong - name is a regular field)
        let query = r#"query {
  trades(
    where: {
      pair: {
        token: {
          name: "ETH"
        }
      }
    }
  ) {
    id
    pair {
      token {
        id
        name
      }
    }
  }
}"#;
        let payload = create_test_payload(query);
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let converted_query = result.query["query"].as_str().unwrap();
        println!("Converted query: {}", converted_query);

        // Should correctly convert to nested structure with name as regular field
        assert!(
            converted_query.contains("pair: {token: {name: {_eq: \"ETH\"}}}"),
            "Expected pair: {{token: {{name: {{_eq: \"ETH\"}}}}}} in converted query, got: {}",
            converted_query
        );
        // Should NOT incorrectly wrap name with id
        assert!(
            !converted_query.contains("pair: {token: {name: {id: {_eq: \"ETH\"}}}}"),
            "Should NOT have id wrapper for regular field 'name' within token, got: {}",
            converted_query
        );
    }

    #[test]
    fn test_regular_field_in_selection() {
        init_test_schema_if_needed();
        // With schema-based approach, "token" is defined as a regular field in the schema for Trade
        // so it should be treated as regular (not nested)
        let query = r#"query {
  trades(
    where: {
      token: "0"
    }
  ) {
    id
    token
  }
}"#;
        let payload = create_test_payload(query);
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let converted_query = result.query["query"].as_str().unwrap();

        // Since "token" is explicitly in the selection as a regular field, it should be treated as regular
        assert!(
            converted_query.contains("token: {_eq: \"0\"}"),
            "Expected token: {{_eq: \"0\"}} (regular field) in converted query, got: {}",
            converted_query
        );
        // Should NOT have id wrapper since it's a regular field
        assert!(
            !converted_query.contains("token: {id: {_eq: \"0\"}}"),
            "Should NOT have id wrapper for regular field 'token', got: {}",
            converted_query
        );
    }

    // Tests for capitalization of _by_pk queries
    #[test]
    fn test_single_entity_by_pk_capitalization() {
        // Test that single entity queries use capitalized entity name for _by_pk
        // Bug: pair(id: "0") was converting to pair_by_pk instead of Pair_by_pk
        let payload = create_test_payload("query Pair { pair(id: \"0\") { id feed } }");
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let converted_query = result.query["query"].as_str().unwrap();
        println!("Converted query: {}", converted_query);

        // Should use capitalized entity name: Pair_by_pk not pair_by_pk
        assert!(
            converted_query.contains("Pair_by_pk(id: \"0\")"),
            "Expected Pair_by_pk (capitalized) in converted query, got: {}",
            converted_query
        );

        // Should NOT contain lowercase version
        assert!(
            !converted_query.contains("pair_by_pk"),
            "Should NOT contain lowercase pair_by_pk in converted query, got: {}",
            converted_query
        );
    }

    #[test]
    fn test_single_entity_by_pk_capitalization_stream() {
        // Test with stream entity
        let payload = create_test_payload("query { stream(id: \"123\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let converted_query = result.query["query"].as_str().unwrap();

        // Should use capitalized entity name: Stream_by_pk not stream_by_pk
        assert!(
            converted_query.contains("Stream_by_pk(id: \"123\")"),
            "Expected Stream_by_pk (capitalized) in converted query, got: {}",
            converted_query
        );

        // Should NOT contain lowercase version
        assert!(
            !converted_query.contains("stream_by_pk"),
            "Should NOT contain lowercase stream_by_pk in converted query, got: {}",
            converted_query
        );
    }

    #[test]
    fn test_single_entity_by_pk_capitalization_user() {
        // Test with user entity (ends with 'r' not 's')
        let payload = create_test_payload("query { user(id: \"0x123\") { id address } }");
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let converted_query = result.query["query"].as_str().unwrap();

        // Should use capitalized entity name: User_by_pk not user_by_pk
        assert!(
            converted_query.contains("User_by_pk(id: \"0x123\")"),
            "Expected User_by_pk (capitalized) in converted query, got: {}",
            converted_query
        );

        // Should NOT contain lowercase version
        assert!(
            !converted_query.contains("user_by_pk"),
            "Should NOT contain lowercase user_by_pk in converted query, got: {}",
            converted_query
        );
    }

    #[test]
    fn test_single_entity_by_pk_capitalization_batch() {
        // Test with batch entity (ends with 'ch')
        let payload = create_test_payload("query { batch(id: \"456\") { id label } }");
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let converted_query = result.query["query"].as_str().unwrap();

        // Should use capitalized entity name: Batch_by_pk not batch_by_pk
        assert!(
            converted_query.contains("Batch_by_pk(id: \"456\")"),
            "Expected Batch_by_pk (capitalized) in converted query, got: {}",
            converted_query
        );

        // Should NOT contain lowercase version
        assert!(
            !converted_query.contains("batch_by_pk"),
            "Should NOT contain lowercase batch_by_pk in converted query, got: {}",
            converted_query
        );
    }

    #[test]
    fn test_collection_query_maintains_capitalization() {
        // Verify that collection queries (plural) still use capitalized entity names
        let payload = create_test_payload("query { pairs(first: 10) { id feed } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let converted_query = result.query["query"].as_str().unwrap();

        // Should use capitalized singular entity name for collection: Pair not pair
        assert!(
            converted_query.contains("Pair("),
            "Expected Pair( (capitalized) in converted query, got: {}",
            converted_query
        );
    }

    #[test]
    fn test_user_reported_bug_pair_by_pk() {
        // This test reproduces the exact bug reported by the user:
        // Original query: query Pair { pair(id: "0") { id feed } }
        // Was converting to: pair_by_pk(id: "0") { id feed }
        // Should convert to: Pair_by_pk(id: "0") { id feed }
        //
        // The error was: "field 'pair_by_pk' not found in type: 'query_root'"
        // Because the schema expects Pair_by_pk (capitalized)
        let payload = create_test_payload("query Pair { pair(id: \"0\") { id feed } }");
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let converted_query = result.query["query"].as_str().unwrap();
        println!("User reported bug - converted query: {}", converted_query);

        // Should use capitalized entity name: Pair_by_pk not pair_by_pk
        assert!(
            converted_query.contains("Pair_by_pk(id: \"0\")"),
            "Expected Pair_by_pk (capitalized) in converted query, got: {}",
            converted_query
        );

        // Should NOT contain the buggy lowercase version
        assert!(
            !converted_query.contains("pair_by_pk"),
            "Should NOT contain lowercase pair_by_pk in converted query, got: {}",
            converted_query
        );

        // Verify the full structure
        assert!(converted_query.contains("id"));
        assert!(converted_query.contains("feed"));
    }

    #[test]
    fn test_query_with_variables() {
        // Test that variables are passed through correctly
        let payload = json!({
            "query": "query FactoriesAndBundles($first: Int!) { factories(first: $first) { id poolCount } bundles(first: $first) { id nativePriceUSD } }",
            "variables": {
                "first": 5
            }
        });
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();

        // Verify query is converted
        assert!(result.query.get("query").is_some());
        let query = result.query["query"].as_str().unwrap();
        assert!(query.contains("Factory"));
        assert!(query.contains("Bundle"));

        // Verify variables are passed through
        assert!(result.query.get("variables").is_some());
        let variables = result.query.get("variables").unwrap();
        assert_eq!(variables["first"], 5);

        // Verify variable references in query are preserved (not converted to literals)
        assert!(
            query.contains("$first"),
            "Variable reference $first should be preserved in converted query"
        );
    }

    #[test]
    fn test_query_without_variables() {
        // Test that queries without variables still work
        let payload = json!({
            "query": "query { factories(first: 5) { id } }"
        });
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();

        // Should not have variables field
        assert!(result.query.get("variables").is_none());

        // Query should still be converted
        assert!(result.query.get("query").is_some());
    }

    #[test]
    fn test_query_with_variables_and_chain_id() {
        // Test variables work with chain_id
        let payload = json!({
            "query": "query GetTrades($limit: Int!) { trades(first: $limit) { id trader } }",
            "variables": {
                "limit": 100
            }
        });
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();

        // Verify variables are passed through
        assert_eq!(result.query["variables"]["limit"], 100);

        // Verify query includes chainId filter and preserves variable
        let query = result.query["query"].as_str().unwrap();
        assert!(query.contains("chainId: {_eq: \"1\"}"));
        assert!(
            query.contains("$limit"),
            "Variable reference $limit should be preserved"
        );
    }

    #[test]
    fn test_nested_entity_with_variable() {
        init_test_schema_if_needed();
        // Test that nested entity filters with variables are converted correctly
        // pair: $pairid should become pair: {id: {_eq: $pairid}}
        let payload = json!({
            "query": "query Trades($pairid: String!) { trades(where: { pair: $pairid }) { id pair { id } } }",
            "variables": {
                "pairid": "0"
            }
        });
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();

        let query = result.query["query"].as_str().unwrap();
        println!("Converted query: {}", query);

        // Verify variable definition is preserved in query header
        assert!(
            query.contains("query Trades($pairid: String!)"),
            "Variable definition should be preserved in query header, got: {}",
            query
        );

        // Verify nested entity variable is converted correctly
        assert!(
            query.contains("pair: {id: {_eq: $pairid}}"),
            "Expected pair: {{id: {{_eq: $pairid}}}} in converted query, got: {}",
            query
        );

        // Should NOT contain the incorrect format
        assert!(
            !query.contains("pair: {_eq: $pairid}"),
            "Should NOT contain 'pair: {{_eq: $pairid}}' (missing id wrapper), got: {}",
            query
        );

        // Verify variables are passed through
        assert_eq!(result.query["variables"]["pairid"], "0");
    }

    #[test]
    fn test_nested_entity_with_variable_deep_nesting() {
        init_test_schema_if_needed();
        // Test the actual query structure from the user's example
        // pair: $pairid should become pair: {id: {_eq: $pairid}} even with deep nesting
        let payload = json!({
            "query": "query Trades($pairid: String!) { trades(where: { pair: $pairid }) { id pair { fee { liqFeeP } } } }",
            "variables": {
                "pairid": "0"
            }
        });
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();

        let query = result.query["query"].as_str().unwrap();
        println!("Converted query: {}", query);

        // Verify variable definition is preserved in query header
        assert!(
            query.contains("query Trades($pairid: String!)"),
            "Variable definition should be preserved in query header, got: {}",
            query
        );

        // Verify nested entity variable is converted correctly
        assert!(
            query.contains("pair: {id: {_eq: $pairid}}"),
            "Expected pair: {{id: {{_eq: $pairid}}}} in converted query, got: {}",
            query
        );

        // Should NOT contain the incorrect format
        assert!(
            !query.contains("pair: {_eq: $pairid}"),
            "Should NOT contain 'pair: {{_eq: $pairid}}' (missing id wrapper), got: {}",
            query
        );

        // Verify variables are passed through
        assert_eq!(result.query["variables"]["pairid"], "0");
    }

    #[test]
    fn test_variable_with_object() {
        // Test that object variables are passed through correctly
        let payload = json!({
            "query": "query GetTrades($where: TradeWhereInput!) { trades(where: $where) { id trader } }",
            "variables": {
                "where": {
                    "isOpen": true,
                    "trader": "0x123"
                }
            }
        });
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();

        // Verify variables are passed through with object structure
        assert!(result.query.get("variables").is_some());
        let variables = result.query.get("variables").unwrap();
        assert_eq!(variables["where"]["isOpen"], true);
        assert_eq!(variables["where"]["trader"], "0x123");

        // Verify variable reference is preserved in query and where clause is included
        let query = result.query["query"].as_str().unwrap();
        assert!(
            query.contains("$where"),
            "Variable reference $where should be preserved"
        );
        assert!(
            query.contains("where: $where"),
            "Where clause with variable should be included in query"
        );
    }

    #[test]
    fn test_variable_with_array() {
        // Test that array variables are passed through correctly
        let payload = json!({
            "query": "query GetTrades($ids: [String!]!) { trades(where: { id_in: $ids }) { id trader } }",
            "variables": {
                "ids": ["0", "1", "2"]
            }
        });
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();

        // Verify variables are passed through with array structure
        assert!(result.query.get("variables").is_some());
        let variables = result.query.get("variables").unwrap();
        assert!(variables["ids"].is_array());
        assert_eq!(variables["ids"][0], "0");
        assert_eq!(variables["ids"][1], "1");
        assert_eq!(variables["ids"][2], "2");

        // Verify variable reference is preserved in query
        let query = result.query["query"].as_str().unwrap();
        assert!(
            query.contains("$ids"),
            "Variable reference $ids should be preserved"
        );
    }

    #[test]
    fn test_variable_with_nested_object() {
        // Test that nested object variables work
        let payload = json!({
            "query": "query GetTrades($filter: TradeFilterInput!) { trades(where: $filter) { id trader pair { id } } }",
            "variables": {
                "filter": {
                    "pair": {
                        "id": "0"
                    },
                    "isOpen": true
                }
            }
        });
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();

        // Verify nested object variables are passed through
        assert!(result.query.get("variables").is_some());
        let variables = result.query.get("variables").unwrap();
        assert_eq!(variables["filter"]["pair"]["id"], "0");
        assert_eq!(variables["filter"]["isOpen"], true);

        // Verify variable reference is preserved
        let query = result.query["query"].as_str().unwrap();
        assert!(
            query.contains("$filter"),
            "Variable reference $filter should be preserved"
        );
    }

    #[test]
    fn test_variable_with_mixed_types() {
        // Test variables with mixed types (scalar, object, array)
        let payload = json!({
            "query": "query GetTrades($limit: Int!, $where: TradeWhereInput!, $ids: [String!]) { trades(first: $limit, where: $where) { id trader } }",
            "variables": {
                "limit": 100,
                "where": {
                    "id_in": ["0", "1"],
                    "isOpen": true
                },
                "ids": ["0", "1", "2"]
            }
        });
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();

        // Verify all variable types are passed through correctly
        let variables = result.query.get("variables").unwrap();
        assert_eq!(variables["limit"], 100);
        assert_eq!(variables["where"]["isOpen"], true);
        assert!(variables["ids"].is_array());

        // Verify all variable references are preserved
        let query = result.query["query"].as_str().unwrap();
        assert!(query.contains("$limit"));
        assert!(query.contains("$where"));
    }

    #[test]
    fn test_variable_type_id_converted_to_string() {
        // ID! should be converted to String! in the header, but variable usage and body stay intact
        let payload = json!({
            "query": "query getUserVolume($id: ID!) { user(id: $id) { totalVolume __typename } }",
            "variables": {
                "id": "0xabc"
            }
        });
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();

        let query = result.query["query"].as_str().unwrap();
        // Header should now declare String!
        assert!(
            query.contains("query getUserVolume($id: String!)"),
            "Expected ID! to be converted to String! in header, got: {}",
            query
        );
        // Body should still refer to $id
        assert!(query.contains("User_by_pk(id: $id)"));
        // Variables should be passed through unchanged
        assert_eq!(result.query["variables"]["id"], "0xabc");
    }

    #[test]
    fn test_variable_type_bytes_converted_to_string() {
        // Bytes and Bytes! should be converted to String / String!
        let payload = json!({
            "query": "query GetSomething($data: Bytes!, $maybeData: Bytes) { user(id: \"x\") { id __typename } }",
            "variables": {
                "data": "0xdeadbeef",
                "maybeData": null
            }
        });
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();

        let query = result.query["query"].as_str().unwrap();
        assert!(
            query.contains("query GetSomething($data: String!, $maybeData: String)"),
            "Expected Bytes/Bytes! to be converted to String/String! in header, got: {}",
            query
        );
        // Variables still present
        assert_eq!(result.query["variables"]["data"], "0xdeadbeef");
    }

    #[test]
    fn test_variable_type_array_id_converted_to_string() {
        // Array ID types should also be converted
        let payload = json!({
            "query": "query GetUsers($ids: [ID!]!) { users(first: 10, where: { id_in: $ids }) { id } }",
            "variables": {
                "ids": ["0x1", "0x2"]
            }
        });
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();

        let query = result.query["query"].as_str().unwrap();
        assert!(
            query.contains("query GetUsers($ids: [String!]!)"),
            "Expected [ID!]! to be converted to [String!]! in header, got: {}",
            query
        );
        // Ensure the query name itself is untouched (no accidental ID -> String inside names)
        assert!(query.contains("query GetUsers("));
        // Variables should still be arrays
        let vars = &result.query["variables"]["ids"];
        assert!(vars.is_array());
        assert_eq!(vars[0], "0x1");
        assert_eq!(vars[1], "0x2");
    }

    #[test]
    fn test_variable_type_bigint_converted_to_numeric() {
        // BigInt! and BigInt should be converted to numeric!/numeric
        let payload = json!({
            "query": "query Limits($minLeverage: BigInt!, $maxLeverage: BigInt) { limits(where: { leverage_gte: $minLeverage, leverage_lte: $maxLeverage }) { id leverage } }",
            "variables": {
                "minLeverage": "5",
                "maxLeverage": "10"
            }
        });
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();

        let query = result.query["query"].as_str().unwrap();
        assert!(
            query.contains("query Limits($minLeverage: numeric!, $maxLeverage: numeric)"),
            "Expected BigInt/BigInt! to be converted to numeric/numeric! in header, got: {}",
            query
        );
        // Variables preserved
        assert_eq!(result.query["variables"]["minLeverage"], "5");
        assert_eq!(result.query["variables"]["maxLeverage"], "10");
    }

    #[test]
    fn test_variable_type_bigdecimal_converted_to_numeric() {
        // BigDecimal! and BigDecimal should be converted to numeric!/numeric
        let payload = json!({
            "query": "query Prices($price: BigDecimal!, $avgPrice: BigDecimal) { trades(where: { openPrice_gte: $price, openPrice_lte: $avgPrice }) { id openPrice } }",
            "variables": {
                "price": "1.23",
                "avgPrice": "4.56"
            }
        });
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();

        let query = result.query["query"].as_str().unwrap();
        assert!(
            query.contains("query Prices($price: numeric!, $avgPrice: numeric)"),
            "Expected BigDecimal/BigDecimal! to be converted to numeric/numeric! in header, got: {}",
            query
        );
        assert_eq!(result.query["variables"]["price"], "1.23");
        assert_eq!(result.query["variables"]["avgPrice"], "4.56");
    }

    #[test]
    fn test_variable_type_multiline_header() {
        // Test that multi-line variable definitions work correctly
        // This is the exact format from the user's failing query
        let payload = json!({
            "query": "query Limits(\n  $pair: String!\n  $isActive: Boolean!\n  $first: Int!\n  $minLeverage: BigInt\n) {\n  limits(first: $first, where: { pair: $pair, isActive: $isActive, leverage_gte: $minLeverage }) { id leverage } }",
            "variables": {
                "pair": "0",
                "isActive": true,
                "first": 100,
                "minLeverage": "5"
            }
        });
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();

        let query = result.query["query"].as_str().unwrap();
        // Should convert BigInt to numeric even with newlines
        assert!(
            query.contains("$minLeverage: numeric"),
            "Expected BigInt to be converted to numeric in multi-line header, got: {}",
            query
        );
        // Should NOT contain BigInt anymore
        assert!(
            !query.contains("$minLeverage: BigInt"),
            "Should not contain BigInt in converted query, got: {}",
            query
        );
        // Variables preserved
        assert_eq!(result.query["variables"]["minLeverage"], "5");
    }

    #[test]
    fn test_variable_type_enum_conversion() {
        init_test_schema_if_needed();
        // Test that String variables used with enum fields get converted to the enum type
        // This simulates the orderAction enum scenario
        let payload = json!({
            "query": "query ListOperations($operation: String) { orders(where: { orderAction: $operation }) { id trader } }",
            "variables": {
                "operation": "Open"
            }
        });
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();

        let query = result.query["query"].as_str().unwrap();
        println!("Converted query: {}", query);

        // Should convert String to orderaction (the enum type)
        assert!(
            query.contains("$operation: orderaction"),
            "Expected String to be converted to orderaction enum type, got: {}",
            query
        );
        // Should NOT contain String type for operation anymore
        assert!(
            !query.contains("$operation: String"),
            "Should not contain String type for operation in converted query, got: {}",
            query
        );
    }

    #[test]
    fn test_variable_type_enum_conversion_non_nullable() {
        init_test_schema_if_needed();
        // Test that non-nullable String! variables get converted to enum! (preserving nullability)
        let payload = json!({
            "query": "query ListOperations($operation: String!) { orders(where: { orderAction: $operation }) { id } }",
            "variables": {
                "operation": "Open"
            }
        });
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();

        let query = result.query["query"].as_str().unwrap();
        println!("Converted query: {}", query);

        // Should convert String! to orderaction! (preserving non-nullable)
        assert!(
            query.contains("$operation: orderaction!"),
            "Expected String! to be converted to orderaction! enum type, got: {}",
            query
        );
    }

    // ─────────────────────────────────────────────────────────────────────
    // Header rewrite: `*_filter` → `*_bool_exp`
    // ─────────────────────────────────────────────────────────────────────

    #[test]
    fn rewrite_filter_type_names_basic() {
        let h = "query Q($where: Trade_filter)";
        assert_eq!(
            rewrite_filter_type_names(h),
            "query Q($where: Trade_bool_exp)"
        );
    }

    #[test]
    fn rewrite_filter_type_names_non_null() {
        let h = "query Q($where: Trade_filter!)";
        assert_eq!(
            rewrite_filter_type_names(h),
            "query Q($where: Trade_bool_exp!)"
        );
    }

    #[test]
    fn rewrite_filter_type_names_list() {
        let h = "query Q($wheres: [Trade_filter!]!)";
        assert_eq!(
            rewrite_filter_type_names(h),
            "query Q($wheres: [Trade_bool_exp!]!)"
        );
    }

    #[test]
    fn rewrite_filter_type_names_multiple_vars() {
        let h = "query Q($a: Trade_filter, $b: Pair_filter!, $c: Int)";
        assert_eq!(
            rewrite_filter_type_names(h),
            "query Q($a: Trade_bool_exp, $b: Pair_bool_exp!, $c: Int)"
        );
    }

    #[test]
    fn rewrite_filter_type_names_does_not_touch_unrelated_types() {
        // ID/Int/Bytes types and PascalCase entity names without _filter must
        // be untouched.
        let h = "query Q($id: ID!, $ids: [Bytes!]!, $entity: Pair)";
        assert_eq!(rewrite_filter_type_names(h), h);
    }

    #[test]
    fn rewrite_filter_type_names_skips_lowercase_filter_suffix() {
        // `query_filter_helper` is not a type name; the function must not
        // rewrite the middle of an identifier.
        let h = "query my_filter_query($x: Int)";
        assert_eq!(rewrite_filter_type_names(h), h);
    }

    #[test]
    fn rewrite_filter_type_names_skips_var_named_filter() {
        // Variable names ending in `_filter` aren't types — only the *type*
        // position should be rewritten. The function inspects all identifiers,
        // but the suffix gate requires PascalCase, so lowercase identifiers
        // (the convention for variable names) are not rewritten.
        let h = "query Q($where_filter: Trade_filter)";
        assert_eq!(
            rewrite_filter_type_names(h),
            "query Q($where_filter: Trade_bool_exp)"
        );
    }

    #[test]
    fn rewrite_filter_type_names_handles_multiline_header() {
        let h = "query Q(\n  $where: Trade_filter,\n  $orderBy: String\n)";
        assert_eq!(
            rewrite_filter_type_names(h),
            "query Q(\n  $where: Trade_bool_exp,\n  $orderBy: String\n)"
        );
    }

    #[test]
    fn rewrite_filter_type_names_camel_case_entities() {
        // Entity names like `UniswapDayData_filter` (the original bug report).
        let h = "query Q($where: UniswapDayData_filter)";
        assert_eq!(
            rewrite_filter_type_names(h),
            "query Q($where: UniswapDayData_bool_exp)"
        );
    }

    // ─────────────────────────────────────────────────────────────────────
    // JSON filter-value translation — operator parity with the inline path
    // ─────────────────────────────────────────────────────────────────────

    #[test]
    fn translate_default_equality_no_suffix() {
        init_test_schema_if_needed();
        let v = json!({ "name": "X" });
        let out = translate_subgraph_filter_value(&v, "Pair");
        assert_eq!(out, json!({ "name": { "_eq": "X" } }));
    }

    #[test]
    fn translate_not_suffix() {
        init_test_schema_if_needed();
        let v = json!({ "name_not": "X" });
        let out = translate_subgraph_filter_value(&v, "Pair");
        assert_eq!(out, json!({ "name": { "_neq": "X" } }));
    }

    #[test]
    fn translate_gt_gte_lt_lte() {
        init_test_schema_if_needed();
        let v = json!({
            "amount_gt": 1,
            "amount_gte": 2,
            "amount_lt": 3,
            "amount_lte": 4,
        });
        let out = translate_subgraph_filter_value(&v, "Trade");
        // Every condition survives: the first keeps the field's key and the
        // rest, which would collide with it, join `_and`.
        assert_eq!(
            out,
            json!({
                "amount": { "_gt": 1 },
                "_and": [
                    { "amount": { "_gte": 2 } },
                    { "amount": { "_lt": 3 } },
                    { "amount": { "_lte": 4 } },
                ],
            })
        );
        // Spot-check that each op individually translates correctly.
        for (suffix, op) in [
            ("amount_gt", "_gt"),
            ("amount_gte", "_gte"),
            ("amount_lt", "_lt"),
            ("amount_lte", "_lte"),
        ] {
            let single = translate_subgraph_filter_value(&json!({ suffix: 1 }), "Trade");
            assert_eq!(single, json!({ "amount": { op: 1 } }));
        }
    }

    #[test]
    fn translate_in_and_not_in() {
        init_test_schema_if_needed();
        let v = json!({ "id_in": ["a", "b"] });
        let out = translate_subgraph_filter_value(&v, "Trade");
        assert_eq!(out, json!({ "id": { "_in": ["a", "b"] } }));

        let v = json!({ "id_not_in": ["a", "b"] });
        let out = translate_subgraph_filter_value(&v, "Trade");
        assert_eq!(out, json!({ "id": { "_nin": ["a", "b"] } }));
    }

    #[test]
    fn translate_contains_starts_ends_with() {
        init_test_schema_if_needed();
        // graph-node's plain forms are case-sensitive (`like`); `_nocase` is `ilike`.
        let cases = [
            ("name_contains", "_like", "%X%"),
            ("name_starts_with", "_like", "X%"),
            ("name_ends_with", "_like", "%X"),
            ("name_contains_nocase", "_ilike", "%X%"),
            ("name_starts_with_nocase", "_ilike", "X%"),
            ("name_ends_with_nocase", "_ilike", "%X"),
        ];
        for (suffix, op, pattern) in cases {
            let v = json!({ suffix: "X" });
            let out = translate_subgraph_filter_value(&v, "Pair");
            assert_eq!(
                out,
                json!({ "name": { op: pattern } }),
                "suffix {}",
                suffix
            );
        }
    }

    #[test]
    fn translate_negated_string_ops_go_into_not() {
        init_test_schema_if_needed();
        let cases = [
            ("name_not_contains", "_like", "%X%"),
            ("name_not_starts_with", "_like", "X%"),
            ("name_not_ends_with", "_like", "%X"),
            ("name_not_contains_nocase", "_ilike", "%X%"),
            ("name_not_starts_with_nocase", "_ilike", "X%"),
            ("name_not_ends_with_nocase", "_ilike", "%X"),
        ];
        for (suffix, op, pattern) in cases {
            let v = json!({ suffix: "X" });
            let out = translate_subgraph_filter_value(&v, "Pair");
            assert_eq!(
                out,
                json!({ "_not": { "name": { op: pattern } } }),
                "suffix {}",
                suffix
            );
        }
    }

    #[test]
    fn translate_and_connective() {
        init_test_schema_if_needed();
        let v = json!({
            "and": [
                { "amount_gt": 1 },
                { "amount_lt": 10 }
            ]
        });
        let out = translate_subgraph_filter_value(&v, "Trade");
        assert_eq!(
            out,
            json!({
                "_and": [
                    { "amount": { "_gt": 1 } },
                    { "amount": { "_lt": 10 } }
                ]
            })
        );
    }

    #[test]
    fn translate_or_connective() {
        init_test_schema_if_needed();
        let v = json!({
            "or": [
                { "name": "A" },
                { "name": "B" }
            ]
        });
        let out = translate_subgraph_filter_value(&v, "Pair");
        assert_eq!(
            out,
            json!({
                "_or": [
                    { "name": { "_eq": "A" } },
                    { "name": { "_eq": "B" } }
                ]
            })
        );
    }

    #[test]
    fn translate_hasura_shape_connective_aliases() {
        // Already-Hasura-style `_and`/`_or` should translate inner entries
        // but keep the connective key unchanged.
        init_test_schema_if_needed();
        let v = json!({
            "_or": [
                { "amount_gt": 1 },
                { "amount": { "_lt": 2 } }
            ]
        });
        let out = translate_subgraph_filter_value(&v, "Trade");
        assert_eq!(
            out,
            json!({
                "_or": [
                    { "amount": { "_gt": 1 } },
                    { "amount": { "_lt": 2 } }
                ]
            })
        );
    }

    #[test]
    fn translate_multiple_filters_in_one_object() {
        init_test_schema_if_needed();
        let v = json!({
            "amount_gt": 1,
            "isOpen": false
        });
        let out = translate_subgraph_filter_value(&v, "Trade");
        assert_eq!(
            out,
            json!({
                "amount": { "_gt": 1 },
                "isOpen": { "_eq": false }
            })
        );
    }

    #[test]
    fn translate_chain_id_special_case() {
        init_test_schema_if_needed();
        let v = json!({ "chainId": "1" });
        let out = translate_subgraph_filter_value(&v, "Trade");
        assert_eq!(out, json!({ "chainId": { "_eq": "1" } }));
    }

    #[test]
    fn translate_nested_entity_scalar_ref() {
        init_test_schema_if_needed();
        // `pair` is a nested entity in the test schema (Trade.pair → Pair).
        let v = json!({ "pair": "0xabc" });
        let out = translate_subgraph_filter_value(&v, "Trade");
        assert_eq!(out, json!({ "pair": { "id": { "_eq": "0xabc" } } }));
    }

    #[test]
    fn translate_nested_entity_object_ref() {
        init_test_schema_if_needed();
        let v = json!({ "pair": { "name": "ETH" } });
        let out = translate_subgraph_filter_value(&v, "Trade");
        assert_eq!(out, json!({ "pair": { "name": { "_eq": "ETH" } } }));
    }

    #[test]
    fn translate_nested_entity_trailing_underscore() {
        // Subgraph's strict form: `pair_: { name: "ETH" }`. The trailing
        // underscore should be stripped on the way through.
        init_test_schema_if_needed();
        let v = json!({ "pair_": { "name": "ETH" } });
        let out = translate_subgraph_filter_value(&v, "Trade");
        assert_eq!(out, json!({ "pair": { "name": { "_eq": "ETH" } } }));
    }

    #[test]
    fn translate_deeply_nested_with_scalar_at_depth() {
        init_test_schema_if_needed();
        // Trade.pair → Pair, Pair.token → Token. Scalar at depth becomes id eq.
        let v = json!({ "pair": { "token": "0xtok" } });
        let out = translate_subgraph_filter_value(&v, "Trade");
        assert_eq!(
            out,
            json!({ "pair": { "token": { "id": { "_eq": "0xtok" } } } })
        );
    }

    #[test]
    fn translate_deeply_nested_with_operator_at_depth() {
        init_test_schema_if_needed();
        let v = json!({ "pair": { "token": { "amount_gt": 100 } } });
        let out = translate_subgraph_filter_value(&v, "Trade");
        assert_eq!(
            out,
            json!({ "pair": { "token": { "amount": { "_gt": 100 } } } })
        );
    }

    #[test]
    fn translate_already_hasura_value_passes_through() {
        // If the caller has already shaped the value for Hasura, the leaf
        // emits the object verbatim (no double-wrapping).
        init_test_schema_if_needed();
        let v = json!({ "amount": { "_gt": 1 } });
        let out = translate_subgraph_filter_value(&v, "Trade");
        assert_eq!(out, json!({ "amount": { "_gt": 1 } }));
    }

    #[test]
    fn translate_mixed_subgraph_and_hasura_in_and() {
        init_test_schema_if_needed();
        let v = json!({
            "_and": [
                { "amount_gt": 1 },
                { "amount": { "_lt": 5 } }
            ]
        });
        let out = translate_subgraph_filter_value(&v, "Trade");
        assert_eq!(
            out,
            json!({
                "_and": [
                    { "amount": { "_gt": 1 } },
                    { "amount": { "_lt": 5 } }
                ]
            })
        );
    }

    #[test]
    fn translate_empty_object() {
        init_test_schema_if_needed();
        let v = json!({});
        let out = translate_subgraph_filter_value(&v, "Trade");
        assert_eq!(out, json!({}));
    }

    #[test]
    fn translate_non_object_passes_through() {
        init_test_schema_if_needed();
        assert_eq!(
            translate_subgraph_filter_value(&Value::Null, "Trade"),
            Value::Null
        );
        assert_eq!(
            translate_subgraph_filter_value(&json!(42), "Trade"),
            json!(42)
        );
        assert_eq!(
            translate_subgraph_filter_value(&json!("x"), "Trade"),
            json!("x")
        );
        assert_eq!(
            translate_subgraph_filter_value(&json!([1, 2]), "Trade"),
            json!([1, 2])
        );
    }

    #[test]
    fn translate_contains_any_unsupported_pass_through() {
        init_test_schema_if_needed();
        let v = json!({ "tags_containsAny": ["a", "b"] });
        let out = translate_subgraph_filter_value(&v, "Trade");
        // Pass-through so upstream returns a meaningful error.
        assert_eq!(out, json!({ "tags_containsAny": ["a", "b"] }));
    }

    #[test]
    fn translate_boolean_equality() {
        init_test_schema_if_needed();
        let v = json!({ "isOpen": false });
        let out = translate_subgraph_filter_value(&v, "Trade");
        assert_eq!(out, json!({ "isOpen": { "_eq": false } }));
    }

    #[test]
    fn translate_numeric_equality_from_string() {
        // Subgraph clients often send BigInt-typed fields as strings.
        init_test_schema_if_needed();
        let v = json!({ "amount": "100" });
        let out = translate_subgraph_filter_value(&v, "Trade");
        assert_eq!(out, json!({ "amount": { "_eq": "100" } }));
    }

    #[test]
    fn translate_explicit_not_block_recurses() {
        // Top-level `_not` is itself a connective; its body must be translated.
        init_test_schema_if_needed();
        let v = json!({ "_not": { "amount_gt": 1 } });
        let out = translate_subgraph_filter_value(&v, "Trade");
        assert_eq!(out, json!({ "_not": { "amount": { "_gt": 1 } } }));
    }

    #[test]
    fn translate_unsuffixed_underscore_field_does_not_split() {
        // A bare key ending in `_` (with nothing after) should be treated as
        // a nested-entity reference with the trailing `_` stripped, not as
        // an operator suffix. The schema has `Pair.token`, so use that.
        init_test_schema_if_needed();
        let v = json!({ "token_": { "name": "ETH" } });
        let out = translate_subgraph_filter_value(&v, "Pair");
        assert_eq!(out, json!({ "token": { "name": { "_eq": "ETH" } } }));
    }

    // ─────────────────────────────────────────────────────────────────────
    // End-to-end: filter-typed variables flow through conversion
    // ─────────────────────────────────────────────────────────────────────

    #[test]
    fn end_to_end_filter_variable_rewrites_type_and_value() {
        init_test_schema_if_needed();
        let payload = json!({
            "query": "query Q($first: Int, $where: Trade_filter) { trades(first: $first, where: $where) { id amount } }",
            "variables": {
                "first": 10,
                "where": { "amount_gt": 100, "isOpen": false }
            }
        });
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();

        // Header type rewritten.
        assert!(
            query.contains("$where: Trade_bool_exp"),
            "Expected Trade_bool_exp in header, got: {}",
            query
        );
        assert!(
            !query.contains("Trade_filter"),
            "Should not contain Trade_filter, got: {}",
            query
        );

        // Where clause forwards the variable verbatim.
        assert!(
            query.contains("where: $where"),
            "Expected where: $where, got: {}",
            query
        );

        // Variable value translated to Hasura shape.
        let translated_where = &result.query["variables"]["where"];
        assert_eq!(
            translated_where,
            &json!({
                "amount": { "_gt": 100 },
                "isOpen": { "_eq": false }
            })
        );

        // Untouched variables remain unchanged.
        assert_eq!(result.query["variables"]["first"], 10);

        // filter_variable_entities exposes the mapping for callers.
        assert_eq!(
            result.filter_variable_entities.get("where"),
            Some(&"Trade".to_string())
        );
    }

    #[test]
    fn end_to_end_filter_variable_with_and_connective() {
        init_test_schema_if_needed();
        let payload = json!({
            "query": "query Q($where: Trade_filter) { trades(where: $where) { id } }",
            "variables": {
                "where": {
                    "and": [
                        { "amount_gte": 1 },
                        { "amount_lte": 10 }
                    ]
                }
            }
        });
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        assert_eq!(
            &result.query["variables"]["where"],
            &json!({
                "_and": [
                    { "amount": { "_gte": 1 } },
                    { "amount": { "_lte": 10 } }
                ]
            })
        );
    }

    #[test]
    fn end_to_end_filter_variable_missing_value_is_safe() {
        // If the variable is declared but no value is supplied, we don't
        // panic; we simply leave the variables payload untouched (sans key).
        init_test_schema_if_needed();
        let payload = json!({
            "query": "query Q($where: Trade_filter) { trades(where: $where) { id } }",
            "variables": { "other": 1 }
        });
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        assert_eq!(result.query["variables"]["other"], 1);
        assert!(result.query["variables"].get("where").is_none());
    }

    #[test]
    fn end_to_end_filter_variable_with_chain_id() {
        // chain_id is normally injected into the where clause. When `where`
        // is a variable it is added to the variable's value instead, once,
        // and the query text is left alone.
        init_test_schema_if_needed();
        let payload = json!({
            "query": "query Q($where: Trade_filter) { trades(where: $where) { id } }",
            "variables": { "where": { "amount_gt": 1 } }
        });
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(
            query.contains("where: $where"),
            "Expected where: $where, got: {}",
            query
        );
        // No literal chainId injection alongside the variable reference.
        assert!(
            !query.contains("chainId: {_eq: \"1\"}"),
            "Did not expect chainId injection when where is a variable, got: {}",
            query
        );
        assert_eq!(
            &result.query["variables"]["where"],
            &json!({ "_and": [{ "amount": { "_gt": 1 } }, { "chainId": { "_eq": "1" } }] })
        );
    }

    #[test]
    fn end_to_end_already_hasura_shaped_variable_is_idempotent() {
        // A client that has migrated to Hyperindex-native shape will declare
        // `$where: Trade_bool_exp` (no rewrite needed) and send a Hasura
        // value. Translation should still pass it through unchanged.
        init_test_schema_if_needed();
        let payload = json!({
            "query": "query Q($where: Trade_bool_exp) { trades(where: $where) { id } }",
            "variables": { "where": { "amount": { "_gt": 1 } } }
        });
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(query.contains("$where: Trade_bool_exp"));

        // The variable is still classified as a filter variable (it's used as
        // `where: $var`), so the translator runs — but on an already-Hasura
        // value it's a no-op for the leaf field.
        assert_eq!(
            &result.query["variables"]["where"],
            &json!({ "amount": { "_gt": 1 } })
        );
    }

    #[test]
    fn end_to_end_inline_where_still_works_after_changes() {
        // Regression guard: the original inline-where path must not be
        // affected by the new variable plumbing.
        init_test_schema_if_needed();
        let payload = json!({
            "query": "query { trades(where: { amount_gt: 100 }) { id } }"
        });
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(
            query.contains("where: {amount: {_gt: 100}}"),
            "Inline where regression — got: {}",
            query
        );
        assert!(result.filter_variable_entities.is_empty());
    }

    #[test]
    fn end_to_end_multiple_entities_each_with_filter_variable() {
        init_test_schema_if_needed();
        let payload = json!({
            "query": "query Q($tw: Trade_filter, $pw: Pair_filter) { trades(where: $tw) { id } pairs(where: $pw) { id } }",
            "variables": {
                "tw": { "amount_gt": 1 },
                "pw": { "name_contains": "ETH" }
            }
        });
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(query.contains("$tw: Trade_bool_exp"));
        assert!(query.contains("$pw: Pair_bool_exp"));
        assert_eq!(
            &result.query["variables"]["tw"],
            &json!({ "amount": { "_gt": 1 } })
        );
        assert_eq!(
            &result.query["variables"]["pw"],
            &json!({ "name": { "_like": "%ETH%" } })
        );
        assert_eq!(result.filter_variable_entities.len(), 2);
    }

    #[test]
    fn end_to_end_filter_variable_with_nested_entity_scalar() {
        init_test_schema_if_needed();
        let payload = json!({
            "query": "query Q($where: Trade_filter) { trades(where: $where) { id pair { id } } }",
            "variables": {
                "where": { "pair": "0xabc" }
            }
        });
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        assert_eq!(
            &result.query["variables"]["where"],
            &json!({ "pair": { "id": { "_eq": "0xabc" } } })
        );
    }

    // ─────────────────────────────────────────────────────────────────────
    // Backwards-compat regression: existing variable handling untouched
    // ─────────────────────────────────────────────────────────────────────

    #[test]
    fn regression_non_filter_variable_passes_unchanged() {
        // A `$first: Int` variable with no filter usage should be left alone
        // and the entity map should not record it.
        let payload = json!({
            "query": "query Q($first: Int) { trades(first: $first) { id } }",
            "variables": { "first": 5 }
        });
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        assert_eq!(result.query["variables"]["first"], 5);
        assert!(result.filter_variable_entities.is_empty());
    }

    #[test]
    fn regression_bigint_to_numeric_still_applied() {
        // The header rewrite for BigInt → numeric was the older feature; make
        // sure the new `*_filter` rewrite layered on top doesn't disturb it.
        init_test_schema_if_needed();
        let payload = json!({
            "query": "query Q($a: BigInt!) { trades(where: { amount_gte: $a }) { id } }",
            "variables": { "a": "100" }
        });
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(
            query.contains("$a: numeric!"),
            "BigInt → numeric regression, got: {}",
            query
        );
    }

    #[test]
    fn regression_enum_string_override_still_applied() {
        init_test_schema_if_needed();
        let payload = json!({
            "query": "query Q($op: String!) { orders(where: { orderAction: $op }) { id } }",
            "variables": { "op": "Open" }
        });
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(
            query.contains("$op: orderaction!"),
            "Enum override regression, got: {}",
            query
        );
    }

    #[test]
    fn test_a_where_field_named_like_pagination_does_not_become_pagination() {
        // `parse_single_param` hoists the contents of `where: {...}` into the
        // argument map unprefixed, and `first` is a legal input-field name. The
        // filter then arrived as pagination: `where: {first: 2}` silently became
        // `limit: 2`, taking the real `first: 3` with it and dropping the filter.
        // The hoisted key is now marked, so it stays a filter.
        init_test_schema_if_needed();
        for (query, expected) in [
            (
                "{ pools(first: 1) { id poolHourData(first: 3, where: { first: 2 }) { id } } }",
                "poolHourData(limit: 3, order_by: {id: asc}, where: {first: {_eq: 2}})",
            ),
            (
                "{ pools(first: 3, where: { first: 2, skip_gt: 1 }) { id } }",
                "Pool(limit: 3, order_by: {id: asc}, where: {first: {_eq: 2}, skip: {_gt: 1}})",
            ),
        ] {
            let result = convert_subgraph_to_hyperindex(&create_test_payload(query), None).unwrap();
            let converted = result.query["query"].as_str().unwrap();
            assert!(converted.contains(expected), "{converted}");
        }
    }

    #[test]
    fn test_nested_arguments_without_commas_are_split() {
        // Commas are optional in GraphQL. `parse_graphql_params` only splits on
        // them, so `first: 3 skip: 1` arrived as a single `first` of "3 skip: 1"
        // and was emitted verbatim as `limit: 3 skip: 1` - malformed GraphQL,
        // forwarded upstream.
        init_test_schema_if_needed();
        for (query, expected) in [
            (
                "{ pools(first: 1) { id poolHourData(first: 3 skip: 1) { id } } }",
                "poolHourData(limit: 3, offset: 1, order_by: {id: asc})",
            ),
            (
                "{ pools(first: 1) { id poolHourData(first: 3 orderBy: periodStartUnix) { id } } }",
                "poolHourData(limit: 3, order_by: [{periodStartUnix: asc}, {id: asc}])",
            ),
            (
                "{ pools(first: 1) { id poolHourData(\n  first: 3\n  skip: 1\n) { id } } }",
                "poolHourData(limit: 3, offset: 1, order_by: {id: asc})",
            ),
        ] {
            let result = convert_subgraph_to_hyperindex(&create_test_payload(query), None)
                .unwrap_or_else(|e| panic!("expected {query:?} to convert, got {e:?}"));
            let converted = result.query["query"].as_str().unwrap();
            assert!(converted.contains(expected), "{converted}");
        }
    }

    #[test]
    fn test_unparseable_nested_arguments_are_rejected() {
        // An argument list that yields nothing is not an empty one. Dropping it
        // answers a different question than the one that was asked.
        init_test_schema_if_needed();
        let payload =
            create_test_payload("{ pools(first: 1) { id poolHourData(garbage) { id } } }");
        match convert_subgraph_to_hyperindex(&payload, None) {
            Err(ConversionError::UnsupportedNestedArgument(field, detail)) => {
                assert_eq!(field, "poolHourData");
                assert_eq!(detail, "unparseable arguments");
            }
            other => panic!("expected UnsupportedNestedArgument, got {other:?}"),
        }
    }

    #[test]
    fn test_comment_inside_nested_arguments_is_dropped() {
        // A `#` comment runs to end of line and can hold a `)`, which closed the
        // argument list early and forwarded malformed GraphQL. Keeping the
        // comment is no better: the arguments are re-emitted on one line, so it
        // would comment out the rest of them and the closing paren with it.
        init_test_schema_if_needed();
        for query in [
            "{\n  pools(first: 1) {\n    poolHourData(\n      first: 3   # only three (for now)\n      skip: 1\n    ) { id }\n  }\n}",
            "{\n  pools(first: 1) {\n    poolHourData(\n      first: 3   # keep ) three\n      skip: 1\n    ) { id }\n  }\n}",
            "{\n  pools(first: 1) {\n    poolHourData(first: 3, # a note\n      skip: 1) { id }\n  }\n}",
        ] {
            let result = convert_subgraph_to_hyperindex(&create_test_payload(query), None)
                .unwrap_or_else(|e| panic!("expected {query:?} to convert, got {e:?}"));
            let converted = result.query["query"].as_str().unwrap();
            assert!(
                converted.contains("poolHourData(limit: 3, offset: 1, order_by: {id: asc})"),
                "{converted}"
            );
            assert!(!converted.contains('#'), "comment survived: {converted}");
        }
    }

    #[test]
    fn test_comment_in_a_selection_set_is_left_alone() {
        // An open paren in a note used to start an argument scan that ate the
        // rest of the selection set - silently before nested args were
        // translated, and as a bogus `UnsupportedNestedArgument` after.
        init_test_schema_if_needed();
        let query = "{\n  pools(first: 1) {\n    id   # a note with ( an open paren\n    poolHourData(first: 3) { id }\n  }\n}";
        let result = convert_subgraph_to_hyperindex(&create_test_payload(query), None).unwrap();
        let converted = result.query["query"].as_str().unwrap();
        assert!(converted.contains("poolHourData(limit: 3, order_by: {id: asc})"), "{converted}");
        assert!(
            converted.contains("# a note with ( an open paren"),
            "the comment itself should survive: {converted}"
        );
    }

    #[test]
    fn test_a_hash_inside_a_string_argument_is_not_a_comment() {
        // `#` only opens a comment outside a string. If it were treated as one
        // here the scan would end inside the quotes and the argument name would
        // be lost, so the error naming `foo` is the proof that it did not.
        let sanitized = convert_selection_arguments(
            "b(first: 3, foo: \"#)(\") { id }",
            None,
            &mut ArgContext::default(),
        );
        match sanitized {
            Err(ConversionError::UnsupportedNestedArgument(field, detail)) => {
                assert_eq!(field, "b");
                assert_eq!(detail, "foo");
            }
            other => panic!("expected UnsupportedNestedArgument naming foo, got {other:?}"),
        }
    }

    #[test]
    fn test_nested_field_variable_first_and_skip_pass_through() {
        // `$f`/`$s` keep their declared `Int` type, so they need no rewriting -
        // unlike a variable sort, which does.
        init_test_schema_if_needed();
        let payload = create_test_payload(
            "query Q($f: Int, $s: Int) { pools(first: 1) { id poolHourData(first: $f, skip: $s) { periodStartUnix } } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(
            query.contains("poolHourData(limit: $f, offset: $s, order_by: {id: asc})"),
            "{query}"
        );
    }

    #[test]
    fn test_nested_field_args_are_translated_inside_a_fragment() {
        // Fragment bodies go through the same sanitizer, by way of
        // `sanitize_fragment_arguments`.
        init_test_schema_if_needed();
        let payload = create_test_payload(
            "query { pools(first: 1) { id ...PoolRows } } fragment PoolRows on Pool { poolHourData(first: 3, orderBy: periodStartUnix, orderDirection: desc) { periodStartUnix } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(
            query.contains("poolHourData(limit: 3, order_by: [{periodStartUnix: desc}, {id: desc}])"),
            "{query}"
        );
    }

    #[test]
    fn test_aliased_nested_field_keeps_its_alias() {
        init_test_schema_if_needed();
        let payload = create_test_payload(
            "query { pools(first: 1) { id recent: poolHourData(first: 2, orderBy: periodStartUnix, orderDirection: desc) { periodStartUnix } } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(
            query.contains("recent: poolHourData(limit: 2, order_by: [{periodStartUnix: desc}, {id: desc}])"),
            "{query}"
        );
    }

    #[test]
    fn test_two_levels_of_nesting_are_translated() {
        init_test_schema_if_needed();
        let payload = create_test_payload(
            "query { pools(first: 1) { id poolHourData(first: 2) { periodStartUnix swaps(first: 3, orderBy: timestamp) { id } } } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(query.contains("poolHourData(limit: 2, order_by: {id: asc})"), "{query}");
        assert!(
            query.contains("swaps(limit: 3, order_by: [{timestamp: asc}, {id: asc}])"),
            "{query}"
        );
    }

    #[test]
    fn test_nested_variable_sort_is_converted() {
        // A nested `orderBy: $o` gets the same treatment as a root one: the
        // client's variable is retyped to the nested entity's `order_by` and
        // its value rebuilt at request time.
        init_test_schema_if_needed();
        let payload = json!({
            "query": "query Q($o: String!, $d: OrderDirection!) { pools(first: 1) { poolHourData(first: 3, orderBy: $o, orderDirection: $d) { id } } }",
            "variables": {"o": "periodStartUnix", "d": "desc"}
        });
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(query.contains("poolHourData(limit: 3, order_by: $o)"), "{query}");
        assert!(query.contains("$o: [PoolHourData_order_by!]"), "{query}");
        assert_eq!(
            result.query["variables"]["o"],
            json!([{"periodStartUnix": "desc"}, {"id": "desc"}])
        );
    }

    #[test]
    fn test_nested_where_is_converted() {
        // Converted against the nested entity, as at the root, rather than
        // dropped (unfiltered rows under a 200) or rejected.
        init_test_schema_if_needed();
        let payload = create_test_payload(
            "query { pools(first: 1) { id poolHourData(first: 3, where: { volumeUSD_gt: \"5\" }) { periodStartUnix } } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(
            query.contains("poolHourData(limit: 3, order_by: {id: asc}, where: {volumeUSD: {_gt: \"5\"}})"),
            "{query}"
        );
    }

    #[test]
    fn test_unknown_nested_argument_is_rejected() {
        // graph-node accepts exactly first/skip/orderBy/orderDirection/where on a
        // derived field, so anything else is a filter or a mistake. Either way,
        // dropping it answers a question that was not asked.
        init_test_schema_if_needed();
        let payload = create_test_payload(
            "query { pools(first: 1) { id poolHourData(block: { number: 100 }) { periodStartUnix } } }",
        );
        match convert_subgraph_to_hyperindex(&payload, None) {
            Err(ConversionError::UnsupportedNestedArgument(field, detail)) => {
                assert_eq!(field, "poolHourData");
                assert_eq!(detail, "block");
            }
            other => panic!("expected UnsupportedNestedArgument, got {other:?}"),
        }
    }

    #[test]
    fn test_nested_order_direction_without_order_by_sorts_by_id() {
        // Same as the root path: a direction with nothing to sort by is ignored,
        // and graph-node's default `id` order applies.
        init_test_schema_if_needed();
        let payload = create_test_payload(
            "query { pools(first: 1) { id poolHourData(first: 2, orderDirection: desc) { periodStartUnix } } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(query.contains("poolHourData(limit: 2, order_by: {id: asc})"), "{query}");
    }

    #[test]
    fn test_nested_field_first_and_order_by_translated() {
        // Regression: nested-field args (first/orderBy/orderDirection) on
        // sub-selections like `poolHourData(...)` and `poolDayData(...)` were
        // silently stripped by sanitize_selection_set, so the user got
        // unbounded/unordered nested rows. They should now be translated to
        // Hasura's limit/order_by form - with the same `id` tie-break the root
        // path adds, since a nested page whose sort key repeats is otherwise
        // just as unstable as a root one.
        init_test_schema_if_needed();
        let payload = create_test_payload(
            "{ pools(first: 1, where: { id: \"0x2a2c512beaa8eb15495726c235472d82effb7a6b\" }) { id poolHourData(first: 3, orderBy: periodStartUnix, orderDirection: desc) { periodStartUnix volumeUSD } poolDayData(first: 3, orderBy: date, orderDirection: desc) { date volumeUSD } } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(
            query.contains("poolHourData(limit: 3, order_by: [{periodStartUnix: desc}, {id: desc}])"),
            "poolHourData nested args not translated:\n{}",
            query
        );
        assert!(
            query.contains("poolDayData(limit: 3, order_by: [{date: desc}, {id: desc}])"),
            "poolDayData nested args not translated:\n{}",
            query
        );
    }

    #[test]
    fn test_nested_field_skip_translated() {
        init_test_schema_if_needed();
        let payload = create_test_payload(
            "{ pools(first: 1) { id poolHourData(first: 5, skip: 10) { periodStartUnix } } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(
            query.contains("poolHourData(limit: 5, offset: 10, order_by: {id: asc})"),
            "nested skip not translated to offset:\n{}",
            query
        );
    }

    #[test]
    fn test_nested_field_orderby_default_direction() {
        init_test_schema_if_needed();
        let payload = create_test_payload(
            "{ pools(first: 1) { id poolHourData(orderBy: periodStartUnix) { periodStartUnix } } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(
            query.contains("poolHourData(limit: 100, order_by: [{periodStartUnix: asc}, {id: asc}])"),
            "nested orderBy without direction should default to asc:\n{}",
            query
        );
    }

    #[test]
    fn regression_query_with_variables_passthrough() {
        // The pre-existing test from `test_query_with_variables`, repeated
        // verbatim here to make any drift in the new path immediately visible.
        let payload = json!({
            "query": "query FactoriesAndBundles($first: Int!) { factories(first: $first) { id poolCount } bundles(first: $first) { id nativePriceUSD } }",
            "variables": { "first": 5 }
        });
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        assert_eq!(result.query["variables"]["first"], 5);
        assert!(result.filter_variable_entities.is_empty());
        let query = result.query["query"].as_str().unwrap();
        assert!(query.contains("$first"));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Argus production queries (see `test-queries.md`).
    //
    // Three converter bugs surfaced by their 31-query client. Each is pinned here
    // with the query text exactly as they send it.
    // ─────────────────────────────────────────────────────────────────────────

    fn argus_payload(query: &str, variables: Value) -> Value {
        json!({ "query": query, "variables": variables })
    }

    // ---- Cause A: order arguments passed as variables ----

    /// Argus `SwapsOf`: literal `orderBy`, variable `orderDirection`.
    ///
    /// Previously the whole `order_by` argument was dropped, so this returned
    /// arbitrarily-ordered rows with a 200.
    #[test]
    fn argus_swaps_of_order_direction_variable() {
        init_test_schema_if_needed();
        let payload = argus_payload(
            "query SwapsOf($launch: Bytes!, $from: BigInt!, $first: Int!, $skip: Int!, $direction: OrderDirection!) { swaps(first: $first, skip: $skip, where: { launch: $launch, timestamp_gte: $from }, orderBy: ordinal, orderDirection: $direction) { id ordinal } }",
            json!({"launch": "0xabc", "from": "100", "first": 50, "skip": 0, "direction": "desc"}),
        );
        let result = convert_subgraph_to_hyperindex(&payload, Some("5042")).unwrap();
        let query = result.query["query"].as_str().unwrap();

        assert!(
            query.contains("order_by: [{ordinal: $direction}, {id: $direction}]"),
            "ordering must survive a variable direction: {}",
            query
        );
        assert!(
            query.contains("$direction: order_by!"),
            "OrderDirection has no Hasura equivalent: {}",
            query
        );
        assert!(!query.contains("OrderDirection"), "{}", query);
        // The client's own value is already a valid `order_by` enum value.
        assert_eq!(result.query["variables"]["direction"], json!("desc"));
    }

    #[test]
    fn order_direction_variable_preserves_nullability() {
        let payload = argus_payload(
            "query Q($direction: OrderDirection) { swaps(orderBy: ordinal, orderDirection: $direction) { id } }",
            json!({}),
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(query.contains("$direction: order_by)"), "{}", query);
        assert!(!query.contains("order_by!"), "{}", query);
    }

    /// Argus `LaunchKeyPage`: the order field itself is a variable.
    ///
    /// GraphQL has no variable object keys, so the client's `$orderBy` is retyped
    /// to `[Launch_order_by!]` and its value rebuilt as `[{field: direction}]`.
    #[test]
    fn argus_launch_key_page_order_by_variable() {
        init_test_schema_if_needed();
        let payload = argus_payload(
            "query LaunchKeyPage($where: Launch_filter!, $orderBy: Launch_orderBy!, $direction: OrderDirection!, $first: Int!) { launches(first: $first, where: $where, orderBy: $orderBy, orderDirection: $direction) { id key: createdAt } }",
            json!({"where": {"dividendsPaid_gt": "0"}, "orderBy": "createdAt", "direction": "desc", "first": 25}),
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();

        assert!(query.contains("order_by: $orderBy"), "{}", query);
        assert!(query.contains("$orderBy: [Launch_order_by!]"), "{}", query);
        assert!(!query.contains("Launch_orderBy"), "{}", query);
        // `$direction` is folded into the rebuilt value, so its declaration would
        // be unused — and GraphQL rejects an unused variable definition.
        assert!(!query.contains("$direction"), "{}", query);
        assert!(query.contains("$where: Launch_bool_exp!"), "{}", query);

        let vars = &result.query["variables"];
        assert_eq!(vars["orderBy"], json!([{"createdAt": "desc"}, {"id": "desc"}]));
        assert_eq!(vars["where"], json!({"dividendsPaid": {"_gt": "0"}}));
        assert!(vars.get("direction").is_none(), "vars: {}", vars);

        assert_eq!(result.dropped_variables, vec!["direction".to_string()]);
        assert_eq!(result.order_by_variables.len(), 1);
        assert_eq!(result.order_by_variables[0].target_var, "orderBy");
        assert_eq!(result.order_by_variables[0].entity, "Launch");
    }

    #[test]
    fn order_by_variable_defaults_direction_to_asc() {
        let payload = argus_payload(
            "query Q($ob: Launch_orderBy!) { launches(orderBy: $ob) { id } }",
            json!({"ob": "createdAt"}),
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        assert_eq!(
            result.query["variables"]["ob"],
            json!([{"createdAt": "asc"}, {"id": "asc"}])
        );
    }

    #[test]
    fn order_by_variable_with_literal_direction() {
        let payload = argus_payload(
            "query Q($ob: Launch_orderBy!) { launches(orderBy: $ob, orderDirection: desc) { id } }",
            json!({"ob": "createdAt"}),
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        assert_eq!(
            result.query["variables"]["ob"],
            json!([{"createdAt": "desc"}, {"id": "desc"}])
        );
    }

    #[test]
    fn order_by_variable_missing_value_is_safe() {
        // No `ob` key in the payload: graph-node's default `id` order rather
        // than failing the request, or leaving the rows unordered.
        let payload = argus_payload(
            "query Q($ob: Launch_orderBy!) { launches(orderBy: $ob) { id } }",
            json!({}),
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        assert_eq!(result.query["variables"]["ob"], json!([{"id": "asc"}]));
    }

    #[test]
    fn order_by_variable_normalizes_uppercase_direction() {
        // Hasura matches its enum exactly; subgraph clients sometimes send `DESC`.
        let payload = argus_payload(
            "query Q($ob: Launch_orderBy!, $dir: OrderDirection!) { launches(orderBy: $ob, orderDirection: $dir) { id } }",
            json!({"ob": "createdAt", "dir": "DESC"}),
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        assert_eq!(
            result.query["variables"]["ob"],
            json!([{"createdAt": "desc"}, {"id": "desc"}])
        );
    }

    #[test]
    fn order_by_variable_expands_nested_field_syntax() {
        // Subgraph spells nested ordering `parent__child`.
        let payload = argus_payload(
            "query Q($ob: Swap_orderBy!) { swaps(orderBy: $ob, orderDirection: desc) { id } }",
            json!({"ob": "launch__createdAt"}),
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        assert_eq!(
            result.query["variables"]["ob"],
            json!([{"launch": {"createdAt": "desc"}}, {"id": "desc"}])
        );
    }

    #[test]
    fn literal_nested_order_by_expands_too() {
        let payload = create_test_payload(
            "query { swaps(orderBy: launch__createdAt, orderDirection: desc) { id } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(
            query.contains("order_by: [{launch: {createdAt: desc}}, {id: desc}]"),
            "{}",
            query
        );
    }

    #[test]
    fn order_direction_variable_without_order_by_drops_declaration() {
        // Nothing to order by, so the variable is unused — and an unused
        // declaration is a GraphQL validation error.
        let payload = argus_payload(
            "query Q($direction: OrderDirection!) { swaps(orderDirection: $direction) { id } }",
            json!({"direction": "desc"}),
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        // graph-node ignores a lone `orderDirection` and sorts by `id` ascending.
        assert!(query.contains("Swap(limit: 100, order_by: {id: asc})"), "{}", query);
        assert!(!query.contains("$direction"), "{}", query);
        // The whole definition list is gone, so the parens must go with it.
        assert!(query.starts_with("query Q {"), "{}", query);
        assert!(result.query["variables"].get("direction").is_none());
    }

    #[test]
    fn same_order_by_variable_across_two_entities_synthesizes_second() {
        // One variable cannot be typed as both `[Launch_order_by!]` and
        // `[Swap_order_by!]`.
        let payload = argus_payload(
            "query Q($ob: String!) { launches(orderBy: $ob) { id } swaps(orderBy: $ob) { id } }",
            json!({"ob": "createdAt"}),
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(query.contains("$ob: [Launch_order_by!]"), "{}", query);
        assert!(query.contains("$order_by_1: [Swap_order_by!]"), "{}", query);
        assert!(query.contains("order_by: $order_by_1"), "{}", query);
        assert_eq!(result.order_by_variables.len(), 2);
    }

    #[test]
    fn order_by_variable_shared_by_same_entity_is_not_duplicated() {
        let payload = argus_payload(
            "query Q($ob: String!) { launches(orderBy: $ob) { id } launches(orderBy: $ob) { name } }",
            json!({"ob": "createdAt"}),
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(!query.contains("order_by_1"), "{}", query);
        assert_eq!(result.order_by_variables.len(), 1);
    }

    // ---- Cause B: operator suffixes on entity-reference fields ----

    /// Argus `HoursOfLaunches`: `launch_in` filters a relation by id.
    ///
    /// Previously emitted `launch: {_in: $ids}`, which Hasura rejects because
    /// `_in` is not a field of `Launch_bool_exp`.
    #[test]
    fn argus_hours_of_launches_nested_in_filter() {
        init_test_schema_if_needed();
        let payload = argus_payload(
            "query HoursOfLaunches($ids: [Bytes!]!, $since: BigInt!, $cursor: Bytes!, $first: Int!) { launchHourDatas(first: $first, where: { launch_in: $ids, periodStart_gte: $since, id_gt: $cursor }, orderBy: id) { id periodStart } }",
            json!({"ids": ["0xa"], "since": "1", "cursor": "", "first": 10}),
        );
        let result = convert_subgraph_to_hyperindex(&payload, Some("5042")).unwrap();
        let query = result.query["query"].as_str().unwrap();

        assert!(query.contains("launch: {id: {_in: $ids}}"), "{}", query);
        // A scalar `id_gt` must be untouched by the same change.
        assert!(query.contains("id: {_gt: $cursor}"), "{}", query);
        assert!(query.contains("periodStart: {_gte: $since}"), "{}", query);
        assert!(query.contains("order_by: {id: asc}"), "{}", query);
    }

    #[test]
    fn argus_days_of_launches_nested_in_filter() {
        init_test_schema_if_needed();
        let payload = argus_payload(
            "query DaysOfLaunches($ids: [Bytes!]!, $since: BigInt!, $cursor: Bytes!, $first: Int!) { launchDayDatas(first: $first, where: { launch_in: $ids, periodStart_gte: $since, id_gt: $cursor }, orderBy: id) { id } }",
            json!({"ids": ["0xa"], "since": "1", "cursor": "", "first": 10}),
        );
        let result = convert_subgraph_to_hyperindex(&payload, Some("5042")).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(query.contains("launch: {id: {_in: $ids}}"), "{}", query);
    }

    #[test]
    fn nested_not_in_filter_routes_through_id() {
        init_test_schema_if_needed();
        let payload = create_test_payload(
            "query { swaps(where: { launch_not_in: [\"0xa\", \"0xb\"] }) { id } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(
            query.contains("launch: {id: {_nin: [\"0xa\", \"0xb\"]}}"),
            "{}",
            query
        );
    }

    #[test]
    fn scalar_in_filter_is_unaffected_by_nested_routing() {
        init_test_schema_if_needed();
        let payload = create_test_payload("query { launches(where: { id_in: [\"0xa\"] }) { id } }");
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(query.contains("id: {_in: [\"0xa\"]}"), "{}", query);
        assert!(!query.contains("{id: {id:"), "{}", query);
    }

    /// The same bug existed on the JSON path, which is what a whole-`where`
    /// variable travels through.
    #[test]
    fn nested_in_filter_translates_on_json_variable_path() {
        init_test_schema_if_needed();
        let translated = translate_subgraph_filter_value(
            &json!({"launch_in": ["0xa"], "periodStart_gte": "1"}),
            "LaunchHourData",
        );
        assert_eq!(
            translated,
            json!({"launch": {"id": {"_in": ["0xa"]}}, "periodStart": {"_gte": "1"}})
        );
    }

    #[test]
    fn nested_not_in_filter_translates_on_json_variable_path() {
        init_test_schema_if_needed();
        let translated =
            translate_subgraph_filter_value(&json!({"launch_not_in": ["0xa"]}), "Swap");
        assert_eq!(translated, json!({"launch": {"id": {"_nin": ["0xa"]}}}));
    }

    /// Argus `FallenBack` constrains the same relation twice: by id
    /// (`launch_not_in`) and by one of its own fields (`launch_: {...}`). Both
    /// render as `launch: {...}`, and a GraphQL object cannot repeat a key.
    #[test]
    fn relation_filtered_by_id_and_by_field_does_not_duplicate_key() {
        init_test_schema_if_needed();
        let payload = argus_payload(
            "query FallenBack($exclude: [String!]!, $quote: String!, $first: Int!) { swaps(first: $first, where: { launch_not_in: $exclude, launch_: { quoteAsset: $quote } }, orderBy: ordinal, orderDirection: desc) { ordinal } }",
            json!({"exclude": ["0xa"], "quote": "USDC", "first": 5}),
        );
        let result = convert_subgraph_to_hyperindex(&payload, Some("5042")).unwrap();
        let query = result.query["query"].as_str().unwrap();

        assert!(
            query.contains("launch: {id: {_nin: $exclude}}"),
            "{}",
            query
        );
        assert!(
            query.contains("_and: [{launch: {quoteAsset: {_eq: $quote}}}]"),
            "{}",
            query
        );
        // The strict-nested spelling must not leak through.
        assert!(!query.contains("launch_:"), "{}", query);
        let where_start = query.find("where: {").unwrap();
        assert_eq!(
            query[where_start..].matches("launch: {").count(),
            2,
            "one at top level, one inside _and: {}",
            query
        );
    }

    #[test]
    fn strict_nested_relation_syntax_is_stripped_inline() {
        init_test_schema_if_needed();
        let payload = create_test_payload(
            "query { swaps(where: { launch_: { quoteAsset: \"USDC\" } }) { id } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(
            query.contains("launch: {quoteAsset: {_eq: \"USDC\"}}"),
            "{}",
            query
        );
        assert!(!query.contains("launch_"), "{}", query);
    }

    // ---- `rewrite_variable_definitions` ----

    fn retype(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn remove(names: &[&str]) -> HashSet<String> {
        names.iter().map(|n| n.to_string()).collect()
    }

    #[test]
    fn rewrite_vardefs_retypes_preserving_position() {
        let out = rewrite_variable_definitions(
            "query Q($a: Int!, $d: OrderDirection!, $b: String)",
            &retype(&[("d", "order_by!")]),
            &HashSet::new(),
        );
        assert_eq!(out, "query Q($a: Int!, $d: order_by!, $b: String)");
    }

    #[test]
    fn rewrite_vardefs_removes_middle_declaration_cleanly() {
        let out = rewrite_variable_definitions(
            "query Q($a: Int!, $d: OrderDirection!, $b: String)",
            &HashMap::new(),
            &remove(&["d"]),
        );
        assert_eq!(out, "query Q($a: Int!, $b: String)");
    }

    #[test]
    fn rewrite_vardefs_removing_last_declaration_drops_parens() {
        let out = rewrite_variable_definitions(
            "query Q($d: OrderDirection!)",
            &HashMap::new(),
            &remove(&["d"]),
        );
        assert_eq!(out, "query Q");
    }

    #[test]
    fn rewrite_vardefs_handles_comma_less_list() {
        // GraphQL treats commas as whitespace, so they are optional.
        let out = rewrite_variable_definitions(
            "query Q($a: Int $d: OrderDirection!)",
            &retype(&[("d", "order_by!")]),
            &HashSet::new(),
        );
        assert_eq!(out, "query Q($a: Int, $d: order_by!)");
    }

    #[test]
    fn rewrite_vardefs_retype_drops_incompatible_default() {
        // `= createdAt` would not typecheck against `[Launch_order_by!]`.
        let out = rewrite_variable_definitions(
            "query Q($ob: Launch_orderBy! = createdAt)",
            &retype(&[("ob", "[Launch_order_by!]")]),
            &HashSet::new(),
        );
        assert_eq!(out, "query Q($ob: [Launch_order_by!])");
    }

    #[test]
    fn rewrite_vardefs_preserves_default_on_untouched_declaration() {
        let out = rewrite_variable_definitions(
            "query Q($a: Int = 5, $d: OrderDirection!)",
            &retype(&[("d", "order_by!")]),
            &HashSet::new(),
        );
        assert_eq!(out, "query Q($a: Int = 5, $d: order_by!)");
    }

    #[test]
    fn rewrite_vardefs_adds_a_declaration_it_has_not_seen() {
        // A synthesized order_by variable has no declaration to rewrite.
        let out = rewrite_variable_definitions(
            "query Q($a: Int!)",
            &retype(&[("order_by_1", "[Swap_order_by!]")]),
            &HashSet::new(),
        );
        assert_eq!(out, "query Q($a: Int!, $order_by_1: [Swap_order_by!])");
    }

    #[test]
    fn rewrite_vardefs_creates_a_list_when_the_header_has_none() {
        let out = rewrite_variable_definitions(
            "query Q",
            &retype(&[("order_by_1", "[Swap_order_by!]")]),
            &HashSet::new(),
        );
        assert_eq!(out, "query Q($order_by_1: [Swap_order_by!])");
    }

    #[test]
    fn rewrite_vardefs_is_a_noop_without_work() {
        let header = "query Q($a: Int!)";
        assert_eq!(
            rewrite_variable_definitions(header, &HashMap::new(), &HashSet::new()),
            header
        );
    }

    #[test]
    fn referenced_variables_matches_whole_tokens() {
        let found = referenced_variables("  Launch(limit: $first, order_by: $orderBy) {\n  id\n}");
        assert!(found.contains("first"));
        assert!(found.contains("orderBy"));
        // `$order` must not be found merely because `$orderBy` is present.
        assert!(!found.contains("order"));
    }

    /// A variable still referenced elsewhere must never be removed.
    #[test]
    fn direction_variable_still_used_elsewhere_is_kept() {
        let payload = argus_payload(
            "query Q($dir: OrderDirection!) { launches(orderBy: $dir) { id } swaps(orderBy: ordinal, orderDirection: $dir) { id } }",
            json!({"dir": "desc"}),
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        // First field repurposes `$dir` as the Launch order_by, second still
        // references it as a direction — so it must stay declared.
        assert!(query.contains("$dir"), "{}", query);
    }

    // ---- Regressions the Argus write-up asks to be pinned ----
    //
    // These pass today. They are pinned because breaking them returns an empty
    // list rather than an error, which their UI cannot distinguish from "no rows".

    #[test]
    fn argus_root_field_plurals_singularize_exactly() {
        for (field, entity) in [
            ("launches", "Launch"),
            ("swaps", "Swap"),
            ("holders", "Holder"),
            ("launchHourDatas", "LaunchHourData"),
            ("launchDayDatas", "LaunchDayData"),
            ("protocolDayDatas", "ProtocolDayData"),
            ("txTransferses", "TxTransfers"),
        ] {
            let payload = create_test_payload(&format!("query {{ {}(first: 1) {{ id }} }}", field));
            let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
            let query = result.query["query"].as_str().unwrap();
            assert!(
                query.contains(&format!("{}(limit: 1, order_by: {{id: asc}})", entity)),
                "{} should convert to {}: {}",
                field,
                entity,
                query
            );
            assert_eq!(result.field_name_map.get(entity), Some(&field.to_string()));
        }
    }

    /// Their paging is `id_gt` over `orderBy: id`. If id encoding or ordering
    /// changed, paging would silently skip or repeat rows.
    #[test]
    fn argus_keyset_pagination_round_trips_unchanged() {
        let payload = argus_payload(
            "query Launches($cursor: Bytes!, $first: Int!) { launches(first: $first, where: { id_gt: $cursor }, orderBy: id) { id name } }",
            json!({"cursor": "0xaa", "first": 500}),
        );
        let result = convert_subgraph_to_hyperindex(&payload, Some("5042")).unwrap();
        assert_eq!(
            result.query["query"].as_str().unwrap(),
            "query Launches($cursor: String!, $first: Int!) {\n  Launch(limit: $first, order_by: {id: asc}, where: {chainId: {_eq: \"5042\"}, id: {_gt: $cursor}}) {\n    id name\n  }\n}"
        );
        // The cursor value must be forwarded byte-for-byte and stay lowercase.
        assert_eq!(result.query["variables"]["cursor"], json!("0xaa"));
    }

    #[test]
    fn regression_literal_order_by_gains_id_tiebreak() {
        let payload = create_test_payload(
            "query { streams(first: 10, orderBy: name, orderDirection: desc) { id } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(query.contains("order_by: [{name: desc}, {id: desc}]"), "{}", query);
    }

    #[test]
    fn regression_literal_order_by_defaults_to_asc_with_id_tiebreak() {
        let payload = create_test_payload("query { streams(orderBy: name) { id } }");
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(query.contains("order_by: [{name: asc}, {id: asc}]"), "{}", query);
    }

    // ---- interface fragments + id tie-break (UserTransaction stand-in) ----

    const CLIENT_QUERY: &str = "query UserTransactions($userAddress: String!, $first: Int!, $skip: Int!) { userTransactions(where: { user: $userAddress }, orderBy: timestamp, orderDirection: desc, first: $first, skip: $skip) { id timestamp txHash action ... on Supply { amount reserve { symbol decimals } assetPriceUSD } ... on Borrow { amount borrowRateMode reserve { symbol } } } }";

    #[test]
    fn interface_fragments_query_is_rewritten_end_to_end() {
        init_test_schema_if_needed();
        let payload = argus_payload(
            CLIENT_QUERY,
            json!({"userAddress": "0xabc", "first": 10, "skip": 0}),
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        let flat = query.split_whitespace().collect::<Vec<_>>().join(" ");

        assert!(!flat.contains("... on"), "fragments must be gone: {flat}");
        assert!(flat.contains("_on_Supply: supply { amount reserve { symbol decimals } assetPriceUSD }"), "{flat}");
        assert!(flat.contains("_on_Borrow: borrow { amount borrowRateMode reserve { symbol } }"), "{flat}");
        // The other two fixes must still apply to the same field.
        assert!(flat.contains("order_by: [{timestamp: desc}, {id: desc}]"), "{flat}");
        assert!(flat.contains("user: {id: {_eq: $userAddress}}"), "{flat}");
        assert!(result.shape_plans.contains_key("UserTransaction"));
        assert_eq!(result.query["variables"]["userAddress"], json!("0xabc"));
    }

    #[test]
    fn query_without_fragments_has_no_shape_plan_and_is_unchanged() {
        init_test_schema_if_needed();
        let payload = create_test_payload("query { userTransactions(first: 5) { id supply { amount } } }");
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        assert!(result.shape_plans.is_empty());
        let query = result.query["query"].as_str().unwrap();
        assert_eq!(
            query,
            "query {\n  UserTransaction(limit: 5, order_by: {id: asc}) {\n    id supply { amount }\n  }\n}"
        );
    }

    #[test]
    fn by_pk_root_with_a_fragment_gets_a_plan() {
        init_test_schema_if_needed();
        let payload = create_test_payload(
            "query { userTransaction(id: \"1:2:0xab\") { id ... on Supply { amount } } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(query.contains("UserTransaction_by_pk"), "{query}");
        assert!(query.contains("_on_Supply: supply"), "{query}");
        assert!(result.shape_plans.contains_key("UserTransaction_by_pk"));
    }

    #[test]
    fn named_fragment_definition_is_rewritten_in_the_output() {
        init_test_schema_if_needed();
        let payload = create_test_payload(
            "fragment Tx on UserTransaction { id ... on Supply { amount } }\nquery { userTransactions(first: 5) { ...Tx } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        let flat = query.split_whitespace().collect::<Vec<_>>().join(" ");
        assert!(flat.contains("fragment Tx on UserTransaction"), "{flat}");
        assert!(flat.contains("_on_Supply: supply { amount }"), "{flat}");
        assert!(flat.contains("...Tx"), "{flat}");
        assert!(!flat.contains("... on"), "{flat}");
        assert!(result.shape_plans["UserTransaction"].children_for_test().contains(&"_on_Supply".to_string()));
    }

    #[test]
    fn two_roots_of_the_same_entity_merge_their_plans() {
        init_test_schema_if_needed();
        let payload = create_test_payload(
            "query { userTransactions(first: 1) { id ... on Supply { amount } } userTransactions(first: 1) { id ... on Borrow { borrowRateMode } } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let mut keys = result.shape_plans["UserTransaction"].children_for_test();
        keys.sort();
        assert_eq!(keys, vec!["_on_Borrow".to_string(), "_on_Supply".to_string()]);
    }

    #[test]
    fn order_by_id_gets_no_duplicate_tiebreak() {
        init_test_schema_if_needed();
        let payload = create_test_payload("query { streams(orderBy: id, orderDirection: desc) { id } }");
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(query.contains("order_by: {id: desc}"), "{query}");
        assert!(!query.contains("[{id"), "{query}");
    }

    #[test]
    fn order_by_variable_set_to_id_adds_nothing() {
        let payload = argus_payload(
            "query Q($ob: Launch_orderBy!) { launches(orderBy: $ob, orderDirection: asc) { id } }",
            json!({"ob": "id"}),
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        assert_eq!(result.query["variables"]["ob"], json!([{"id": "asc"}]));
    }

    #[test]
    fn tiebreak_follows_a_variable_direction() {
        init_test_schema_if_needed();
        let payload = create_test_payload(
            "query Q($dir: OrderDirection!) { streams(orderBy: name, orderDirection: $dir) { id } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(query.contains("order_by: [{name: $dir}, {id: $dir}]"), "{query}");
        assert!(query.contains("$dir: order_by"), "{query}");
    }

    #[test]
    fn tiebreak_helpers_respect_the_off_switch() {
        assert_eq!(literal_order_by_with_tiebreak("name", "desc", false), "{name: desc}");
        assert_eq!(
            literal_order_by_with_tiebreak("name", "desc", true),
            "[{name: desc}, {id: desc}]"
        );
        assert_eq!(literal_order_by_with_tiebreak("id", "asc", true), "{id: asc}");
        assert_eq!(
            order_by_json_with_tiebreak("name", "DESC", false),
            json!([{"name": "desc"}])
        );
        assert_eq!(
            order_by_json_with_tiebreak("pool__name", "asc", true),
            json!([{"pool": {"name": "asc"}}, {"id": "asc"}])
        );
    }

    // ── Root aliases ─────────────────────────────────────────────────────────

    #[test]
    fn aliased_collection_keeps_its_alias() {
        init_test_schema_if_needed();
        let payload = create_test_payload(
            "query { recent: streams(first: 2, orderBy: timestamp, orderDirection: desc) { id } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(
            query.contains("recent: Stream(limit: 2, order_by: [{timestamp: desc}, {id: desc}])"),
            "{query}"
        );
        assert_eq!(result.field_name_map.get("recent"), Some(&"recent".to_string()));
    }

    #[test]
    fn aliased_primary_key_lookup_keeps_its_alias() {
        init_test_schema_if_needed();
        let payload = create_test_payload("query { mine: token(id: \"0xabc\") { id symbol } }");
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(query.contains("mine: Token_by_pk(id: \"0xabc\")"), "{query}");
        assert_eq!(result.field_name_map.get("mine"), Some(&"mine".to_string()));
    }

    #[test]
    fn same_entity_twice_under_two_aliases() {
        init_test_schema_if_needed();
        let payload = create_test_payload(
            "query { a: tokens(first: 1, orderBy: id) { id } b: tokens(first: 1, orderBy: id, orderDirection: desc) { id } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(query.contains("a: Token(limit: 1, order_by: {id: asc})"), "{query}");
        assert!(query.contains("b: Token(limit: 1, order_by: {id: desc})"), "{query}");
        assert_eq!(result.field_name_map.get("a"), Some(&"a".to_string()));
        assert_eq!(result.field_name_map.get("b"), Some(&"b".to_string()));
    }

    #[test]
    fn alias_beside_an_unaliased_root_of_the_same_entity() {
        init_test_schema_if_needed();
        let payload =
            create_test_payload("query { tokens(first: 1) { id } top: tokens(first: 5) { id } }");
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(query.contains("  Token(limit: 1, order_by: {id: asc})"), "{query}");
        assert!(query.contains("top: Token(limit: 5, order_by: {id: asc})"), "{query}");
        assert_eq!(result.field_name_map.get("Token"), Some(&"tokens".to_string()));
        assert_eq!(result.field_name_map.get("top"), Some(&"top".to_string()));
    }

    #[test]
    fn aliased_root_with_fragments_keys_its_shape_plan_by_the_alias() {
        init_test_schema_if_needed();
        let payload = create_test_payload(
            "query { recent: userTransactions(first: 1) { id ... on Supply { amount } } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        assert!(result.shape_plans.contains_key("recent"), "{:?}", result.shape_plans.keys());
        assert!(!result.shape_plans.contains_key("UserTransaction"));
    }

    // ── operationName ────────────────────────────────────────────────────────

    #[test]
    fn operation_name_selects_one_of_several_operations() {
        init_test_schema_if_needed();
        let payload = json!({
            "query": "query A { tokens(first: 1) { id } }\nquery B { streams(first: 2) { id } }",
            "operationName": "B"
        });
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(query.starts_with("query B {"), "{query}");
        assert!(query.contains("Stream(limit: 2, order_by: {id: asc})"), "{query}");
        assert!(!query.contains("Token"), "{query}");
    }

    #[test]
    fn several_operations_without_an_operation_name_are_rejected() {
        let payload = create_test_payload("query A { tokens { id } } query B { streams { id } }");
        let err = convert_subgraph_to_hyperindex(&payload, None).unwrap_err();
        assert!(matches!(err, ConversionError::MissingOperationName), "{err}");
    }

    #[test]
    fn an_unknown_operation_name_is_rejected() {
        let payload = json!({
            "query": "query A { tokens { id } } query B { streams { id } }",
            "operationName": "C"
        });
        let err = convert_subgraph_to_hyperindex(&payload, None).unwrap_err();
        assert!(matches!(err, ConversionError::UnknownOperation(ref n) if n == "C"), "{err}");
    }

    #[test]
    fn only_fragments_the_selected_operation_reaches_are_kept() {
        init_test_schema_if_needed();
        let payload = json!({
            "query": "query A { tokens(first: 1) { ...TokenFields } }\nquery B { streams(first: 1) { ...StreamFields } }\nfragment TokenFields on Token { id symbol }\nfragment StreamFields on Stream { id ...Nested }\nfragment Nested on Stream { name }",
            "operationName": "B"
        });
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(query.contains("fragment StreamFields on Stream"), "{query}");
        assert!(query.contains("fragment Nested on Stream"), "{query}");
        assert!(!query.contains("TokenFields"), "{query}");
    }

    #[test]
    fn selected_operation_keeps_only_its_own_variables() {
        init_test_schema_if_needed();
        let payload = json!({
            "query": "query A($n: Int!) { tokens(first: $n) { id } }\nquery B($m: Int!) { streams(first: $m) { id } }",
            "operationName": "B",
            "variables": {"n": 1, "m": 2}
        });
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(query.contains("Stream(limit: $m, order_by: {id: asc})"), "{query}");
        assert_eq!(result.query["variables"], json!({"m": 2}));
    }

    #[test]
    fn a_single_operation_ignores_operation_name() {
        init_test_schema_if_needed();
        let plain = convert_subgraph_to_hyperindex(
            &create_test_payload("query A { streams(first: 1) { id } }"),
            None,
        )
        .unwrap();
        let named = convert_subgraph_to_hyperindex(
            &json!({"query": "query A { streams(first: 1) { id } }", "operationName": "Other"}),
            None,
        )
        .unwrap();
        assert_eq!(plain.query, named.query);
    }

    #[test]
    fn split_definitions_respects_strings_and_header_defaults() {
        let defs = split_definitions(
            "query A($w: X = {a: \"}\"}) { a } # trailing } comment\nfragment F on T { b }\n{ c }",
        )
        .unwrap();
        assert_eq!(defs.len(), 3);
        assert_eq!(defs[0].operation_name.as_deref(), Some("A"));
        assert_eq!(defs[1].fragment.as_deref(), Some("F"));
        assert!(defs[2].operation_name.is_none() && defs[2].fragment.is_none());
        assert_eq!(fragment_spreads("{ ...A ... on T { x } ... @include(if: $y) { z } ...B }"), vec!["A", "B"]);
    }

    // ── Nested-field and directive arguments ─────────────────────────────────

    #[test]
    fn termini_history_query_keeps_nested_sorting_and_paging() {
        init_test_schema_if_needed();
        let payload = create_test_payload(
            "query historyQuery { swapUsers(where: { id: \"0x2f62\" }) { id count swap(first: 100 skip: 0 orderBy: swapUserIndex orderDirection: desc) { id swapUserIndex tokenIn { id symbol } } } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(
            query.contains(
                "swap(limit: 100, offset: 0, order_by: [{swapUserIndex: desc}, {id: desc}]) { id swapUserIndex tokenIn { id symbol } }"
            ),
            "{query}"
        );
        assert!(
            query.contains("SwapUser(limit: 100, order_by: {id: asc}, where: {id: {_eq: \"0x2f62\"}})"),
            "{query}"
        );
    }

    #[test]
    fn nested_where_is_converted_against_the_child_entity() {
        init_test_schema_if_needed();
        let payload = create_test_payload(
            "query { swapUsers(first: 1) { swap(where: { fee_gt: \"0\", tokenIn: \"0xt\" }) { id } } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(
            query.contains("swap(limit: 100, order_by: {id: asc}, where: {fee: {_gt: \"0\"}, tokenIn: {id: {_eq: \"0xt\"}}})"),
            "{query}"
        );
    }

    #[test]
    fn nested_where_on_the_chain_route_gets_no_chain_filter() {
        init_test_schema_if_needed();
        let payload =
            create_test_payload("query { swapUsers(first: 1) { swap(first: 2) { id } } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("5031")).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(query.contains("swap(limit: 2, order_by: {id: asc})"), "{query}");
        assert_eq!(query.matches("chainId").count(), 1, "{query}");
    }

    #[test]
    fn nested_paging_variables_are_kept_and_stay_declared() {
        init_test_schema_if_needed();
        let payload = json!({
            "query": "query Q($first: Int!, $skip: Int!) { swapUsers(first: 1) { swap(first: $first, skip: $skip) { id } } }",
            "variables": {"first": 5, "skip": 10}
        });
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(query.contains("swap(limit: $first, offset: $skip, order_by: {id: asc})"), "{query}");
        assert!(query.starts_with("query Q($first: Int!, $skip: Int!)"), "{query}");
        assert_eq!(result.query["variables"], json!({"first": 5, "skip": 10}));
    }

    #[test]
    fn nested_order_by_variable_is_rebuilt_for_the_child_entity() {
        init_test_schema_if_needed();
        let payload = json!({
            "query": "query Q($ob: Swap_orderBy!, $dir: OrderDirection!) { swapUsers(first: 1) { swap(orderBy: $ob, orderDirection: $dir) { id } } }",
            "variables": {"ob": "swapUserIndex", "dir": "desc"}
        });
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        // The page size defaults; the variable order replaces the default sort.
        assert!(query.contains("swap(limit: 100, order_by: $ob)"), "{query}");
        assert!(query.contains("$ob: [Swap_order_by!]"), "{query}");
        assert!(!query.contains("$dir"), "{query}");
        assert_eq!(
            result.query["variables"]["ob"],
            json!([{"swapUserIndex": "desc"}, {"id": "desc"}])
        );
    }

    #[test]
    fn arguments_two_levels_down_are_converted() {
        init_test_schema_if_needed();
        let payload = create_test_payload(
            "query { swapUsers(first: 1) { swap(first: 1) { sender { swap(first: 3, orderBy: fee) { id } } } } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(query.contains("swap(limit: 1, order_by: {id: asc}) { sender { swap(limit: 3, order_by: [{fee: asc}, {id: asc}])"), "{query}");
    }

    #[test]
    fn arguments_inside_an_aliased_nested_field_are_converted() {
        init_test_schema_if_needed();
        let payload = create_test_payload(
            "query { swapUsers(first: 1) { latest: swap(first: 1, orderBy: swapUserIndex, orderDirection: desc) { id } } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(
            query.contains("latest: swap(limit: 1, order_by: [{swapUserIndex: desc}, {id: desc}])"),
            "{query}"
        );
    }

    #[test]
    fn arguments_inside_a_named_fragment_are_converted() {
        init_test_schema_if_needed();
        let payload = create_test_payload(
            "query { swapUsers(first: 1) { ...UserFields } } fragment UserFields on SwapUser { id swap(first: 2, orderBy: swapUserIndex, orderDirection: desc) { id } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(
            query.contains("fragment UserFields on SwapUser {id swap(limit: 2, order_by: [{swapUserIndex: desc}, {id: desc}]) { id }}"),
            "{query}"
        );
    }

    #[test]
    fn arguments_inside_an_inline_fragment_are_converted() {
        init_test_schema_if_needed();
        let payload = create_test_payload(
            "query { swapUsers(first: 1) { id ... on SwapUser { swap(first: 2) { id } } } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        // An inline fragment on the field's own type is flattened into it.
        assert!(query.contains("swap(limit: 2, order_by: {id: asc}) {"), "{query}");
    }

    #[test]
    fn directive_arguments_are_kept() {
        init_test_schema_if_needed();
        let payload = json!({
            "query": "query Q($x: Boolean!) { swaps(first: 1) { id fee @include(if: $x) tokenIn @skip(if: true) { id } } }",
            "variables": {"x": false}
        });
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(query.contains("fee @include(if: $x)"), "{query}");
        assert!(query.contains("tokenIn @skip(if: true) { id }"), "{query}");
        assert!(query.starts_with("query Q($x: Boolean!)"), "{query}");
        assert_eq!(result.query["variables"], json!({"x": false}));
    }

    #[test]
    fn a_parenthesis_inside_a_string_argument_is_not_mangled() {
        init_test_schema_if_needed();
        let payload = create_test_payload(
            "query { swapUsers(first: 1) { swap(where: { id: \"a(b)c\" }) { id } } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(query.contains("swap(limit: 100, order_by: {id: asc}, where: {id: {_eq: \"a(b)c\"}}) { id }"), "{query}");
    }

    #[test]
    fn nested_arguments_on_a_field_of_unknown_type_are_still_converted() {
        init_test_schema_if_needed();
        let payload = create_test_payload(
            "query { streams(first: 1) { actions(first: 5, orderBy: timestamp, orderDirection: desc) { id } } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(
            query.contains("actions(limit: 5, order_by: [{timestamp: desc}, {id: desc}]) { id }"),
            "{query}"
        );
    }

    #[test]
    fn root_arguments_separated_only_by_spaces() {
        init_test_schema_if_needed();
        let payload = create_test_payload(
            "query { streams(first: 2 skip: 4 orderBy: name orderDirection: desc where: { name: \"a b\" }) { id } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(
            query.contains("Stream(limit: 2, offset: 4, order_by: [{name: desc}, {id: desc}], where: {name: {_eq: \"a b\"}})"),
            "{query}"
        );
    }

    #[test]
    fn starts_with_argument_name_detects_the_next_argument() {
        assert!(starts_with_argument_name(" skip: 0"));
        assert!(starts_with_argument_name("orderDirection : desc"));
        assert!(!starts_with_argument_name(" 100"));
        assert!(!starts_with_argument_name(" desc"));
        assert!(!starts_with_argument_name(" {a: 1}"));
    }

    // ── Inline and / or ──────────────────────────────────────────────────────

    #[test]
    fn inline_or_of_two_equality_filters() {
        init_test_schema_if_needed();
        let payload = create_test_payload(
            "query { tokens(where: {or: [{symbol: \"USDT\"}, {symbol: \"USDC.e\"}]}) { id } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(
            query.contains("Token(limit: 100, order_by: {id: asc}, where: {_or: [{symbol: {_eq: \"USDT\"}}, {symbol: {_eq: \"USDC.e\"}}]})"),
            "{query}"
        );
    }

    #[test]
    fn inline_and_with_a_range_inside_one_item() {
        init_test_schema_if_needed();
        let payload = create_test_payload(
            "query { swaps(where: {and: [{fee_gt: 1, fee_lt: 5}, {trader: \"0xa\"}]}) { id } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(
            query.contains("Swap(limit: 100, order_by: {id: asc}, where: {_and: [{_and: [{fee: {_gt: 1}}, {fee: {_lt: 5}}]}, {trader: {_eq: \"0xa\"}}]})"),
            "{query}"
        );
    }

    #[test]
    fn inline_or_nested_inside_and() {
        init_test_schema_if_needed();
        let payload = create_test_payload(
            "query { swaps(where: {and: [{trader: \"0xa\"}, {or: [{fee: 1}, {fee: 2}]}]}) { id } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(
            query.contains("where: {_and: [{trader: {_eq: \"0xa\"}}, {_or: [{fee: {_eq: 1}}, {fee: {_eq: 2}}]}]}"),
            "{query}"
        );
    }

    #[test]
    fn inline_or_items_use_operators_and_relation_filters() {
        init_test_schema_if_needed();
        let payload = create_test_payload(
            "query { swaps(where: {or: [{fee_in: [1, 2]}, {tokenIn_: {symbol: \"USDT\"}}, {tokenIn: \"0xt\"}]}) { id } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(
            query.contains("_or: [{fee: {_in: [1, 2]}}, {tokenIn: {symbol: {_eq: \"USDT\"}}}, {tokenIn: {id: {_eq: \"0xt\"}}}]"),
            "{query}"
        );
    }

    #[test]
    fn inline_or_on_the_chain_route_keeps_the_chain_filter_outside() {
        init_test_schema_if_needed();
        let payload = create_test_payload(
            "query { tokens(where: {or: [{symbol: \"A\"}, {symbol: \"B\"}]}) { id } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, Some("5031")).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(
            query.contains("where: {chainId: {_eq: \"5031\"}, _or: [{symbol: {_eq: \"A\"}}, {symbol: {_eq: \"B\"}}]}"),
            "{query}"
        );
    }

    #[test]
    fn inline_or_items_with_variables_retype_them() {
        init_test_schema_if_needed();
        let payload = json!({
            "query": "query Q($a: String!, $b: String!) { swaps(where: {or: [{fee: $a}, {trader: $b}]}) { id } }",
            "variables": {"a": "1", "b": "0xb"}
        });
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(query.contains("_or: [{fee: {_eq: $a}}, {trader: {_eq: $b}}]"), "{query}");
        // `fee` is numeric, so `$a` has to be declared that way.
        assert!(query.contains("$a: numeric!"), "{query}");
        assert!(query.contains("$b: String!"), "{query}");
    }

    #[test]
    fn inline_or_inside_a_relation_filter() {
        init_test_schema_if_needed();
        let payload = create_test_payload(
            "query { swaps(where: {tokenIn_: {or: [{symbol: \"A\"}, {symbol: \"B\"}]}}) { id } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(
            query.contains("tokenIn: {_or: [{symbol: {_eq: \"A\"}}, {symbol: {_eq: \"B\"}}]}"),
            "{query}"
        );
    }

    #[test]
    fn split_list_items_handles_nesting_strings_and_missing_commas() {
        assert_eq!(
            split_list_items("[{a: \"x,}\"}, {b: [1, 2]} {c: 3}]"),
            vec!["{a: \"x,}\"}", "{b: [1, 2]}", "{c: 3}"]
        );
    }

    // ── Singular vs collection from the schema ───────────────────────────────

    #[test]
    fn singular_entity_ending_in_s_is_a_primary_key_lookup() {
        init_test_schema_if_needed();
        let payload = create_test_payload("query { status(id: \"1\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(query.contains("Status_by_pk(id: \"1\") {"), "{query}");
        assert_eq!(result.field_name_map.get("Status_by_pk"), Some(&"status".to_string()));
    }

    #[test]
    fn plural_of_a_singular_ending_in_s_is_a_collection() {
        init_test_schema_if_needed();
        let payload = create_test_payload("query { statuses(where: {id: \"1\"}) { id } }");
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(
            query.contains("  Status(limit: 100, order_by: {id: asc}, where: {id: {_eq: \"1\"}})"),
            "{query}"
        );
    }

    #[test]
    fn collection_filtered_by_id_alone_stays_a_collection() {
        init_test_schema_if_needed();
        let payload = create_test_payload("query { swapUsers(where: {id: \"0xa\"}) { id } }");
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(
            query.contains("  SwapUser(limit: 100, order_by: {id: asc}, where: {id: {_eq: \"0xa\"}})"),
            "{query}"
        );
    }

    #[test]
    fn root_fields_outside_the_schema_use_the_word_rules() {
        init_test_schema_if_needed();
        let payload =
            create_test_payload("query { stream(id: \"1\") { id } streams(first: 1) { id } }");
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(query.contains("Stream_by_pk(id: \"1\")"), "{query}");
        assert!(query.contains("  Stream(limit: 1, order_by: {id: asc})"), "{query}");
    }

    #[test]
    fn single_entity_fields_are_classified_from_the_schema() {
        init_test_schema_if_needed();
        let id_only: HashMap<String, String> = [("id".to_string(), "\"1\"".to_string())].into();
        assert_eq!(classify_root_field("swapUser", &id_only), ("SwapUser".to_string(), true));
        assert_eq!(classify_root_field("status", &id_only), ("Status".to_string(), true));
        assert_eq!(classify_root_field("swapUsers", &id_only), ("SwapUser".to_string(), false));
    }

    // ── block / subgraphError arguments ──────────────────────────────────────

    #[test]
    fn number_gte_adds_a_progress_root_and_no_where() {
        init_test_schema_if_needed();
        let payload = create_test_payload(
            "query { swaps(first: 2, block: {number_gte: 123}) { id } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(query.contains("  Swap(limit: 2, order_by: {id: asc}) {"), "{query}");
        assert!(!query.contains("block:") && !query.contains("where"), "{query}");
        assert!(query.contains("__converter_meta: "), "{query}");
        assert_eq!(result.min_block, Some(123));
    }

    #[test]
    fn number_gte_on_a_primary_key_lookup_keeps_the_lookup() {
        init_test_schema_if_needed();
        let payload = create_test_payload(
            "query { token(id: \"0xa\", block: {number_gte: 5}) { id } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(query.contains("Token_by_pk(id: \"0xa\")"), "{query}");
        assert_eq!(result.min_block, Some(5));
    }

    #[test]
    fn number_gte_variables_are_resolved_and_dropped() {
        init_test_schema_if_needed();
        let payload = json!({
            "query": "query Q($n: Int!, $b: Block_height) { swaps(first: 1, block: {number_gte: $n}) { id } tokens(first: 1, block: $b) { id } }",
            "variables": {"n": 7, "b": {"number_gte": 9}}
        });
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(query.starts_with("query Q {"), "{query}");
        assert_eq!(result.min_block, Some(9));
        assert_eq!(result.query["variables"], json!({}));
    }

    #[test]
    fn an_unset_block_variable_sets_no_requirement() {
        init_test_schema_if_needed();
        let payload = json!({
            "query": "query Q($b: Block_height) { swaps(first: 1, block: $b) { id } }",
            "variables": {}
        });
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        assert_eq!(result.min_block, None);
    }

    #[test]
    fn historical_block_arguments_are_refused() {
        init_test_schema_if_needed();
        for query in [
            "query { swaps(block: {number: 100}) { id } }",
            "query { swaps(block: {hash: \"0xabc\"}) { id } }",
        ] {
            let err = convert_subgraph_to_hyperindex(&create_test_payload(query), None).unwrap_err();
            assert!(matches!(err, ConversionError::UnsupportedBlockConstraint(_)), "{query}: {err}");
        }
        let payload = json!({
            "query": "query Q($b: Block_height) { swaps(block: $b) { id } }",
            "variables": {"b": {"number": 100}}
        });
        let err = convert_subgraph_to_hyperindex(&payload, None).unwrap_err();
        assert!(matches!(err, ConversionError::UnsupportedBlockConstraint(_)), "{err}");
    }

    #[test]
    fn subgraph_error_argument_is_dropped() {
        init_test_schema_if_needed();
        let payload =
            create_test_payload("query { swaps(first: 1, subgraphError: allow) { id } }");
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(query.contains("  Swap(limit: 1, order_by: {id: asc}) {"), "{query}");
        assert!(!query.contains("subgraphError"), "{query}");
        assert_eq!(result.min_block, None);
    }

    #[test]
    fn progress_root_shapes() {
        assert_eq!(
            progress_root("__converter_meta", Some("5031"), true, false),
            "  __converter_meta: _meta(where: {chainId: {_eq: 5031}}) {\n    progressBlock\n  }"
        );
        assert_eq!(
            progress_root("_meta", None, true, true),
            "  _meta {\n    progressBlock progressBlockTime\n  }"
        );
        assert_eq!(
            progress_root("_meta", Some("1"), false, false),
            "  _meta: chain_metadata(where: {chain_id: {_eq: 1}}) {\n    latest_processed_block\n  }"
        );
    }

    // ── _meta ────────────────────────────────────────────────────────────────

    #[test]
    fn meta_block_number_and_timestamp_read_hasura_meta() {
        init_test_schema_if_needed();
        let payload = create_test_payload("query { _meta { block { number timestamp } } }");
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        assert_eq!(
            result.query["query"].as_str().unwrap(),
            "query {\n  _meta {\n    progressBlock progressBlockTime\n  }\n}"
        );
        assert_eq!(
            result.meta_selections.first().cloned(),
            Some(MetaSelection {
                block_number: true,
                block_timestamp: true,
                key: "_meta".to_string(),
                ..MetaSelection::default()
            })
        );
    }

    #[test]
    fn meta_without_hasura_meta_falls_back_to_chain_metadata() {
        assert_eq!(
            progress_root("_meta", None, false, false),
            "  _meta: chain_metadata {\n    latest_processed_block\n  }"
        );
        let selection = convert_meta_selection("block { number }", false, true, "_meta".to_string());
        assert!(selection.unwrap().block_number);
    }

    #[test]
    fn meta_still_refuses_fields_without_a_hyperindex_source() {
        init_test_schema_if_needed();
        let payload = create_test_payload(
            "query { _meta { deployment block { number hash timestamp } hasIndexingErrors } }",
        );
        match convert_subgraph_to_hyperindex(&payload, None) {
            Err(ConversionError::ComplexMetaQuery(fields)) => {
                assert_eq!(fields, "block.hash, hasIndexingErrors")
            }
            other => panic!("expected ComplexMetaQuery, got {:?}", other.map(|r| r.query)),
        }
    }

    #[test]
    fn meta_alongside_entity_roots() {
        init_test_schema_if_needed();
        let payload = create_test_payload(
            "query { _meta { block { number } } tokens(first: 1) { id symbol } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(query.contains("  Token(limit: 1, order_by: {id: asc}) {"), "{query}");
        assert!(query.contains("  _meta {\n    progressBlock\n  }"), "{query}");
        assert_eq!(result.meta_selections.first().cloned().unwrap().key, "_meta");
        assert_eq!(result.field_name_map.get("Token"), Some(&"tokens".to_string()));
    }

    #[test]
    fn aliased_meta_is_answered_under_its_alias() {
        init_test_schema_if_needed();
        let payload = create_test_payload("query { status: _meta { block { number } } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("5031")).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(
            query.contains("status: _meta(where: {chainId: {_eq: 5031}}) {"),
            "{query}"
        );
        assert_eq!(result.meta_selections.first().cloned().unwrap().key, "status");
        assert_eq!(result.field_name_map.get("status"), Some(&"status".to_string()));
    }

    #[test]
    fn meta_with_a_number_gte_argument() {
        init_test_schema_if_needed();
        let payload =
            create_test_payload("query { _meta(block: {number_gte: 42}) { block { number } } }");
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(query.contains("  _meta {\n    progressBlock\n  }"), "{query}");
        assert!(query.contains("__converter_meta: _meta {"), "{query}");
        assert_eq!(result.min_block, Some(42));
    }

    #[test]
    fn a_root_merely_containing_meta_is_an_entity() {
        init_test_schema_if_needed();
        let payload = create_test_payload("query { token_metadatas(first: 1) { id } }");
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        assert!(result.meta_selections.first().cloned().is_none());
        assert!(!result.query["query"].as_str().unwrap().contains("progressBlock"));
    }

    // ── Case-sensitive string operators ──────────────────────────────────────

    #[test]
    fn plain_string_operators_are_case_sensitive_and_nocase_ones_are_not() {
        init_test_schema_if_needed();
        let payload = create_test_payload(
            "query { tokens(where: {symbol_contains: \"usd\", name_starts_with_nocase: \"wr\"}) { id } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(query.contains("symbol: {_like: \"%usd%\"}"), "{query}");
        assert!(query.contains("name: {_ilike: \"wr%\"}"), "{query}");
    }

    #[test]
    fn like_patterns_follow_graph_node() {
        // graph-node does not escape `%`/`_`, and `_contains` keeps a value that
        // already starts or ends with `%` as it is.
        assert_eq!(like_pattern("a_b", LikeShape::Contains), "%a_b%");
        assert_eq!(like_pattern("%ab", LikeShape::Contains), "%ab");
        assert_eq!(like_pattern("ab%", LikeShape::Contains), "ab%");
        assert_eq!(like_pattern("%ab", LikeShape::StartsWith), "%ab%");
        assert_eq!(like_pattern("ab", LikeShape::EndsWith), "%ab");
    }

    #[test]
    fn negated_case_sensitive_operator_in_a_relation_filter() {
        init_test_schema_if_needed();
        let payload = create_test_payload(
            "query { swaps(where: {tokenIn_: {symbol_not_ends_with: \".e\"}}) { id } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(query.contains("tokenIn: {_not: {symbol: {_like: \"%.e\"}}}"), "{query}");
    }

    #[test]
    fn variable_filters_use_the_same_operators() {
        init_test_schema_if_needed();
        let out = translate_subgraph_filter_value(
            &json!({"name_contains": "ETH", "name_not_contains_nocase": "btc", "symbol_starts_with": "W"}),
            "Pair",
        );
        assert_eq!(
            out,
            json!({
                "name": {"_like": "%ETH%"},
                "symbol": {"_like": "W%"},
                "_not": {"name": {"_ilike": "%btc%"}}
            })
        );
    }

    #[test]
    fn string_match_filter_prefers_the_longest_suffix() {
        let (field, filter) = string_match_filter("name_not_contains_nocase").unwrap();
        assert_eq!(field, "name");
        assert!(filter.negated && filter.operator == "_ilike");
        let (field, filter) = string_match_filter("name_contains").unwrap();
        assert_eq!(field, "name");
        assert!(!filter.negated && filter.operator == "_like");
        assert!(string_match_filter("name_in").is_none());
    }

    // ── graph-node defaults: first 100, id asc ───────────────────────────────

    #[test]
    fn collection_without_first_or_order_by_gets_graph_node_defaults() {
        init_test_schema_if_needed();
        let payload = create_test_payload("query { tokens { id } }");
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        assert_eq!(
            result.query["query"].as_str().unwrap(),
            "query {\n  Token(limit: 100, order_by: {id: asc}) {\n    id\n  }\n}"
        );
    }

    #[test]
    fn nested_list_without_arguments_gets_graph_node_defaults() {
        init_test_schema_if_needed();
        let payload = create_test_payload("query { swapUsers(first: 1) { id swap { id } } }");
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(query.contains("id swap(limit: 100, order_by: {id: asc}) { id }"), "{query}");
    }

    #[test]
    fn single_objects_get_no_defaults() {
        init_test_schema_if_needed();
        let payload = create_test_payload(
            "query { token(id: \"0xa\") { id } swaps(first: 1, orderBy: fee) { tokenIn { id } } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(query.contains("Token_by_pk(id: \"0xa\") {"), "{query}");
        assert!(query.contains("tokenIn { id }"), "{query}");
    }

    #[test]
    fn explicit_first_and_order_by_replace_the_defaults() {
        init_test_schema_if_needed();
        let payload = json!({
            "query": "query Q($n: Int!, $ob: Swap_orderBy!) { swaps(first: $n, orderBy: $ob) { id } tokens(orderBy: symbol) { id } }",
            "variables": {"n": 3, "ob": "fee"}
        });
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(query.contains("Swap(limit: $n, order_by: $ob)"), "{query}");
        assert!(
            query.contains("Token(limit: 100, order_by: [{symbol: asc}, {id: asc}])"),
            "{query}"
        );
    }

    #[test]
    fn first_null_is_the_default_page() {
        init_test_schema_if_needed();
        let payload = create_test_payload("query { tokens(first: null, orderBy: id) { id } }");
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(query.contains("Token(limit: 100, order_by: {id: asc})"), "{query}");
    }

    #[test]
    fn defaults_switched_off_leave_the_arguments_as_before() {
        init_test_schema_if_needed();
        let mut ctx = ArgContext {
            defaults: GraphNodeDefaults {
                first: None,
                order_by_id: false,
            },
            ..ArgContext::default()
        };
        let mut params = HashMap::new();
        params.insert("skip".to_string(), "5".to_string());
        assert_eq!(
            convert_collection_args(&params, "Token", None, &mut ctx).unwrap(),
            vec!["offset: 5".to_string()]
        );
        let mut ctx = ArgContext::default();
        assert_eq!(
            convert_collection_args(&params, "Token", None, &mut ctx).unwrap(),
            vec![
                "limit: 100".to_string(),
                "offset: 5".to_string(),
                "order_by: {id: asc}".to_string()
            ]
        );
    }

    #[test]
    fn default_switches_read_from_the_environment() {
        let env = |pairs: &'static [(&'static str, &'static str)]| {
            move |name: &str| {
                pairs
                    .iter()
                    .find(|(k, _)| *k == name)
                    .map(|(_, v)| v.to_string())
            }
        };
        assert_eq!(GraphNodeDefaults::from_env(env(&[])), GraphNodeDefaults::GRAPH_NODE);
        assert_eq!(
            GraphNodeDefaults::from_env(env(&[
                ("CONVERTER_DEFAULT_FIRST", "off"),
                ("CONVERTER_DEFAULT_ORDER_ID", "OFF")
            ])),
            GraphNodeDefaults {
                first: None,
                order_by_id: false
            }
        );
        assert_eq!(
            GraphNodeDefaults::from_env(env(&[("CONVERTER_DEFAULT_FIRST", "0")])).first,
            None
        );
        assert_eq!(
            GraphNodeDefaults::from_env(env(&[("CONVERTER_DEFAULT_FIRST", "250")])).first,
            Some(250)
        );
        assert_eq!(
            GraphNodeDefaults::from_env(env(&[("CONVERTER_DEFAULT_FIRST", "lots")])).first,
            Some(100)
        );
    }

    #[test]
    fn conditions_on_one_field_come_out_in_a_fixed_order() {
        init_test_schema_if_needed();
        for _ in 0..20 {
            let payload = create_test_payload(
                "query { swaps(where: {fee_lt: 5, fee_gt: 1, tokenIn_: {symbol_starts_with: \"W\", symbol_not: \"X\", decimals: 18}}) { id } }",
            );
            let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
            let query = result.query["query"].as_str().unwrap();
            assert!(query.contains("_and: [{fee: {_gt: 1}}, {fee: {_lt: 5}}]"), "{query}");
            assert!(
                query.contains("tokenIn: {decimals: {_eq: 18}, _and: [{symbol: {_neq: \"X\"}}, {symbol: {_like: \"W%\"}}]}"),
                "{query}"
            );
        }
    }

    #[test]
    fn inline_negations_on_two_fields_are_separate() {
        init_test_schema_if_needed();
        let payload = create_test_payload(
            "query { pairs(where: {name_not_contains: \"a\", symbol_not_starts_with: \"b\"}) { id } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        // One `_not` key, the other negation in `_and`: not "not (a and b)".
        assert!(
            query.contains("where: {_not: {name: {_like: \"%a%\"}}, _and: [{_not: {symbol: {_like: \"b%\"}}}]}"),
            "{query}"
        );
    }

    #[test]
    fn variable_negations_on_two_fields_are_separate() {
        init_test_schema_if_needed();
        let out = translate_subgraph_filter_value(
            &json!({"name_not_contains": "a", "symbol_not_starts_with": "b"}),
            "Pair",
        );
        assert_eq!(
            out,
            json!({
                "_not": {"name": {"_like": "%a%"}},
                "_and": [{"_not": {"symbol": {"_like": "b%"}}}]
            })
        );
    }

    #[test]
    fn variable_range_and_connective_share_one_and() {
        init_test_schema_if_needed();
        let out = translate_subgraph_filter_value(
            &json!({"amount_gt": 1, "amount_lt": 9, "and": [{"name": "x"}]}),
            "Trade",
        );
        assert_eq!(
            out,
            json!({
                "amount": {"_gt": 1},
                "_and": [{"amount": {"_lt": 9}}, {"name": {"_eq": "x"}}]
            })
        );
    }


    #[test]
    fn tiebreak_switch_values() {
        assert!(tiebreak_setting(None));
        assert!(tiebreak_setting(Some("on")));
        assert!(tiebreak_setting(Some("garbage")));
        for off in ["off", "OFF", " 0 ", "false", "no"] {
            assert!(!tiebreak_setting(Some(off)), "{off}");
        }
    }


    #[test]
    fn contains_with_a_variable_builds_the_pattern_from_its_value() {
        init_test_schema_if_needed();
        let payload = json!({
            "query": "query Q($q: String!) { pairs(where: {name_contains: $q}) { id } }",
            "variables": {"q": "eth"}
        });
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(query.contains("where: {name: {_like: $q__contains}}"), "{query}");
        assert!(query.starts_with("query Q($q__contains: String!)"), "{query}");
        assert!(!query.contains("$q:"), "{query}");
        assert_eq!(result.query["variables"], json!({"q__contains": "%eth%"}));
        assert_eq!(result.dropped_variables, vec!["q".to_string()]);
    }

    #[test]
    fn one_variable_in_several_string_filters_and_an_equality() {
        init_test_schema_if_needed();
        let payload = json!({
            "query": "query Q($q: String) { pairs(where: {name_starts_with: $q, name_not_ends_with_nocase: $q, symbol: $q}) { id } }",
            "variables": {"q": "W"}
        });
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(query.contains("name: {_like: $q__starts_with}"), "{query}");
        assert!(query.contains("_not: {name: {_ilike: $q__ends_with}}"), "{query}");
        assert!(query.contains("symbol: {_eq: $q}"), "{query}");
        // `$q` is still read by the equality, so it stays.
        assert!(
            query.starts_with("query Q($q: String, $q__ends_with: String, $q__starts_with: String)"),
            "{query}"
        );
        assert_eq!(
            result.query["variables"],
            json!({"q": "W", "q__starts_with": "W%", "q__ends_with": "%W"})
        );
        assert!(result.dropped_variables.is_empty());
    }

    #[test]
    fn pattern_variable_uses_the_declared_default() {
        init_test_schema_if_needed();
        let payload = json!({
            "query": "query Q($q: String = \"ab\") { pairs(where: {name_contains: $q}) { id } }"
        });
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        assert_eq!(result.query["variables"], json!({"q__contains": "%ab%"}));
    }

    #[test]
    fn null_pattern_variable_is_left_unset() {
        init_test_schema_if_needed();
        let payload = json!({
            "query": "query Q($q: String) { pairs(where: {name_contains: $q}) { id } }",
            "variables": {"q": null}
        });
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        assert_eq!(result.query["variables"], json!({}));
    }

    #[test]
    fn pattern_variables_in_relation_filters_and_nested_fields() {
        init_test_schema_if_needed();
        let payload = json!({
            "query": "query Q($s: String!, $t: String!) { swaps(where: {tokenIn_: {symbol_ends_with: $s}}) { id } swapUsers { swap(where: {trader_contains: $t}) { id } } }",
            "variables": {"s": ".e", "t": "0xab"}
        });
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(query.contains("tokenIn: {symbol: {_like: $s__ends_with}}"), "{query}");
        assert!(query.contains("trader: {_like: $t__contains}"), "{query}");
        assert_eq!(
            result.query["variables"],
            json!({"s__ends_with": "%.e", "t__contains": "%0xab%"})
        );
    }


    #[test]
    fn where_variable_on_the_chain_route_keeps_the_chain_filter() {
        init_test_schema_if_needed();
        let payload = json!({
            "query": "query Q($w: Swap_filter) { swaps(where: $w) { id } }",
            "variables": {"w": {"fee_gt": 3}}
        });
        let result = convert_subgraph_to_hyperindex(&payload, Some("5031")).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(query.contains("where: $w"), "{query}");
        assert_eq!(
            result.query["variables"]["w"],
            json!({"_and": [{"fee": {"_gt": 3}}, {"chainId": {"_eq": "5031"}}]})
        );
    }

    #[test]
    fn missing_or_empty_where_variable_on_the_chain_route() {
        init_test_schema_if_needed();
        for variables in [None, Some(json!({})), Some(json!({"w": null})), Some(json!({"w": {}}))] {
            let mut payload = json!({
                "query": "query Q($w: Swap_bool_exp) { swaps(where: $w) { id } }"
            });
            if let Some(v) = variables.clone() {
                payload["variables"] = v;
            }
            let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
            assert_eq!(
                result.query["variables"],
                json!({"w": {"chainId": {"_eq": "1"}}}),
                "{variables:?}"
            );
        }
    }

    #[test]
    fn where_variable_off_the_chain_route_is_untouched() {
        init_test_schema_if_needed();
        let payload = json!({
            "query": "query Q($w: Swap_bool_exp) { swaps(where: $w) { id } }",
            "variables": {"w": {"id": {"_eq": "a"}}}
        });
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        assert_eq!(result.query["variables"], json!({"w": {"id": {"_eq": "a"}}}));
    }


    #[test]
    fn lookup_on_the_chain_route_filters_by_chain() {
        init_test_schema_if_needed();
        let payload = json!({
            "query": "query Q($id: ID!) { token(id: $id) { id } mine: token(id: \"0xb\") { id } }",
            "variables": {"id": "0xa"}
        });
        let result = convert_subgraph_to_hyperindex(&payload, Some("5031")).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(
            query.contains("Token_by_pk: Token(limit: 1, where: {chainId: {_eq: \"5031\"}, id: {_eq: $id}}) {"),
            "{query}"
        );
        assert!(
            query.contains("mine: Token(limit: 1, where: {chainId: {_eq: \"5031\"}, id: {_eq: \"0xb\"}}) {"),
            "{query}"
        );
        assert!(query.starts_with("query Q($id: String!)"), "{query}");
        assert_eq!(result.single_row_keys, vec!["Token_by_pk".to_string(), "mine".to_string()]);
    }

    #[test]
    fn lookup_off_the_chain_route_stays_by_pk() {
        init_test_schema_if_needed();
        let payload = create_test_payload("query { token(id: \"0xa\") { id } }");
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(query.contains("Token_by_pk(id: \"0xa\")"), "{query}");
        assert!(result.single_row_keys.is_empty());
    }


    #[test]
    fn null_order_by_variable_gets_the_default_order() {
        init_test_schema_if_needed();
        let payload = json!({
            "query": "query Q($ob: Swap_orderBy, $dir: OrderDirection) { swaps(orderBy: $ob, orderDirection: $dir) { id } }",
            "variables": {"ob": null, "dir": "desc"}
        });
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        // A lone direction is ignored, as in graph-node.
        assert_eq!(result.query["variables"]["ob"], json!([{"id": "asc"}]));
    }

    #[test]
    fn null_first_variable_gets_the_default_page() {
        init_test_schema_if_needed();
        let payload = json!({
            "query": "query Q($first: Int, $n: Int) { swaps(first: $first) { id } swapUsers { swap(first: $n) { id } } }",
            "variables": {"first": null}
        });
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(query.contains("Swap(limit: $first"), "{query}");
        assert!(query.contains("swap(limit: $n"), "{query}");
        // Explicit null and left out both mean graph-node's 100.
        assert_eq!(result.query["variables"], json!({"first": 100, "n": 100}));
    }

    #[test]
    fn first_variable_with_a_value_or_a_declared_default_is_kept() {
        init_test_schema_if_needed();
        let payload = json!({
            "query": "query Q($first: Int, $d: Int = 7, $req: Int!) { a: swaps(first: $first) { id } b: swaps(first: $d) { id } c: swaps(first: $req) { id } }",
            "variables": {"first": 5}
        });
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        // `$d` is left to Hasura, which applies its default; a missing `$req` is
        // the client's error to see.
        assert_eq!(result.query["variables"], json!({"first": 5}));
    }


    #[test]
    fn deployment_id_from_hyperindex_urls() {
        for (url, id) in [
            ("https://indexer.hyperindex.xyz/b232c6d/v1/graphql", Some("b232c6d")),
            ("https://indexer.eu.hyperindex.xyz/303e73c/v1/graphql", Some("303e73c")),
            ("https://indexer.us.hyperindex.xyz/32230f9/v1/graphql/", Some("32230f9")),
            ("https://0xostium-7cfbfa6.dedicated.hyperindex.xyz/v1/graphql", Some("0xostium-7cfbfa6")),
            // The converter's own endpoint is not the indexer's.
            ("https://indexer.us.hyperindex.xyz/32230f9/converter", None),
            ("https://indexer.hyperindex.xyz/v1/graphql", None),
            ("http://localhost:8080/v1/graphql", None),
            ("not a url", None),
        ] {
            assert_eq!(deployment_id_from_url(url).as_deref(), id, "{url}");
        }
    }

    #[test]
    fn meta_deployment_is_served() {
        init_test_schema_if_needed();
        let payload = create_test_payload("query { _meta { deployment block { number } } }");
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let selection = result.meta_selections.first().cloned().unwrap();
        assert!(selection.deployment && selection.block_number);
        let query = result.query["query"].as_str().unwrap();
        // Answered locally: nothing about it goes upstream.
        assert!(!query.contains("deployment"), "{query}");
    }

    #[test]
    fn meta_deployment_needs_a_known_deployment() {
        match convert_meta_selection("deployment", true, false, "_meta".to_string()) {
            Err(ConversionError::ComplexMetaQuery(fields)) => assert_eq!(fields, "deployment"),
            other => panic!("expected ComplexMetaQuery, got {other:?}"),
        }
    }


    // ── review of PR #21 ─────────────────────────────────────────────────────

    #[test]
    fn arguments_inside_an_untyped_inline_fragment_are_converted() {
        init_test_schema_if_needed();
        for query in [
            "query Q($x: Boolean!) { swapUsers(first: 1) { id ... @include(if: $x) { swap(first: 2, orderBy: fee, orderDirection: desc) { id } } } }",
            "query { swapUsers(first: 1) { id ... { swap(first: 2, orderBy: fee, orderDirection: desc) { id } } } }",
        ] {
            let result = convert_subgraph_to_hyperindex(&create_test_payload(query), None).unwrap();
            let converted = result.query["query"].as_str().unwrap();
            assert!(
                converted.contains("swap(limit: 2, order_by: [{fee: desc}, {id: desc}])"),
                "{query}\n{converted}"
            );
        }
    }

    #[test]
    fn directives_on_root_fields_are_kept() {
        init_test_schema_if_needed();
        let payload = create_test_payload(
            "query Q($x: Boolean!, $y: Boolean!) { tokens(first: 1) @include(if: $x) { id } swap(id: \"1\") @skip(if: $y) { id } _meta @include(if: $x) { block { number } } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let converted = result.query["query"].as_str().unwrap();
        assert!(
            converted.contains("Token(limit: 1, order_by: {id: asc}) @include(if: $x) {"),
            "{converted}"
        );
        assert!(converted.contains("Swap_by_pk(id: \"1\") @skip(if: $y) {"), "{converted}");
        assert!(converted.contains("progressBlock") && converted.contains("@include(if: $x) {"), "{converted}");
        assert!(!converted.contains("Include"), "{converted}");
    }

    #[test]
    fn a_directive_without_arguments_on_a_root_field() {
        init_test_schema_if_needed();
        let payload = create_test_payload("query { tokens @cached { id } }");
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let converted = result.query["query"].as_str().unwrap();
        assert!(converted.contains("Token(limit: 100, order_by: {id: asc}) @cached {"), "{converted}");
    }

    #[test]
    fn a_header_with_an_object_default_is_kept_whole() {
        init_test_schema_if_needed();
        let payload = create_test_payload(
            "query Q($w: Token_filter = {symbol: \"A\", decimals_gt: 6}, $n: Int) { tokens(first: $n, where: $w) { id } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let converted = result.query["query"].as_str().unwrap();
        // The default is subgraph syntax, so it leaves the header and is sent,
        // translated, as the value.
        assert!(converted.starts_with("query Q($w: Token_bool_exp, $n: Int) {"), "{converted}");
        assert!(converted.contains("Token(limit: $n, order_by: {id: asc}, where: $w)"), "{converted}");
        assert_eq!(
            result.query["variables"]["w"],
            json!({"symbol": {"_eq": "A"}, "decimals": {"_gt": 6}})
        );
    }

    #[test]
    fn variable_declarations_with_commas_in_a_default() {
        let decls = parse_variable_declarations("query Q($w: T = {a: 1, b: [2, 3]}, $n: Int!)");
        assert_eq!(decls.get("w").map(String::as_str), Some("T = {a: 1, b: [2, 3]}"));
        assert_eq!(decls.get("n").map(String::as_str), Some("Int!"));
    }

    #[test]
    fn where_default_on_the_chain_route_keeps_both_filters() {
        init_test_schema_if_needed();
        let payload = create_test_payload(
            "query Q($w: Token_filter = {symbol: \"A\"}) { tokens(where: $w) { id } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        assert_eq!(
            result.query["variables"]["w"],
            json!({"_and": [{"symbol": {"_eq": "A"}}, {"chainId": {"_eq": "1"}}]})
        );
        // A value the client sends replaces the default, as in GraphQL.
        let mut payload = payload;
        payload["variables"] = json!({"w": {"symbol": "B"}});
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        assert_eq!(
            result.query["variables"]["w"],
            json!({"_and": [{"symbol": {"_eq": "B"}}, {"chainId": {"_eq": "1"}}]})
        );
    }

    #[test]
    fn a_required_where_variable_left_out_is_not_filled_in() {
        init_test_schema_if_needed();
        // graph-node rejects the request; serving the whole chain instead would
        // be an answer to a question that was not asked.
        let payload = create_test_payload("query Q($w: Token_filter!) { tokens(where: $w) { id } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        assert!(result.query["variables"].get("w").is_none(), "{}", result.query);
    }

    #[test]
    fn graphql_literals_become_json() {
        assert_eq!(
            graphql_literal_to_json("{a: 1, b: [\"x\", true, null], c: ENUM, d: 1.5}"),
            Some(json!({"a": 1, "b": ["x", true, null], "c": "ENUM", "d": 1.5}))
        );
        assert_eq!(graphql_literal_to_json("$v"), None);
        assert_eq!(graphql_literal_to_json("{a: "), None);
    }

    #[test]
    fn number_gte_variable_values_follow_graph_node() {
        init_test_schema_if_needed();
        let query = "query Q($n: Int) { tokens(first: 1, block: {number_gte: $n}) { id } }";
        let run = |variables: Value| {
            convert_subgraph_to_hyperindex(&json!({"query": query, "variables": variables}), None)
        };
        // Missing or null: graph-node cannot read a block number, so it errors.
        for variables in [json!({}), json!({"n": null}), json!({"n": "abc"}), json!({"n": 3000000000u64})] {
            match run(variables.clone()) {
                Err(ConversionError::InvalidBlockNumber(_)) => {}
                other => panic!("{variables}: expected InvalidBlockNumber, got {:?}", other.map(|r| r.min_block)),
            }
        }
        // graph-node compares `latest >= N`, so a negative N is always met.
        assert_eq!(run(json!({"n": -5})).unwrap().min_block, None);
        assert_eq!(run(json!({"n": 12})).unwrap().min_block, Some(12));
        assert_eq!(run(json!({"n": "12"})).unwrap().min_block, Some(12));
    }

    #[test]
    fn number_gte_literals_and_defaults() {
        init_test_schema_if_needed();
        let min_block = |query: &str| {
            convert_subgraph_to_hyperindex(&create_test_payload(query), None).map(|r| r.min_block)
        };
        assert_eq!(min_block("{ tokens(block: {number_gte: -5}) { id } }").unwrap(), None);
        assert_eq!(min_block("{ tokens(block: {number_gte: 9}) { id } }").unwrap(), Some(9));
        assert!(matches!(
            min_block("{ tokens(block: {number_gte: \"x\"}) { id } }"),
            Err(ConversionError::InvalidBlockNumber(_))
        ));
        assert_eq!(
            min_block("query Q($n: Int = 7) { tokens(block: {number_gte: $n}) { id } }").unwrap(),
            Some(7)
        );
    }

    #[test]
    fn block_variable_values_follow_graph_node() {
        init_test_schema_if_needed();
        let query = "query Q($b: Block_height) { tokens(block: $b) { id } }";
        let run = |variables: Value| {
            convert_subgraph_to_hyperindex(&json!({"query": query, "variables": variables}), None)
        };
        // A null `block` means the latest block.
        assert_eq!(run(json!({"b": null})).unwrap().min_block, None);
        assert_eq!(run(json!({})).unwrap().min_block, None);
        assert_eq!(run(json!({"b": {"number_gte": 4}})).unwrap().min_block, Some(4));
        assert!(matches!(
            run(json!({"b": {}})),
            Err(ConversionError::UnsupportedBlockConstraint(_))
        ));
        assert!(matches!(
            run(json!({"b": {"number_gte": null}})),
            Err(ConversionError::InvalidBlockNumber(_))
        ));
    }

    #[test]
    fn several_aliased_meta_roots_are_served() {
        init_test_schema_if_needed();
        let payload = create_test_payload(
            "{ a: _meta { block { number } } b: _meta { deployment } _meta { block { number } } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let keys: Vec<&str> = result.meta_selections.iter().map(|m| m.key.as_str()).collect();
        assert_eq!(keys, vec!["a", "b", "_meta"]);
        let query = result.query["query"].as_str().unwrap();
        assert!(query.contains("a: _meta") && query.contains("b: _meta"), "{query}");
    }

    #[test]
    fn the_same_meta_key_twice_is_one_field() {
        init_test_schema_if_needed();
        let payload = create_test_payload("{ _meta { block { number } } _meta { block { number } } }");
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        assert_eq!(result.meta_selections.len(), 1);
    }

    #[test]
    fn operation_name_is_ignored_for_a_single_operation_as_in_graph_node() {
        // graph-node never reads `operationName`; with one operation it runs it.
        let payload = json!({"query": "query A { tokens(first: 1) { id } }", "operationName": "B"});
        assert!(convert_subgraph_to_hyperindex(&payload, None).is_ok());
    }

}
