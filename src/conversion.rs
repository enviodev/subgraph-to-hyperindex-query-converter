use serde_json::Value;
use std::collections::HashMap;
use thiserror::Error;

use crate::schema;

#[derive(Error, Debug)]
pub enum ConversionError {
    #[error("Invalid GraphQL query format")]
    InvalidQueryFormat,
    #[error("Missing required field: {0}")]
    MissingField(String),
    #[error("Unsupported filter: {0}")]
    UnsupportedFilter(String),
    #[error("Complex _meta queries are not supported. Only _meta {{ block {{ number }} }} is currently available")]
    ComplexMetaQuery,
}

/// Result of query conversion, including the converted query and field name mappings
#[derive(Debug)]
pub struct ConversionResult {
    pub query: Value,
    /// Maps Hyperindex field names (e.g., "LpAction") to original query field names (e.g., "lpActions")
    pub field_name_map: HashMap<String, String>,
    /// Indicates if this was a _meta query (needs special response transformation)
    pub is_meta_query: bool,
    /// Maps variable names (without `$`) to the Hyperindex entity name (e.g., "UniswapDayData")
    /// for variables that were declared with a subgraph `*_filter` type and used as a
    /// top-level `where: $var` argument. The caller is expected to translate the JSON
    /// value of each such variable via `translate_subgraph_filter_value` before forwarding
    /// upstream.
    pub filter_variable_entities: HashMap<String, String>,
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
        .ok_or(ConversionError::InvalidQueryFormat)?;

    tracing::info!("Converting query: {}", query);

    // Check if this is an introspection query **only by operation name**.
    // We ONLY bypass conversion when the operation name is `IntrospectionQuery`.
    let trimmed = query.trim_start();
    let is_introspection = if trimmed.starts_with("query") {
        // Look at the header before the first '{'
        let header_end = trimmed.find('{').unwrap_or_else(|| trimmed.len());
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
            is_meta_query: false,
            filter_variable_entities: HashMap::new(),
        });
    }

    // Check if this is a _meta query before conversion
    let is_meta_query = query.contains("_meta");

    // Parse the GraphQL query (simplified parsing for now)
    let MainConversion {
        converted_query,
        field_name_map,
        filter_variable_entities,
    } = convert_query_structure(query, chain_id)?;

    // Build the result with query and optionally variables
    let mut result = serde_json::json!({
        "query": converted_query
    });

    // Extract variables, translating any subgraph `*_filter`-typed variables that
    // were used as `where: $var`. We rewrite the variable value in-place from
    // subgraph filter shape (flat keys like `date_gt`) into Hasura `bool_exp` shape
    // (`date: {_gt: ...}`). This keeps clients that still send subgraph-shaped
    // filter JSON working transparently after the query-level rewrite.
    if let Some(variables) = payload.get("variables") {
        let mut variables = variables.clone();
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
        result["variables"] = variables;
    }

    Ok(ConversionResult {
        query: result,
        field_name_map,
        is_meta_query,
        filter_variable_entities,
    })
}

/// Internal aggregate returned by `convert_query_structure` / `convert_main_query`.
struct MainConversion {
    converted_query: String,
    field_name_map: HashMap<String, String>,
    /// Variable name (without `$`) -> entity name (e.g., "UniswapDayData"). Only
    /// populated for variables that appeared as top-level `where: $var` and were
    /// declared with a subgraph-style `*_filter` type in the query header.
    filter_variable_entities: HashMap<String, String>,
}

fn convert_query_structure(
    query: &str,
    chain_id: Option<&str>,
) -> Result<MainConversion, ConversionError> {
    // Check for _meta query first
    if query.contains("_meta") {
        return Ok(MainConversion {
            converted_query: convert_meta_query(query)?,
            field_name_map: HashMap::new(),
            filter_variable_entities: HashMap::new(),
        });
    }

    // Extract fragments and main query
    let (fragments, main_query) = extract_fragments_and_main_query(query)?;

    // Convert the main query
    let main = convert_main_query(&main_query, chain_id)?;

    // Combine fragments with converted main query
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
                                let fragment_text = sanitize_fragment_arguments(&fragment_text);
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
        while i < bytes.len() && bytes[i] != b',' && bytes[i] != b')' {
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
    // Strip non-null markers, list brackets, and whitespace.
    let cleaned: String = declared_type
        .chars()
        .filter(|c| !c.is_whitespace() && *c != '!' && *c != '[' && *c != ']')
        .collect();
    cleaned.ends_with("_filter") && cleaned.len() > "_filter".len()
}

fn convert_main_query(
    main_query: &str,
    chain_id: Option<&str>,
) -> Result<MainConversion, ConversionError> {
    // Extract query header (name and variable definitions) and body separately
    let (query_header, stripped_query) = if main_query.trim().starts_with("query") {
        let content = main_query.trim();
        if let (Some(start_brace), Some(end_brace)) = (content.find('{'), content.rfind('}')) {
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
        // Already a selection body, no header
        (
            None,
            main_query
                .trim_start_matches('{')
                .trim_end_matches('}')
                .to_string(),
        )
    } else {
        (None, main_query.to_string())
    };

    // Extract multiple entities from the main query body
    let entities = extract_multiple_entities(&stripped_query)?;

    let mut converted_entities = Vec::new();
    // Build mapping from Hyperindex field names to original query field names
    let mut field_name_map: HashMap<String, String> = HashMap::new();
    // Collect variable type overrides from all entities
    let mut all_variable_type_overrides: HashMap<String, String> = HashMap::new();
    // Track variables used as a top-level `where: $var` so their JSON value can
    // be translated from subgraph filter shape to Hasura bool_exp shape at request time.
    // Only variables whose declared type is a subgraph `*_filter` are recorded;
    // variables typed as anything else (custom input types, already-migrated
    // `*_bool_exp`, etc.) pass through verbatim.
    let mut filter_variable_entities: HashMap<String, String> = HashMap::new();
    // Parse variable declarations once for the whole query.
    let header_var_types: HashMap<String, String> = query_header
        .as_deref()
        .map(parse_variable_declarations)
        .unwrap_or_default();

    for (entity, params, selection) in entities {
        let entity_cap = singularize_and_capitalize(&entity);

        // Record the mapping: Hyperindex name -> original query name
        // e.g., "LpAction" -> "lpActions", "LpAction_by_pk" -> "lpActions"
        field_name_map.insert(entity_cap.clone(), entity.clone());
        field_name_map.insert(format!("{}_by_pk", entity_cap), entity.clone());
        // Extract limit/offset, preserving GraphQL variables (e.g., $first/$skip)
        let limit = params.get("first").cloned();
        let offset = params.get("skip").cloned();

        // Single-entity by primary key: singular entity, only 'id' param
        if !entity.ends_with('s') && params.len() == 1 && params.contains_key("id") {
            let pk_query = format!(
                "  {}_by_pk(id: {}) {}",
                entity_cap,
                params.get("id").unwrap(),
                selection
            );
            converted_entities.push(pk_query);
            continue;
        }

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
                let var_name = where_var
                    .trim()
                    .trim_start_matches('$')
                    .to_string();
                if !var_name.is_empty() {
                    let declared = header_var_types.get(&var_name);
                    if declared.map(|t| is_subgraph_filter_type(t)).unwrap_or(false) {
                        filter_variable_entities.insert(var_name, entity_cap.clone());
                    }
                }
                format!("where: {}", where_var)
            } else {
                String::new()
            };
            (clause, HashMap::new())
        } else {
            let result = convert_filters_to_where_clause(&converted_params, &entity_cap)?;
            (result.where_clause, result.variable_type_overrides)
        };

        // Merge variable type overrides
        all_variable_type_overrides.extend(entity_var_overrides);

        let mut params_vec = Vec::new();
        if let Some(l) = limit.as_ref() {
            // Include limit whether it's a literal or a variable (e.g., $first)
            params_vec.push(format!("limit: {}", l));
        }
        if let Some(o) = offset.as_ref() {
            // Include offset whether it's a literal or a variable (e.g., $skip)
            params_vec.push(format!("offset: {}", o));
        }
        // Map orderBy/orderDirection to Hasura order_by
        if let Some(order_field) = params.get("orderBy") {
            let order_dir = params
                .get("orderDirection")
                .map(|s| s.as_str())
                .unwrap_or("asc");
            // Ignore order_by if the order field is a variable (e.g., $orderBy) to keep query valid
            if !order_field.trim_start().starts_with('$')
                && !order_dir.trim_start().starts_with('$')
            {
                params_vec.push(format!("order_by: {{{}: {}}}", order_field, order_dir));
            }
        }
        if !where_clause.is_empty() {
            // The where_clause already has the correct format, just use it directly
            params_vec.push(where_clause);
        }
        let params_str = if params_vec.is_empty() {
            String::new()
        } else {
            format!("({})", params_vec.join(", "))
        };

        let converted_entity = format!("  {}{} {}", entity_cap, params_str, selection);
        converted_entities.push(converted_entity);
    }

    // Reconstruct the query with preserved header (including variable definitions)
    // Convert ID and Bytes types to String in variable definitions
    let query_header_str = if let Some(header) = &query_header {
        // Convert variable type definitions: ID -> String, Bytes -> String, and enum types
        let converted_header =
            convert_variable_types_in_header(header, &all_variable_type_overrides);
        // Additionally rewrite subgraph filter type names (e.g., `Foo_filter`)
        // to their Hyperindex/Hasura equivalents (`Foo_bool_exp`). This is a
        // syntactic rename only — see `translate_subgraph_filter_value` for
        // the runtime JSON-value translation that goes with it.
        let converted_header = rewrite_filter_type_names(&converted_header);
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

    let converted_query = format!(
        "{} {{\n{}\n}}",
        query_header_str,
        converted_entities.join("\n")
    );
    tracing::debug!("Final converted query: {}", converted_query);
    Ok(MainConversion {
        converted_query,
        field_name_map,
        filter_variable_entities,
    })
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

fn extract_multiple_entities(
    query: &str,
) -> Result<Vec<(String, HashMap<String, String>, String)>, ConversionError> {
    let mut entities = Vec::new();
    let query_chars: Vec<char> = query.chars().collect();
    let mut current_pos = 0;

    println!("DEBUG: Parsing query: {}", query);

    // Skip opening brace if present
    while current_pos < query_chars.len() && query_chars[current_pos].is_whitespace() {
        current_pos += 1;
    }
    if current_pos < query_chars.len() && query_chars[current_pos] == '{' {
        println!("DEBUG: Found opening brace at position {}", current_pos);
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

        println!(
            "DEBUG: Looking for entity at position {}, char: '{}'",
            current_pos, query_chars[current_pos]
        );

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

        let entity_name = query_chars[entity_start..current_pos]
            .iter()
            .collect::<String>();
        println!("DEBUG: Found potential entity name: '{}'", entity_name);

        // Skip if this is not a valid entity name (too short or common words)
        if entity_name.len() < 2
            || [
                "id", "in", "on", "to", "of", "at", "by", "is", "it", "as", "or", "an", "if", "up",
                "do", "go", "no", "so", "we", "he", "me", "be", "my", "am", "us", "hi", "lo", "ok",
                "hi", "lo", "ok",
            ]
            .contains(&entity_name.as_str())
        {
            println!(
                "DEBUG: Skipping '{}' as it's not a valid entity name",
                entity_name
            );
            current_pos += 1;
            continue;
        }

        // Look for opening parenthesis or brace after entity name (with optional whitespace)
        while current_pos < query_chars.len() && query_chars[current_pos].is_whitespace() {
            current_pos += 1;
        }

        let mut params = HashMap::new();

        if current_pos < query_chars.len() && query_chars[current_pos] == '(' {
            println!("DEBUG: Found entity definition for '{}'", entity_name);

            // Found an entity definition with parameters, extract parameters
            let params_start = current_pos + 1;
            let mut paren_count = 1; // We're already inside the first parenthesis

            while current_pos < query_chars.len() {
                current_pos += 1;
                if current_pos >= query_chars.len() {
                    break;
                }

                match query_chars[current_pos] {
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

            let params_str = query_chars[params_start..current_pos]
                .iter()
                .collect::<String>();
            parse_graphql_params(&params_str, &mut params)?;

            // Advance past the closing parenthesis
            current_pos += 1;
        } else if current_pos < query_chars.len() && query_chars[current_pos] == '{' {
            println!(
                "DEBUG: Found entity definition for '{}' (no parameters)",
                entity_name
            );
            // Entity without parameters, continue to selection set
        } else {
            println!(
                "DEBUG: No opening parenthesis or brace after '{}', skipping",
                entity_name
            );
            // This is not an entity definition, skip
            current_pos += 1;
            continue;
        }

        // Look for opening brace for selection set
        while current_pos < query_chars.len() && query_chars[current_pos].is_whitespace() {
            current_pos += 1;
        }

        println!(
            "DEBUG: After params, at position {}, char: '{}'",
            current_pos,
            if current_pos < query_chars.len() {
                query_chars[current_pos]
            } else {
                '?'
            }
        );

        if current_pos >= query_chars.len() || query_chars[current_pos] != '{' {
            println!(
                "DEBUG: No opening brace for selection set after '{}', skipping",
                entity_name
            );
            // No selection set, skip this entity
            current_pos += 1;
            continue;
        }

        println!(
            "DEBUG: Found opening brace for selection set at position {}",
            current_pos
        );

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
        let sanitized = sanitize_selection_set(&raw_selection);
        let selection_set = format!("{{\n    {}\n  }}", sanitized);

        println!("DEBUG: Found entity: {}", entity_name);
        println!("DEBUG: Params for {}: {:?}", entity_name, params);
        println!("DEBUG: Selection for {}: {}", entity_name, selection_set);

        entities.push((entity_name, params, selection_set));
    }

    println!(
        "DEBUG: Found {} entities: {:?}",
        entities.len(),
        entities.iter().map(|(name, _, _)| name).collect::<Vec<_>>()
    );
    Ok(entities)
}

fn sanitize_selection_set(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();
    let mut in_string = false;

    while let Some(ch) = chars.next() {
        if ch == '"' {
            in_string = !in_string;
            output.push(ch);
            continue;
        }

        if !in_string && ch == '(' {
            // Capture balanced parentheses and their contents so we can
            // translate subgraph-style nested-field args (first/skip/orderBy/
            // orderDirection) into Hasura form. Unknown args are dropped,
            // matching prior behavior.
            let mut content = String::new();
            let mut depth: i32 = 1;
            let mut in_args_string = false;
            while let Some(nc) = chars.next() {
                if nc == '"' {
                    in_args_string = !in_args_string;
                    content.push(nc);
                    continue;
                }
                if !in_args_string {
                    if nc == '(' {
                        depth += 1;
                    } else if nc == ')' {
                        depth -= 1;
                        if depth == 0 {
                            break;
                        }
                    }
                }
                content.push(nc);
            }
            let translated = translate_nested_field_args(&content);
            if !translated.is_empty() {
                output.push('(');
                output.push_str(&translated);
                output.push(')');
            }
            continue;
        }

        output.push(ch);
    }

    output
}

/// Translate subgraph-style args on a nested selection field into Hasura form.
///
/// Handles `first` → `limit`, `skip` → `offset`, and the
/// `orderBy`/`orderDirection` pair → `order_by: {field: dir}`. Variables are
/// preserved for `first`/`skip`; order_by is skipped when either side is a
/// variable, mirroring the top-level converter (`convert_main_query`). Any
/// other args (notably `where`) are dropped — they need parent-entity context
/// to translate correctly and translating them belongs in a follow-up.
fn translate_nested_field_args(args: &str) -> String {
    let mut params: HashMap<String, String> = HashMap::new();
    if parse_graphql_params(args, &mut params).is_err() {
        return String::new();
    }

    let mut out: Vec<String> = Vec::new();
    if let Some(first) = params.get("first") {
        out.push(format!("limit: {}", first));
    }
    if let Some(skip) = params.get("skip") {
        out.push(format!("offset: {}", skip));
    }
    if let Some(order_field) = params.get("orderBy") {
        let order_dir = params
            .get("orderDirection")
            .map(|s| s.as_str())
            .unwrap_or("asc");
        if !order_field.trim_start().starts_with('$')
            && !order_dir.trim_start().starts_with('$')
        {
            out.push(format!("order_by: {{{}: {}}}", order_field, order_dir));
        }
    }

    out.join(", ")
}

fn sanitize_fragment_arguments(fragment_text: &str) -> String {
    // Only sanitize the selection body after the fragment header
    // Find the first '{' and its matching '}' and strip args in between
    let chars: Vec<char> = fragment_text.chars().collect();
    let Some(open_idx) = chars.iter().position(|c| *c == '{') else {
        return fragment_text.to_string();
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
        return fragment_text.to_string();
    }
    let header: String = chars[..open_idx + 1].iter().collect();
    let body: String = chars[open_idx + 1..pos].iter().collect();
    let tail: String = chars[pos..].iter().collect();
    let sanitized_body = sanitize_selection_set(body.trim());
    format!("{}{}{}", header, sanitized_body, tail)
}

// Removed unused selection set helpers

fn convert_meta_query(query: &str) -> Result<String, ConversionError> {
    // Normalize whitespace: collapse all whitespace sequences into single spaces
    // This handles multiline queries like "_meta {\n    block {\n      number\n    }\n  }"
    let normalized: String = query.split_whitespace().collect::<Vec<_>>().join(" ");

    // Check if it's a simple _meta { block { number } } query
    let simple_meta_pattern = "_meta { block { number } }";
    let complex_meta_patterns = [
        "block { hash",
        "block { parentHash",
        "block { timestamp",
        "deployment",
        "hasIndexingErrors",
    ];

    // Check for complex patterns (using normalized query)
    for pattern in &complex_meta_patterns {
        if normalized.contains(pattern) {
            return Err(ConversionError::ComplexMetaQuery);
        }
    }

    // Check if it's the simple pattern (using normalized query)
    if normalized.contains(simple_meta_pattern) {
        return Ok(
            "query {\n  chain_metadata {\n    latest_fetched_block_number\n  }\n}".to_string(),
        );
    }

    // If it's a _meta query but not the simple pattern, it's complex
    if normalized.contains("_meta") {
        return Err(ConversionError::ComplexMetaQuery);
    }

    // This shouldn't happen, but just in case
    Err(ConversionError::InvalidQueryFormat)
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

    // Check if parent itself is a nested path (e.g., "pair.token")
    // If so, recursively process the first part with the rest as a nested filter
    if parent.contains('.') {
        if let Some(dot_idx) = parent.find('.') {
            let first_part = &parent[..dot_idx];
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

    // Group child filters by field name to handle duplicates
    let mut grouped_child_filters: HashMap<String, Vec<(String, String)>> = HashMap::new();
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

    // Group filters by parent object to avoid duplicates
    let mut grouped_filters: HashMap<String, HashMap<String, String>> = HashMap::new();
    let mut basic_filters: HashMap<String, Vec<(String, String)>> = HashMap::new();

    for (key, value) in flat_filters {
        if key.contains('.') {
            // This is a nested filter (e.g., "user.name_starts_with")
            if let Some(dot_idx) = key.rfind('.') {
                let parent = &key[..dot_idx];
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
    if !and_conditions.is_empty() {
        where_conditions.push(format!("_and: [{}]", and_conditions.join(", ")));
    }

    // Add grouped nested filters (recursively handle arbitrary depth)
    for (parent, child_filters) in grouped_filters {
        let nested_condition =
            process_nested_filters_recursive(&parent, child_filters, entity_name)?;
        where_conditions.push(nested_condition);
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

    // Handle different filter patterns - check longer suffixes first
    if key.ends_with("_not_starts_with_nocase") {
        let field = &key[..key.len() - 23];
        return Ok(format!(
            "_not: {{{}: {{_ilike: \"{}%\"}}}}",
            field,
            value.trim_matches('"')
        ));
    }

    if key.ends_with("_not_ends_with_nocase") {
        let field = &key[..key.len() - 21];
        return Ok(format!(
            "_not: {{{}: {{_ilike: \"%{}\"}}}}",
            field,
            value.trim_matches('"')
        ));
    }

    if key.ends_with("_not_contains_nocase") {
        let field = &key[..key.len() - 20];
        return Ok(format!(
            "_not: {{{}: {{_ilike: \"%{}%\"}}}}",
            field,
            value.trim_matches('"')
        ));
    }

    if key.ends_with("_starts_with_nocase") {
        let field = &key[..key.len() - 19];
        return Ok(format!(
            "{}: {{_ilike: \"{}%\"}}",
            field,
            value.trim_matches('"')
        ));
    }

    if key.ends_with("_ends_with_nocase") {
        let field = &key[..key.len() - 17];
        return Ok(format!(
            "{}: {{_ilike: \"%{}\"}}",
            field,
            value.trim_matches('"')
        ));
    }

    if key.ends_with("_contains_nocase") {
        let field = &key[..key.len() - 16];
        return Ok(format!(
            "{}: {{_ilike: \"%{}%\"}}",
            field,
            value.trim_matches('"')
        ));
    }

    if key.ends_with("_not_starts_with") {
        let field = &key[..key.len() - 16];
        return Ok(format!(
            "_not: {{{}: {{_ilike: \"{}%\"}}}}",
            field,
            value.trim_matches('"')
        ));
    }

    if key.ends_with("_not_ends_with") {
        let field = &key[..key.len() - 14];
        return Ok(format!(
            "_not: {{{}: {{_ilike: \"%{}\"}}}}",
            field,
            value.trim_matches('"')
        ));
    }

    if key.ends_with("_not_contains") {
        let field = &key[..key.len() - 13];
        return Ok(format!(
            "_not: {{{}: {{_ilike: \"%{}%\"}}}}",
            field,
            value.trim_matches('"')
        ));
    }

    if key.ends_with("_starts_with") {
        let field = &key[..key.len() - 12];
        return Ok(format!(
            "{}: {{_ilike: \"{}%\"}}",
            field,
            value.trim_matches('"')
        ));
    }

    if key.ends_with("_ends_with") {
        let field = &key[..key.len() - 10];
        return Ok(format!(
            "{}: {{_ilike: \"%{}\"}}",
            field,
            value.trim_matches('"')
        ));
    }

    if key.ends_with("_contains") {
        let field = &key[..key.len() - 9];
        return Ok(format!(
            "{}: {{_ilike: \"%{}%\"}}",
            field,
            value.trim_matches('"')
        ));
    }

    if key.ends_with("_not_in") {
        let field = &key[..key.len() - 7];
        return Ok(format!("{}: {{_nin: {}}}", field, value));
    }

    if key.ends_with("_gte") {
        let field = &key[..key.len() - 4];
        return Ok(format!("{}: {{_gte: {}}}", field, value));
    }

    if key.ends_with("_lte") {
        let field = &key[..key.len() - 4];
        return Ok(format!("{}: {{_lte: {}}}", field, value));
    }

    if key.ends_with("_not") {
        let field = &key[..key.len() - 4];
        return Ok(format!("{}: {{_neq: {}}}", field, value));
    }

    if key.ends_with("_gt") {
        let field = &key[..key.len() - 3];
        return Ok(format!("{}: {{_gt: {}}}", field, value));
    }

    if key.ends_with("_lt") {
        let field = &key[..key.len() - 3];
        return Ok(format!("{}: {{_lt: {}}}", field, value));
    }

    if key.ends_with("_in") {
        let field = &key[..key.len() - 3];
        return Ok(format!("{}: {{_in: {}}}", field, value));
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
    // `_not_*` operators in the inline path each emit their own `_not: { ... }`
    // wrapper. JSON object keys are unique, so we accumulate the inner fields
    // and emit a single combined `_not` at the end.
    let mut not_accum = serde_json::Map::new();

    for (key, val) in obj {
        match key.as_str() {
            "and" | "_and" => {
                let arr = translate_filter_array(val, entity_name);
                out.insert("_and".to_string(), Value::Array(arr));
            }
            "or" | "_or" => {
                let arr = translate_filter_array(val, entity_name);
                out.insert("_or".to_string(), Value::Array(arr));
            }
            "_not" | "not" => {
                let inner = translate_subgraph_filter_value(val, entity_name);
                if let Value::Object(m) = inner {
                    for (k, v) in m {
                        not_accum.insert(k, v);
                    }
                }
            }
            _ => translate_leaf_filter(key, val, entity_name, &mut out, &mut not_accum),
        }
    }

    if !not_accum.is_empty() {
        out.insert("_not".to_string(), Value::Object(not_accum));
    }

    Value::Object(out)
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

/// Translate a single non-connective filter entry, writing into `out`
/// (or `not_accum` for `_not_*` operators).
fn translate_leaf_filter(
    key: &str,
    value: &Value,
    entity_name: &str,
    out: &mut serde_json::Map<String, Value>,
    not_accum: &mut serde_json::Map<String, Value>,
) {
    // ORDER MATTERS — longest suffixes must be checked first, mirroring the
    // ordering in `convert_basic_filter_to_hasura_condition`. Changes here
    // must be applied to both functions.

    if let Some(field) = key.strip_suffix("_not_starts_with_nocase") {
        not_accum.insert(field.to_string(), ilike_value(value, IlikeShape::StartsWith));
        return;
    }
    if let Some(field) = key.strip_suffix("_not_ends_with_nocase") {
        not_accum.insert(field.to_string(), ilike_value(value, IlikeShape::EndsWith));
        return;
    }
    if let Some(field) = key.strip_suffix("_not_contains_nocase") {
        not_accum.insert(field.to_string(), ilike_value(value, IlikeShape::Contains));
        return;
    }
    if let Some(field) = key.strip_suffix("_starts_with_nocase") {
        out.insert(field.to_string(), ilike_value(value, IlikeShape::StartsWith));
        return;
    }
    if let Some(field) = key.strip_suffix("_ends_with_nocase") {
        out.insert(field.to_string(), ilike_value(value, IlikeShape::EndsWith));
        return;
    }
    if let Some(field) = key.strip_suffix("_contains_nocase") {
        out.insert(field.to_string(), ilike_value(value, IlikeShape::Contains));
        return;
    }
    if let Some(field) = key.strip_suffix("_not_starts_with") {
        not_accum.insert(field.to_string(), ilike_value(value, IlikeShape::StartsWith));
        return;
    }
    if let Some(field) = key.strip_suffix("_not_ends_with") {
        not_accum.insert(field.to_string(), ilike_value(value, IlikeShape::EndsWith));
        return;
    }
    if let Some(field) = key.strip_suffix("_not_contains") {
        not_accum.insert(field.to_string(), ilike_value(value, IlikeShape::Contains));
        return;
    }
    if let Some(field) = key.strip_suffix("_starts_with") {
        out.insert(field.to_string(), ilike_value(value, IlikeShape::StartsWith));
        return;
    }
    if let Some(field) = key.strip_suffix("_ends_with") {
        out.insert(field.to_string(), ilike_value(value, IlikeShape::EndsWith));
        return;
    }
    if let Some(field) = key.strip_suffix("_contains") {
        // NOTE: inline path maps `_contains` to `_ilike` (case-insensitive)
        // even though subgraph's `_contains` is case-sensitive on strings.
        // Mirrored here for behaviour parity.
        out.insert(field.to_string(), ilike_value(value, IlikeShape::Contains));
        return;
    }
    if let Some(field) = key.strip_suffix("_not_in") {
        out.insert(field.to_string(), op_wrap("_nin", value.clone()));
        return;
    }
    if let Some(field) = key.strip_suffix("_gte") {
        out.insert(field.to_string(), op_wrap("_gte", value.clone()));
        return;
    }
    if let Some(field) = key.strip_suffix("_lte") {
        out.insert(field.to_string(), op_wrap("_lte", value.clone()));
        return;
    }
    if let Some(field) = key.strip_suffix("_not") {
        out.insert(field.to_string(), op_wrap("_neq", value.clone()));
        return;
    }
    if let Some(field) = key.strip_suffix("_gt") {
        out.insert(field.to_string(), op_wrap("_gt", value.clone()));
        return;
    }
    if let Some(field) = key.strip_suffix("_lt") {
        out.insert(field.to_string(), op_wrap("_lt", value.clone()));
        return;
    }
    if let Some(field) = key.strip_suffix("_in") {
        out.insert(field.to_string(), op_wrap("_in", value.clone()));
        return;
    }

    // Unsupported in the inline path; pass through here so Hasura surfaces a
    // useful error instead of silently dropping the filter.
    if key.ends_with("_containsAny") || key.ends_with("_containsAll") {
        out.insert(key.to_string(), value.clone());
        return;
    }

    // No operator suffix — direct equality, connective (already handled), or
    // a nested-entity reference.

    if key == "chainId" {
        out.insert(key.to_string(), op_wrap("_eq", value.clone()));
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
            out.insert(target_key.to_string(), translated);
        } else {
            // Object value on a non-nested field — most commonly an already
            // Hasura-shaped sub-expression (e.g. `{_eq: 1}`). Pass through.
            out.insert(target_key.to_string(), value.clone());
        }
        return;
    }

    if is_nested {
        // Scalar against a nested entity → ID equality.
        let mut id_wrapper = serde_json::Map::new();
        id_wrapper.insert("id".to_string(), op_wrap("_eq", value.clone()));
        out.insert(target_key.to_string(), Value::Object(id_wrapper));
        return;
    }

    out.insert(target_key.to_string(), op_wrap("_eq", value.clone()));
}

fn op_wrap(op: &str, value: Value) -> Value {
    let mut m = serde_json::Map::new();
    m.insert(op.to_string(), value);
    Value::Object(m)
}

#[derive(Clone, Copy)]
enum IlikeShape {
    Contains,
    StartsWith,
    EndsWith,
}

fn ilike_value(value: &Value, shape: IlikeShape) -> Value {
    let s = match value {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        // Object/array/null aren't valid ILIKE patterns; forward the raw value
        // so the upstream returns a meaningful type error.
        _ => return op_wrap("_ilike", value.clone()),
    };
    let pattern = match shape {
        IlikeShape::Contains => format!("%{}%", s),
        IlikeShape::StartsWith => format!("{}%", s),
        IlikeShape::EndsWith => format!("%{}", s),
    };
    op_wrap("_ilike", Value::String(pattern))
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

            // Add nested params directly without flattening
            for (nested_key, nested_value) in nested_params {
                params.insert(nested_key, nested_value);
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

fn singularize_and_capitalize(s: &str) -> String {
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
        static INIT_TEST_SCHEMA: std::sync::Once = std::sync::Once::new();
        INIT_TEST_SCHEMA.call_once(|| {
            schema::init_test_schema();
        });
    }

    #[test]
    fn test_basic_collection_query() {
        let payload = create_test_payload("query { streams(first: 10, skip: 0) { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(limit: 10, offset: 0, where: {chainId: {_eq: \"1\"}}) {\n    id name\n  }\n}"
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
            "query": "query {\n  V2_Factory_Swap(limit: 1, order_by: {timestamp: desc}, where: {chainId: {_eq: \"1\"}}) {\n    id timestamp amountUSD\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_single_entity_query() {
        let payload = create_test_payload("query { stream(id: \"123\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream_by_pk(id: \"123\") {\n    id name\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_meta_query_simple() {
        let payload = create_test_payload("query { _meta { block { number } } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  chain_metadata {\n    latest_fetched_block_number\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_meta_query_simple_multiline() {
        // Test the multiline version that was failing before
        let payload =
            create_test_payload("query {\n  _meta {\n    block {\n      number\n    }\n  }\n}");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  chain_metadata {\n    latest_fetched_block_number\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_meta_query_with_tabs_and_mixed_whitespace() {
        // Test with tabs and mixed whitespace
        let payload =
            create_test_payload("query {\n\t_meta {\n\t\tblock {\n\t\t\tnumber\n\t\t}\n\t}\n}");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  chain_metadata {\n    latest_fetched_block_number\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_meta_query_with_extra_spaces() {
        // Test with extra spaces between tokens
        let payload = create_test_payload("query  {   _meta   {   block   {   number   }   }   }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  chain_metadata {\n    latest_fetched_block_number\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_meta_query_complex() {
        let payload = create_test_payload("query { _meta { block { hash number } } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1"));
        assert!(result.is_err());
        match result {
            Err(ConversionError::ComplexMetaQuery) => {}
            _ => panic!("Expected ComplexMetaQuery error"),
        }
    }

    #[test]
    fn test_meta_query_complex_multiline() {
        // Complex meta query with multiline format should also fail
        let payload = create_test_payload(
            "query {\n  _meta {\n    block {\n      hash\n      number\n    }\n  }\n}",
        );
        let result = convert_subgraph_to_hyperindex(&payload, Some("1"));
        assert!(result.is_err());
        match result {
            Err(ConversionError::ComplexMetaQuery) => {}
            _ => panic!("Expected ComplexMetaQuery error"),
        }
    }

    #[test]
    fn test_meta_query_with_deployment() {
        // deployment field should trigger ComplexMetaQuery error
        let payload = create_test_payload("query { _meta { deployment } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1"));
        assert!(result.is_err());
        match result {
            Err(ConversionError::ComplexMetaQuery) => {}
            _ => panic!("Expected ComplexMetaQuery error"),
        }
    }

    #[test]
    fn test_meta_query_with_has_indexing_errors() {
        // hasIndexingErrors field should trigger ComplexMetaQuery error
        let payload = create_test_payload("query { _meta { hasIndexingErrors } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1"));
        assert!(result.is_err());
        match result {
            Err(ConversionError::ComplexMetaQuery) => {}
            _ => panic!("Expected ComplexMetaQuery error"),
        }
    }

    #[test]
    fn test_meta_query_with_timestamp_multiline() {
        // timestamp in multiline format should trigger ComplexMetaQuery error
        let payload =
            create_test_payload("query {\n  _meta {\n    block {\n      timestamp\n    }\n  }\n}");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1"));
        assert!(result.is_err());
        match result {
            Err(ConversionError::ComplexMetaQuery) => {}
            _ => panic!("Expected ComplexMetaQuery error"),
        }
    }

    // Filter tests
    #[test]
    fn test_equality_filter() {
        let payload = create_test_payload("query { streams(name: \"test\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(where: {chainId: {_eq: \"1\"}, name: {_eq: \"test\"}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_not_filter() {
        let payload = create_test_payload("query { streams(name_not: \"test\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(where: {chainId: {_eq: \"1\"}, name: {_neq: \"test\"}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_greater_than_filter() {
        let payload = create_test_payload("query { streams(amount_gt: 100) { id amount } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(where: {chainId: {_eq: \"1\"}, amount: {_gt: 100}}) {\n    id amount\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_greater_than_or_equal_filter() {
        let payload = create_test_payload("query { streams(amount_gte: 100) { id amount } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(where: {chainId: {_eq: \"1\"}, amount: {_gte: 100}}) {\n    id amount\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_less_than_filter() {
        let payload = create_test_payload("query { streams(amount_lt: 100) { id amount } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(where: {chainId: {_eq: \"1\"}, amount: {_lt: 100}}) {\n    id amount\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_less_than_or_equal_filter() {
        let payload = create_test_payload("query { streams(amount_lte: 100) { id amount } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(where: {chainId: {_eq: \"1\"}, amount: {_lte: 100}}) {\n    id amount\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_in_filter() {
        let payload =
            create_test_payload("query { streams(id_in: [\"1\", \"2\", \"3\"]) { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(where: {chainId: {_eq: \"1\"}, id: {_in: [\"1\", \"2\", \"3\"]}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_not_in_filter() {
        let payload =
            create_test_payload("query { streams(id_not_in: [\"1\", \"2\", \"3\"]) { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(where: {chainId: {_eq: \"1\"}, id: {_nin: [\"1\", \"2\", \"3\"]}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_contains_filter() {
        let payload = create_test_payload("query { streams(name_contains: \"test\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(where: {chainId: {_eq: \"1\"}, name: {_ilike: \"%test%\"}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_not_contains_filter() {
        let payload =
            create_test_payload("query { streams(name_not_contains: \"test\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(where: {chainId: {_eq: \"1\"}, _not: {name: {_ilike: \"%test%\"}}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_starts_with_filter() {
        let payload =
            create_test_payload("query { streams(name_starts_with: \"test\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(where: {chainId: {_eq: \"1\"}, name: {_ilike: \"test%\"}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_ends_with_filter() {
        let payload =
            create_test_payload("query { streams(name_ends_with: \"test\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(where: {chainId: {_eq: \"1\"}, name: {_ilike: \"%test\"}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_not_starts_with_filter() {
        let payload =
            create_test_payload("query { streams(name_not_starts_with: \"test\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(where: {chainId: {_eq: \"1\"}, _not: {name: {_ilike: \"test%\"}}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_not_ends_with_filter() {
        let payload =
            create_test_payload("query { streams(name_not_ends_with: \"test\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(where: {chainId: {_eq: \"1\"}, _not: {name: {_ilike: \"%test\"}}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_contains_nocase_filter() {
        let payload =
            create_test_payload("query { streams(name_contains_nocase: \"test\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(where: {chainId: {_eq: \"1\"}, name: {_ilike: \"%test%\"}}) {\n    id name\n  }\n}"
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
            "query": "query {\n  Stream(where: {chainId: {_eq: \"1\"}, _not: {name: {_ilike: \"%test%\"}}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_starts_with_nocase_filter() {
        let payload =
            create_test_payload("query { streams(name_starts_with_nocase: \"test\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(where: {chainId: {_eq: \"1\"}, name: {_ilike: \"test%\"}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_ends_with_nocase_filter() {
        let payload =
            create_test_payload("query { streams(name_ends_with_nocase: \"test\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(where: {chainId: {_eq: \"1\"}, name: {_ilike: \"%test\"}}) {\n    id name\n  }\n}"
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
            "query": "query {\n  Stream(where: {chainId: {_eq: \"1\"}, _not: {name: {_ilike: \"test%\"}}}) {\n    id name\n  }\n}"
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
            "query": "query {\n  Stream(where: {chainId: {_eq: \"1\"}, _not: {name: {_ilike: \"%test\"}}}) {\n    id name\n  }\n}"
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
        assert!(query.contains("name: {_ilike: \"%test%\"}"));
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
            "query": "query {\n  User(where: {chainId: {_eq: \"1\"}, name: {_ilike: \"%john%\"}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_pagination_parameters() {
        let payload = create_test_payload("query { streams(first: 5, skip: 10) { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(limit: 5, offset: 10, where: {chainId: {_eq: \"1\"}}) {\n    id name\n  }\n}"
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
            "query": "query {\n  Stream(order_by: {name: desc}, where: {chainId: {_eq: \"1\"}}) {\n    id name\n  }\n}"
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
            "query": "query {\n  Stream(offset: 10, order_by: {alias: asc}, where: {chainId: {_eq: \"1\"}, alias: {_ilike: \"%113%\"}}) {\n    alias asset { address }\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_complex_selection_set() {
        let payload =
            create_test_payload("query { streams { id name amount status { id name } } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(where: {chainId: {_eq: \"1\"}}) {\n    id name amount status { id name }\n  }\n}"
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
            Err(ConversionError::InvalidQueryFormat) => {}
            _ => panic!("Expected InvalidQueryFormat error"),
        }
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
            "query": "query {\n  Stream(limit: 10, offset: 0) {\n    id name\n  }\n}"
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
            "query": "query {\n  Stream(where: {name: {_eq: \"test\"}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_non_stream_entity_no_chain_id() {
        let payload = create_test_payload("query { users(name_contains: \"john\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let expected = json!({
            "query": "query {\n  User(where: {name: {_ilike: \"%john%\"}}) {\n    id name\n  }\n}"
        });
        assert_eq!(result.query, expected);
    }

    #[test]
    fn test_different_chain_id() {
        let payload = create_test_payload("query { streams(name: \"test\") { id name } }");
        let result = convert_subgraph_to_hyperindex(&payload, Some("5")).unwrap();
        let expected = json!({
            "query": "query {\n  Stream(where: {chainId: {_eq: \"5\"}, name: {_eq: \"test\"}}) {\n    id name\n  }\n}"
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
        assert!(query.contains("alias: {_ilike: \"%113%\"}"));
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
        assert!(query.contains("alias: {_ilike: \"%113%\"}"));
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
        let result = convert_subgraph_to_hyperindex(&payload, Some("1")).unwrap();
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
        assert_eq!(rewrite_filter_type_names(h), "query Q($where: Trade_bool_exp)");
    }

    #[test]
    fn rewrite_filter_type_names_non_null() {
        let h = "query Q($where: Trade_filter!)";
        assert_eq!(rewrite_filter_type_names(h), "query Q($where: Trade_bool_exp!)");
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
        let expected = json!({
            "amount": { "_gt": 1 },
        });
        // Note: in JSON, multiple operators on the same field collapse to a
        // single key (last write wins) — same as Hasura object semantics. We
        // assert membership of each key rather than full equality because
        // HashMap iteration order is not deterministic.
        let out_obj = out.as_object().unwrap();
        assert_eq!(out_obj["amount"].as_object().unwrap().len(), 1);
        assert!(["_gt", "_gte", "_lt", "_lte"].contains(
            &out_obj["amount"].as_object().unwrap().keys().next().unwrap().as_str()
        ));
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
        let _ = expected; // silence warning
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
        let cases = [
            ("name_contains", "%X%"),
            ("name_starts_with", "X%"),
            ("name_ends_with", "%X"),
            ("name_contains_nocase", "%X%"),
            ("name_starts_with_nocase", "X%"),
            ("name_ends_with_nocase", "%X"),
        ];
        for (suffix, pattern) in cases {
            let v = json!({ suffix: "X" });
            let out = translate_subgraph_filter_value(&v, "Pair");
            assert_eq!(out, json!({ "name": { "_ilike": pattern } }), "suffix {}", suffix);
        }
    }

    #[test]
    fn translate_negated_string_ops_go_into_not() {
        init_test_schema_if_needed();
        let cases = [
            ("name_not_contains", "%X%"),
            ("name_not_starts_with", "X%"),
            ("name_not_ends_with", "%X"),
            ("name_not_contains_nocase", "%X%"),
            ("name_not_starts_with_nocase", "X%"),
            ("name_not_ends_with_nocase", "%X"),
        ];
        for (suffix, pattern) in cases {
            let v = json!({ suffix: "X" });
            let out = translate_subgraph_filter_value(&v, "Pair");
            assert_eq!(
                out,
                json!({ "_not": { "name": { "_ilike": pattern } } }),
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
        assert_eq!(translate_subgraph_filter_value(&Value::Null, "Trade"), Value::Null);
        assert_eq!(translate_subgraph_filter_value(&json!(42), "Trade"), json!(42));
        assert_eq!(translate_subgraph_filter_value(&json!("x"), "Trade"), json!("x"));
        assert_eq!(translate_subgraph_filter_value(&json!([1, 2]), "Trade"), json!([1, 2]));
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
        assert!(!query.contains("Trade_filter"), "Should not contain Trade_filter, got: {}", query);

        // Where clause forwards the variable verbatim.
        assert!(query.contains("where: $where"), "Expected where: $where, got: {}", query);

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
        // is a variable, the converter currently forwards the variable as-is
        // — chain_id isn't merged in. This test pins that behaviour so a
        // future change doesn't silently start double-applying chain_id.
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
            &json!({ "amount": { "_gt": 1 } })
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
            &json!({ "name": { "_ilike": "%ETH%" } })
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
    fn test_nested_field_first_and_order_by_translated() {
        // Regression: nested-field args (first/orderBy/orderDirection) on
        // sub-selections like `poolHourData(...)` and `poolDayData(...)` were
        // silently stripped by sanitize_selection_set, so the user got
        // unbounded/unordered nested rows. They should now be translated to
        // Hasura's limit/order_by form.
        init_test_schema_if_needed();
        let payload = create_test_payload(
            "{ pools(first: 1, where: { id: \"0x2a2c512beaa8eb15495726c235472d82effb7a6b\" }) { id poolHourData(first: 3, orderBy: periodStartUnix, orderDirection: desc) { periodStartUnix volumeUSD } poolDayData(first: 3, orderBy: date, orderDirection: desc) { date volumeUSD } } }",
        );
        let result = convert_subgraph_to_hyperindex(&payload, None).unwrap();
        let query = result.query["query"].as_str().unwrap();
        assert!(
            query.contains("poolHourData(limit: 3, order_by: {periodStartUnix: desc})"),
            "poolHourData nested args not translated:\n{}",
            query
        );
        assert!(
            query.contains("poolDayData(limit: 3, order_by: {date: desc})"),
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
            query.contains("poolHourData(limit: 5, offset: 10)"),
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
            query.contains("poolHourData(order_by: {periodStartUnix: asc})"),
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
}
