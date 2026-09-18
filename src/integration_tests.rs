// Integration tests for end-to-end subgraph→HyperIndex query conversion.
//
// These tests run the same realistic queries that used to be forwarded to a
// live HyperIndex endpoint, but assert directly on the converter's output
// string instead. That keeps the suite hermetic — it always runs, requires
// no network, and doesn't depend on any specific entity being present in any
// particular indexer.

use serde_json::{json, Value};

use crate::conversion;

fn assert_converts_to(subgraph_query: &str, expected_hyperindex_query: &str) {
    let payload = json!({ "query": subgraph_query });
    let result = conversion::convert_subgraph_to_hyperindex(&payload, Some("1"))
        .expect("conversion should succeed");
    let expected: Value = json!({ "query": expected_hyperindex_query });
    assert_eq!(result.query, expected);
}

// Some converter outputs include `_and: [arm_a, arm_b]` arrays whose arm order
// is non-deterministic (the upstream where-clause builder iterates over a
// HashMap). Equality against a single golden string would flake. This helper
// passes the test if the actual output matches *any* of the supplied valid
// orderings.
fn assert_converts_to_any(subgraph_query: &str, acceptable: &[&str]) {
    let payload = json!({ "query": subgraph_query });
    let result = conversion::convert_subgraph_to_hyperindex(&payload, Some("1"))
        .expect("conversion should succeed");
    let actual = result.query["query"]
        .as_str()
        .expect("converted query should be a string");
    let matched = acceptable.iter().any(|candidate| *candidate == actual);
    assert!(
        matched,
        "Conversion output did not match any acceptable variant.\n\nActual:\n{}\n\nAcceptable:\n{}",
        actual,
        acceptable
            .iter()
            .enumerate()
            .map(|(i, s)| format!("[variant {}]\n{}", i, s))
            .collect::<Vec<_>>()
            .join("\n\n")
    );
}

#[test]
fn test_actions_and_assets_query() {
    let subgraph = r#"{
  actions(first: 5) {
    id
    block
    category
    chainId
  }
  assets(first: 5) {
    id
    address
    chainId
    decimals
  }
}"#;
    let expected = "query {\n  Action(limit: 5, where: {chainId: {_eq: \"1\"}}) {\n    id\n    block\n    category\n    chainId\n  }\n  Asset(limit: 5, where: {chainId: {_eq: \"1\"}}) {\n    id\n    address\n    chainId\n    decimals\n  }\n}";
    assert_converts_to(subgraph, expected);
}

#[test]
fn test_streams_with_order_by_query() {
    let subgraph = r#"{
  streams(orderBy: id, skip: 10) {
    alias
    asset {
      address
    }
  }
}"#;
    let expected = "query {\n  Stream(offset: 10, order_by: {id: asc}, where: {chainId: {_eq: \"1\"}}) {\n    alias\n    asset {\n      address\n    }\n  }\n}";
    assert_converts_to(subgraph, expected);
}

#[test]
fn test_streams_with_filter_query() {
    let subgraph = r#"{
  streams(orderBy: id, skip: 10, where: {alias_contains: "113"}) {
    alias
    asset {
      address
    }
  }
}"#;
    let expected = "query {\n  Stream(offset: 10, order_by: {id: asc}, where: {chainId: {_eq: \"1\"}, alias: {_ilike: \"%113%\"}}) {\n    alias\n    asset {\n      address\n    }\n  }\n}";
    assert_converts_to(subgraph, expected);
}

#[test]
fn test_streams_with_order_by_and_skip_query() {
    // Same shape as `test_streams_with_order_by_query` — duplicate kept
    // from the original suite for parity.
    let subgraph = r#"{
  streams(orderBy: id, skip: 10) {
    alias
    asset {
      address
    }
  }
}"#;
    let expected = "query {\n  Stream(offset: 10, order_by: {id: asc}, where: {chainId: {_eq: \"1\"}}) {\n    alias\n    asset {\n      address\n    }\n  }\n}";
    assert_converts_to(subgraph, expected);
}

#[test]
fn test_streams_with_order_by_skip_and_filter_query() {
    let subgraph = r#"{
  streams(orderBy: id, skip: 10, where: {alias_contains: "113"}) {
    alias
    asset {
      address
    }
  }
}"#;
    let expected = "query {\n  Stream(offset: 10, order_by: {id: asc}, where: {chainId: {_eq: \"1\"}, alias: {_ilike: \"%113%\"}}) {\n    alias\n    asset {\n      address\n    }\n  }\n}";
    assert_converts_to(subgraph, expected);
}

#[test]
fn test_complex_nested_query_with_multiple_filters() {
    let subgraph = r#"{
  streams(
    first: 10,
    skip: 5,
    where: {
      alias_contains: "test",
      asset: { address_starts_with: "0x" }
    }
  ) {
    id
    alias
    asset {
      address
      decimals
      symbol
    }
  }
}"#;
    let expected = "query {\n  Stream(limit: 10, offset: 5, where: {chainId: {_eq: \"1\"}, alias: {_ilike: \"%test%\"}, asset: {address: {_ilike: \"0x%\"}}}) {\n    id\n    alias\n    asset {\n      address\n      decimals\n      symbol\n    }\n  }\n}";
    assert_converts_to(subgraph, expected);
}

#[test]
fn test_multiple_entities_single_query() {
    let subgraph = r#"{
  streams(first: 5, where: { alias_contains: "test" }) {
    id
    alias
  }
  actions(first: 3) {
    id
    category
  }
  assets(first: 2) {
    id
    address
    symbol
  }
}"#;
    let expected = "query {\n  Stream(limit: 5, where: {chainId: {_eq: \"1\"}, alias: {_ilike: \"%test%\"}}) {\n    id\n    alias\n  }\n  Action(limit: 3, where: {chainId: {_eq: \"1\"}}) {\n    id\n    category\n  }\n  Asset(limit: 2, where: {chainId: {_eq: \"1\"}}) {\n    id\n    address\n    symbol\n  }\n}";
    assert_converts_to(subgraph, expected);
}

#[test]
fn test_advanced_filter_combinations() {
    let subgraph = r#"{
  streams(
    first: 20,
    where: {
      alias_contains: "test",
      alias_not_contains: "invalid"
    }
  ) {
    id
    alias
  }
}"#;
    let variant_a = "query {\n  Stream(limit: 20, where: {chainId: {_eq: \"1\"}, _and: [{alias: {_ilike: \"%test%\"}}, {_not: {alias: {_ilike: \"%invalid%\"}}}]}) {\n    id\n    alias\n  }\n}";
    let variant_b = "query {\n  Stream(limit: 20, where: {chainId: {_eq: \"1\"}, _and: [{_not: {alias: {_ilike: \"%invalid%\"}}}, {alias: {_ilike: \"%test%\"}}]}) {\n    id\n    alias\n  }\n}";
    assert_converts_to_any(subgraph, &[variant_a, variant_b]);
}

#[test]
fn test_pagination_and_ordering_edge_cases() {
    let subgraph = r#"{
  streams(
    first: 1,
    skip: 999,
    orderBy: id,
    orderDirection: desc,
    where: { alias_contains: "test" }
  ) {
    id
    alias
  }
}"#;
    let expected = "query {\n  Stream(limit: 1, offset: 999, order_by: {id: desc}, where: {chainId: {_eq: \"1\"}, alias: {_ilike: \"%test%\"}}) {\n    id\n    alias\n  }\n}";
    assert_converts_to(subgraph, expected);
}

#[test]
fn test_string_vs_numeric_filter_values() {
    let subgraph = r#"{
  streams(
    where: {
      alias_contains: "ll",
      asset: { decimals_gte: 6, decimals_lte: 18 }
    }
  ) {
    id
    alias
    asset { decimals }
  }
}"#;
    let variant_a = "query {\n  Stream(where: {chainId: {_eq: \"1\"}, alias: {_ilike: \"%ll%\"}, asset: {_and: [{decimals: {_lte: 18}}, {decimals: {_gte: 6}}]}}) {\n    id\n    alias\n    asset { decimals }\n  }\n}";
    let variant_b = "query {\n  Stream(where: {chainId: {_eq: \"1\"}, alias: {_ilike: \"%ll%\"}, asset: {_and: [{decimals: {_gte: 6}}, {decimals: {_lte: 18}}]}}) {\n    id\n    alias\n    asset { decimals }\n  }\n}";
    assert_converts_to_any(subgraph, &[variant_a, variant_b]);
}

#[test]
fn test_case_sensitive_vs_insensitive_filters() {
    let subgraph = r#"{
  streams(
    where: {
      alias_contains: "TEST",
      alias_contains_nocase: "test"
    }
  ) {
    id
    alias
  }
}"#;
    let variant_a = "query {\n  Stream(where: {chainId: {_eq: \"1\"}, _and: [{alias: {_ilike: \"%test%\"}}, {alias: {_ilike: \"%TEST%\"}}]}) {\n    id\n    alias\n  }\n}";
    let variant_b = "query {\n  Stream(where: {chainId: {_eq: \"1\"}, _and: [{alias: {_ilike: \"%TEST%\"}}, {alias: {_ilike: \"%test%\"}}]}) {\n    id\n    alias\n  }\n}";
    assert_converts_to_any(subgraph, &[variant_a, variant_b]);
}

#[test]
fn test_response_format_comparison() {
    // Originally compared TheGraph vs HyperIndex responses end-to-end. Now
    // only asserts the converter's output, which is the part this suite owns.
    let subgraph = r#"{
  streams(first: 10, orderBy: id, skip: 10, where: {alias_contains: "113"}) {
    alias
    asset {
      address
    }
  }
}"#;
    let expected = "query {\n  Stream(limit: 10, offset: 10, order_by: {id: asc}, where: {chainId: {_eq: \"1\"}, alias: {_ilike: \"%113%\"}}) {\n    alias\n    asset {\n      address\n    }\n  }\n}";
    assert_converts_to(subgraph, expected);
}

#[test]
fn test_fragments_conversion() {
    let subgraph = r#"fragment ActionFields on Action {
  id
  block
  category
  chainId
}

fragment AssetFields on Asset {
  id
  address
  chainId
  decimals
}

query {
  actions(first: 5) {
    ...ActionFields
  }
  assets(first: 5) {
    ...AssetFields
  }
}"#;
    let expected = "fragment ActionFields on Action {id\n  block\n  category\n  chainId}\nfragment AssetFields on Asset {id\n  address\n  chainId\n  decimals}\nquery {\n  Action(limit: 5, where: {chainId: {_eq: \"1\"}}) {\n    ...ActionFields\n  }\n  Asset(limit: 5, where: {chainId: {_eq: \"1\"}}) {\n    ...AssetFields\n  }\n}";
    assert_converts_to(subgraph, expected);
}

#[test]
fn test_gist_query_1_actions_and_assets() {
    let subgraph = r#"{
  actions(first: 5) {
    id
    block
    category
    chainId
  }
  assets(first: 5) {
    id
    address
    chainId
    decimals
  }
}"#;
    let expected = "query {\n  Action(limit: 5, where: {chainId: {_eq: \"1\"}}) {\n    id\n    block\n    category\n    chainId\n  }\n  Asset(limit: 5, where: {chainId: {_eq: \"1\"}}) {\n    id\n    address\n    chainId\n    decimals\n  }\n}";
    assert_converts_to(subgraph, expected);
}

#[test]
fn test_gist_query_2_streams_with_order_by() {
    let subgraph = r#"{
  streams(orderBy: id, skip: 10) {
    alias
    asset {
      address
    }
  }
}"#;
    let expected = "query {\n  Stream(offset: 10, order_by: {id: asc}, where: {chainId: {_eq: \"1\"}}) {\n    alias\n    asset {\n      address\n    }\n  }\n}";
    assert_converts_to(subgraph, expected);
}

#[test]
fn test_gist_query_3_streams_with_filter() {
    let subgraph = r#"{
  streams(orderBy: id, skip: 10, where: {alias_contains: "113"}) {
    alias
    asset {
      address
    }
  }
}"#;
    let expected = "query {\n  Stream(offset: 10, order_by: {id: asc}, where: {chainId: {_eq: \"1\"}, alias: {_ilike: \"%113%\"}}) {\n    alias\n    asset {\n      address\n    }\n  }\n}";
    assert_converts_to(subgraph, expected);
}

#[test]
fn test_gist_query_4_complex_nested_query() {
    let subgraph = r#"{
  streams(
    first: 10,
    skip: 5,
    where: {
      alias_contains: "test",
      asset: { address_starts_with: "0x" }
    }
  ) {
    id
    alias
    asset {
      address
      decimals
      symbol
    }
  }
}"#;
    let expected = "query {\n  Stream(limit: 10, offset: 5, where: {chainId: {_eq: \"1\"}, alias: {_ilike: \"%test%\"}, asset: {address: {_ilike: \"0x%\"}}}) {\n    id\n    alias\n    asset {\n      address\n      decimals\n      symbol\n    }\n  }\n}";
    assert_converts_to(subgraph, expected);
}

#[test]
fn test_gist_query_5_multiple_entities() {
    let subgraph = r#"{
  streams(first: 5, where: { alias_contains: "test" }) {
    id
    alias
  }
  actions(first: 3) {
    id
    category
  }
  assets(first: 2) {
    id
    address
    symbol
  }
}"#;
    let expected = "query {\n  Stream(limit: 5, where: {chainId: {_eq: \"1\"}, alias: {_ilike: \"%test%\"}}) {\n    id\n    alias\n  }\n  Action(limit: 3, where: {chainId: {_eq: \"1\"}}) {\n    id\n    category\n  }\n  Asset(limit: 2, where: {chainId: {_eq: \"1\"}}) {\n    id\n    address\n    symbol\n  }\n}";
    assert_converts_to(subgraph, expected);
}

#[test]
fn test_gist_query_6_advanced_filters() {
    let subgraph = r#"{
  streams(
    first: 20,
    where: {
      alias_contains: "test",
      alias_not_contains: "invalid"
    }
  ) {
    id
    alias
  }
}"#;
    let variant_a = "query {\n  Stream(limit: 20, where: {chainId: {_eq: \"1\"}, _and: [{alias: {_ilike: \"%test%\"}}, {_not: {alias: {_ilike: \"%invalid%\"}}}]}) {\n    id\n    alias\n  }\n}";
    let variant_b = "query {\n  Stream(limit: 20, where: {chainId: {_eq: \"1\"}, _and: [{_not: {alias: {_ilike: \"%invalid%\"}}}, {alias: {_ilike: \"%test%\"}}]}) {\n    id\n    alias\n  }\n}";
    assert_converts_to_any(subgraph, &[variant_a, variant_b]);
}

#[test]
fn test_gist_query_7_pagination_edge_cases() {
    let subgraph = r#"{
  streams(
    first: 1,
    skip: 999,
    orderBy: id,
    orderDirection: desc,
    where: { alias_contains: "test" }
  ) {
    id
    alias
  }
}"#;
    let expected = "query {\n  Stream(limit: 1, offset: 999, order_by: {id: desc}, where: {chainId: {_eq: \"1\"}, alias: {_ilike: \"%test%\"}}) {\n    id\n    alias\n  }\n}";
    assert_converts_to(subgraph, expected);
}

#[test]
fn test_gist_query_8_mixed_type_filters() {
    let subgraph = r#"{
  streams(
    where: {
      asset: {
        decimals_gte: 6,
        decimals_lte: 18
      }
    }
  ) {
    id
    asset { decimals }
  }
}"#;
    let variant_a = "query {\n  Stream(where: {chainId: {_eq: \"1\"}, asset: {_and: [{decimals: {_lte: 18}}, {decimals: {_gte: 6}}]}}) {\n    id\n    asset { decimals }\n  }\n}";
    let variant_b = "query {\n  Stream(where: {chainId: {_eq: \"1\"}, asset: {_and: [{decimals: {_gte: 6}}, {decimals: {_lte: 18}}]}}) {\n    id\n    asset { decimals }\n  }\n}";
    assert_converts_to_any(subgraph, &[variant_a, variant_b]);
}

#[test]
fn test_gist_query_9_case_sensitivity() {
    let subgraph = r#"{
  streams(
    where: {
      alias_contains: "TEST",
      alias_contains_nocase: "test"
    }
  ) {
    id
    alias
  }
}"#;
    let variant_a = "query {\n  Stream(where: {chainId: {_eq: \"1\"}, _and: [{alias: {_ilike: \"%test%\"}}, {alias: {_ilike: \"%TEST%\"}}]}) {\n    id\n    alias\n  }\n}";
    let variant_b = "query {\n  Stream(where: {chainId: {_eq: \"1\"}, _and: [{alias: {_ilike: \"%TEST%\"}}, {alias: {_ilike: \"%test%\"}}]}) {\n    id\n    alias\n  }\n}";
    assert_converts_to_any(subgraph, &[variant_a, variant_b]);
}

#[test]
fn test_gist_query_10_fragments() {
    let subgraph = r#"fragment ActionFields on Action {
  id
  block
  category
  chainId
}

fragment AssetFields on Asset {
  id
  address
  chainId
  decimals
}

query {
  actions(first: 5) {
    ...ActionFields
  }
  assets(first: 5) {
    ...AssetFields
  }
}"#;
    let expected = "fragment ActionFields on Action {id\n  block\n  category\n  chainId}\nfragment AssetFields on Asset {id\n  address\n  chainId\n  decimals}\nquery {\n  Action(limit: 5, where: {chainId: {_eq: \"1\"}}) {\n    ...ActionFields\n  }\n  Asset(limit: 5, where: {chainId: {_eq: \"1\"}}) {\n    ...AssetFields\n  }\n}";
    assert_converts_to(subgraph, expected);
}

// ─────────────────────────────────────────────────────────────────────────────
// Argus production queries (test-queries.md).
//
// Byte-exact goldens for the queries that exercise the three converter bugs
// their 31-query client hit. Unit-level coverage lives in `conversion::tests`;
// these pin the complete output string so formatting drift is caught too.
// ─────────────────────────────────────────────────────────────────────────────

/// Like `assert_converts_to`, but also asserts the forwarded variables. The
/// order-argument and whole-`where` rewrites both happen in the variable
/// payload, so they cannot be pinned without it.
fn assert_converts_with_variables(
    subgraph_query: &str,
    variables: Value,
    expected_query: &str,
    expected_variables: Value,
) {
    crate::schema::init_test_schema_once();
    let payload = json!({ "query": subgraph_query, "variables": variables });
    let result = conversion::convert_subgraph_to_hyperindex(&payload, Some("5042"))
        .expect("conversion should succeed");
    assert_eq!(
        result.query["query"].as_str().expect("query is a string"),
        expected_query
    );
    assert_eq!(result.query["variables"], expected_variables);
}

#[test]
fn argus_launch_key_page_end_to_end() {
    assert_converts_with_variables(
        "query LaunchKeyPage($where: Launch_filter!, $orderBy: Launch_orderBy!, $direction: OrderDirection!, $first: Int!) { launches(first: $first, where: $where, orderBy: $orderBy, orderDirection: $direction) { id key: createdAt } }",
        json!({"where": {"dividendsPaid_gt": "0"}, "orderBy": "createdAt", "direction": "desc", "first": 25}),
        "query LaunchKeyPage($where: Launch_bool_exp!, $orderBy: [Launch_order_by!], $first: Int!) {\n  Launch(limit: $first, order_by: $orderBy, where: $where) {\n    id key: createdAt\n  }\n}",
        json!({"where": {"dividendsPaid": {"_gt": "0"}}, "orderBy": [{"createdAt": "desc"}], "first": 25}),
    );
}

#[test]
fn argus_swaps_of_end_to_end() {
    assert_converts_with_variables(
        "query SwapsOf($launch: Bytes!, $from: BigInt!, $first: Int!, $skip: Int!, $direction: OrderDirection!) { swaps(first: $first, skip: $skip, where: { launch: $launch, timestamp_gte: $from }, orderBy: ordinal, orderDirection: $direction) { id ordinal } }",
        json!({"launch": "0xabc", "from": "100", "first": 50, "skip": 0, "direction": "desc"}),
        "query SwapsOf($launch: String!, $from: numeric!, $first: Int!, $skip: Int!, $direction: order_by!) {\n  Swap(limit: $first, offset: $skip, order_by: {ordinal: $direction}, where: {chainId: {_eq: \"5042\"}, launch: {id: {_eq: $launch}}, timestamp: {_gte: $from}}) {\n    id ordinal\n  }\n}",
        json!({"launch": "0xabc", "from": "100", "first": 50, "skip": 0, "direction": "desc"}),
    );
}

#[test]
fn argus_hours_of_launches_end_to_end() {
    assert_converts_with_variables(
        "query HoursOfLaunches($ids: [Bytes!]!, $since: BigInt!, $cursor: Bytes!, $first: Int!) { launchHourDatas(first: $first, where: { launch_in: $ids, periodStart_gte: $since, id_gt: $cursor }, orderBy: id) { id periodStart } }",
        json!({"ids": ["0xa", "0xb"], "since": "1700000000", "cursor": "0x", "first": 500}),
        "query HoursOfLaunches($ids: [String!]!, $since: numeric!, $cursor: String!, $first: Int!) {\n  LaunchHourData(limit: $first, order_by: {id: asc}, where: {chainId: {_eq: \"5042\"}, id: {_gt: $cursor}, launch: {id: {_in: $ids}}, periodStart: {_gte: $since}}) {\n    id periodStart\n  }\n}",
        json!({"ids": ["0xa", "0xb"], "since": "1700000000", "cursor": "0x", "first": 500}),
    );
}

/// Argus `Meta` asks for `block { timestamp }` and `hasIndexingErrors`, neither of
/// which Hyperindex can answer. The query is rejected rather than served partially:
/// a response missing `hasIndexingErrors` would read as "no indexing errors".
#[test]
fn argus_meta_end_to_end_is_rejected() {
    crate::schema::init_test_schema_once();
    let payload =
        json!({ "query": "query Meta { _meta { block { number timestamp } hasIndexingErrors } }" });
    match conversion::convert_subgraph_to_hyperindex(&payload, Some("5042")) {
        Err(conversion::ConversionError::ComplexMetaQuery(fields)) => {
            assert_eq!(fields, "block.timestamp, hasIndexingErrors")
        }
        other => panic!("expected ComplexMetaQuery, got {:?}", other.map(|r| r.query)),
    }
}

/// The same query reduced to what Hyperindex can serve.
#[test]
fn argus_meta_reduced_end_to_end() {
    crate::schema::init_test_schema_once();
    let payload = json!({ "query": "query Meta { _meta { block { number } } }" });
    let result = conversion::convert_subgraph_to_hyperindex(&payload, Some("5042"))
        .expect("block { number } is servable");
    assert_eq!(
        result.query["query"].as_str().unwrap(),
        "query {\n  chain_metadata {\n    latest_fetched_block_number\n  }\n}"
    );
    assert!(result.meta_selection.is_some());
}

/// Their keyset pager walks `id_gt` over `orderBy: id`. Breaking either the
/// entity name or the ordering returns an empty list rather than an error, so
/// this pins the whole string.
#[test]
fn argus_keyset_pagination_end_to_end() {
    assert_converts_with_variables(
        "query Launches($cursor: Bytes!, $first: Int!) { launches(first: $first, where: { id_gt: $cursor }, orderBy: id) { id name } }",
        json!({"cursor": "0xaa", "first": 1000}),
        "query Launches($cursor: String!, $first: Int!) {\n  Launch(limit: $first, order_by: {id: asc}, where: {chainId: {_eq: \"5042\"}, id: {_gt: $cursor}}) {\n    id name\n  }\n}",
        json!({"cursor": "0xaa", "first": 1000}),
    );
}
