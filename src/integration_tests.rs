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
