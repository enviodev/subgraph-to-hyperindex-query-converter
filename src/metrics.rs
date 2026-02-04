use prometheus::{
    Counter, Histogram, HistogramOpts, Opts, Registry,
    Encoder, TextEncoder,
};
use once_cell::sync::Lazy;
use std::sync::Mutex;
use std::collections::HashMap;
use sha2::{Sha256, Digest};

// Global registry - initialized once
pub static REGISTRY: Lazy<Registry> = Lazy::new(Registry::new);

// ============================================================================
// REQUEST METRICS
// ============================================================================

/// Total number of requests processed
pub static REQUEST_COUNTER: Lazy<Counter> = Lazy::new(|| {
    let counter = Counter::with_opts(
        Opts::new("converter_requests_total", "Total number of requests processed")
            .namespace("converter")
    ).unwrap();
    REGISTRY.register(Box::new(counter.clone())).unwrap();
    counter
});

/// Request duration histogram (includes all phases)
pub static REQUEST_DURATION: Lazy<Histogram> = Lazy::new(|| {
    let histogram = Histogram::with_opts(
        HistogramOpts::new(
            "converter_request_duration_milliseconds",
            "Total request duration in milliseconds"
        )
        .namespace("converter")
        .buckets(vec![1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0])
    ).unwrap();
    REGISTRY.register(Box::new(histogram.clone())).unwrap();
    histogram
});

// ============================================================================
// CONVERSION METRICS
// ============================================================================

/// Time spent converting subgraph QL to standard QL
pub static CONVERSION_DURATION: Lazy<Histogram> = Lazy::new(|| {
    let histogram = Histogram::with_opts(
        HistogramOpts::new(
            "converter_query_conversion_duration_milliseconds",
            "Time spent converting subgraph QL query to standard QL (milliseconds)"
        )
        .namespace("converter")
        .buckets(vec![0.1, 0.5, 1.0, 5.0, 10.0, 25.0, 50.0, 100.0])
    ).unwrap();
    REGISTRY.register(Box::new(histogram.clone())).unwrap();
    histogram
});

/// Time spent transforming response from standard QL back to subgraph format
pub static RESPONSE_TRANSFORM_DURATION: Lazy<Histogram> = Lazy::new(|| {
    let histogram = Histogram::with_opts(
        HistogramOpts::new(
            "converter_response_transform_duration_milliseconds",
            "Time spent transforming response from standard QL to subgraph format (milliseconds)"
        )
        .namespace("converter")
        .buckets(vec![0.1, 0.5, 1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0])
    ).unwrap();
    REGISTRY.register(Box::new(histogram.clone())).unwrap();
    histogram
});

// ============================================================================
// NETWORK METRICS
// ============================================================================

/// Time waiting for response after sending converted query to Hyperindex
pub static QUERY_RESPONSE_WAIT_DURATION: Lazy<Histogram> = Lazy::new(|| {
    let histogram = Histogram::with_opts(
        HistogramOpts::new(
            "converter_query_response_wait_duration_milliseconds",
            "Time waiting for response after sending converted query to Hyperindex (milliseconds)"
        )
        .namespace("converter")
        .buckets(vec![10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0, 30000.0])
    ).unwrap();
    REGISTRY.register(Box::new(histogram.clone())).unwrap();
    histogram
});

// ============================================================================
// SCHEMA METRICS
// ============================================================================

/// Total number of schema refreshes
pub static SCHEMA_REFRESH_COUNTER: Lazy<Counter> = Lazy::new(|| {
    let counter = Counter::with_opts(
        Opts::new("converter_schema_refreshes_total", "Total number of schema refreshes")
            .namespace("converter")
    ).unwrap();
    REGISTRY.register(Box::new(counter.clone())).unwrap();
    counter
});

/// Time spent fetching schema via introspection
pub static SCHEMA_FETCH_DURATION: Lazy<Histogram> = Lazy::new(|| {
    let histogram = Histogram::with_opts(
        HistogramOpts::new(
            "converter_schema_fetch_duration_milliseconds",
            "Time spent fetching schema via introspection (milliseconds)"
        )
        .namespace("converter")
        .buckets(vec![100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0, 30000.0])
    ).unwrap();
    REGISTRY.register(Box::new(histogram.clone())).unwrap();
    histogram
});

/// Schema cache lookups (hits are implicit - we track misses)
pub static SCHEMA_CACHE_LOOKUPS: Lazy<Counter> = Lazy::new(|| {
    let counter = Counter::with_opts(
        Opts::new("converter_schema_cache_lookups_total", "Total number of schema cache lookups")
            .namespace("converter")
    ).unwrap();
    REGISTRY.register(Box::new(counter.clone())).unwrap();
    counter
});

// ============================================================================
// ERROR METRICS
// ============================================================================

/// Total number of conversion errors
pub static CONVERSION_ERRORS: Lazy<Counter> = Lazy::new(|| {
    let counter = Counter::with_opts(
        Opts::new("converter_conversion_errors_total", "Total number of conversion errors")
            .namespace("converter")
    ).unwrap();
    REGISTRY.register(Box::new(counter.clone())).unwrap();
    counter
});

/// Total number of query execution errors (HTTP failures or GraphQL errors from Hyperindex)
pub static QUERY_EXECUTION_ERRORS: Lazy<Counter> = Lazy::new(|| {
    let counter = Counter::with_opts(
        Opts::new("converter_query_execution_errors_total", "Total number of query execution errors (HTTP or GraphQL)")
            .namespace("converter")
    ).unwrap();
    REGISTRY.register(Box::new(counter.clone())).unwrap();
    counter
});

/// Total number of all errors (conversion + query execution)
pub static TOTAL_ERRORS: Lazy<Counter> = Lazy::new(|| {
    let counter = Counter::with_opts(
        Opts::new("converter_errors_total", "Total number of all errors (conversion + query execution)")
            .namespace("converter")
    ).unwrap();
    REGISTRY.register(Box::new(counter.clone())).unwrap();
    counter
});

/// Total number of schema refresh errors
pub static SCHEMA_REFRESH_ERRORS: Lazy<Counter> = Lazy::new(|| {
    let counter = Counter::with_opts(
        Opts::new("converter_schema_refresh_errors_total", "Total number of schema refresh errors")
            .namespace("converter")
    ).unwrap();
    REGISTRY.register(Box::new(counter.clone())).unwrap();
    counter
});

// ============================================================================
// QUERY TRACKING (Top N queries by frequency)
// ============================================================================

/// Maximum number of queries to track (to limit memory usage)
const MAX_TRACKED_QUERIES: usize = 20;

/// Query statistics for a single query
#[derive(Clone, Debug)]
struct QueryStats {
    count: u64,
    total_conversion_time_ms: f64,
    total_response_time_ms: f64,
    total_time_ms: f64, // Round trip: conversion + response wait
    max_conversion_time_ms: f64,
    max_response_time_ms: f64,
    max_total_time_ms: f64,
    query_preview: String, // Full query text for identification
}

/// In-memory tracking of top queries
static QUERY_STATS: Lazy<Mutex<HashMap<String, QueryStats>>> = Lazy::new(|| {
    Mutex::new(HashMap::new())
});

/// Hash a query string to a short identifier
fn hash_query(query: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(query.as_bytes());
    let hash = hasher.finalize();
    // Use first 16 hex chars as identifier
    format!("{:x}", hash)[..16].to_string()
}

/// Normalize query string for consistent hashing (remove extra whitespace)
fn normalize_query(query: &str) -> String {
    // Remove leading/trailing whitespace and normalize internal whitespace
    query
        .lines()
        .map(|l| l.trim())
        .filter(|l| !l.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

/// Track a query execution
/// This is lightweight - just a hash calculation and map update
pub fn track_query(
    query: &str,
    conversion_time_ms: f64,
    response_time_ms: f64,
    total_time_ms: f64,
) {
    let normalized = normalize_query(query);
    let hash = hash_query(&normalized);
    
    let mut stats = QUERY_STATS.lock().unwrap();
    
    // Get or create entry
    let entry = stats.entry(hash.clone()).or_insert_with(|| {
        QueryStats {
            count: 0,
            total_conversion_time_ms: 0.0,
            total_response_time_ms: 0.0,
            total_time_ms: 0.0,
            max_conversion_time_ms: 0.0,
            max_response_time_ms: 0.0,
            max_total_time_ms: 0.0,
            query_preview: normalized.clone(),
        }
    });
    
    // Update statistics
    entry.count += 1;
    entry.total_conversion_time_ms += conversion_time_ms;
    entry.total_response_time_ms += response_time_ms;
    entry.total_time_ms += total_time_ms;
    entry.max_conversion_time_ms = entry.max_conversion_time_ms.max(conversion_time_ms);
    entry.max_response_time_ms = entry.max_response_time_ms.max(response_time_ms);
    entry.max_total_time_ms = entry.max_total_time_ms.max(total_time_ms);
    
    // If we exceed the limit, remove least frequent queries
    if stats.len() > MAX_TRACKED_QUERIES {
        let mut entries: Vec<(String, u64)> = stats.iter()
            .map(|(hash, stats)| (hash.clone(), stats.count))
            .collect();
        entries.sort_by_key(|(_, count)| *count);
        // Remove bottom 20% of least frequent queries
        let remove_count = MAX_TRACKED_QUERIES / 5;
        for (hash, _) in entries.iter().take(remove_count) {
            stats.remove(hash);
        }
    }
}

/// Get top queries statistics as Prometheus-formatted metrics
fn get_query_stats_metrics() -> String {
    let stats = QUERY_STATS.lock().unwrap();
    
    if stats.is_empty() {
        return String::new();
    }
    
    // Collect into owned vectors to avoid borrow checker issues
    let entries: Vec<(String, QueryStats)> = stats.iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    
    // Sort by frequency (count) descending for most frequent queries
    let mut top_by_count = entries.clone();
    top_by_count.sort_by(|a, b| b.1.count.cmp(&a.1.count));
    let top_by_count: Vec<_> = top_by_count.iter().take(10).collect();
    
    // Sort by average total time descending for slowest queries
    let mut top_by_avg_time = entries.clone();
    top_by_avg_time.sort_by(|a, b| {
        let avg_a = if a.1.count > 0 { a.1.total_time_ms / a.1.count as f64 } else { 0.0 };
        let avg_b = if b.1.count > 0 { b.1.total_time_ms / b.1.count as f64 } else { 0.0 };
        avg_b.partial_cmp(&avg_a).unwrap_or(std::cmp::Ordering::Equal)
    });
    let top_by_avg_time: Vec<_> = top_by_avg_time.iter().take(10).collect();
    
    // Sort by max total time descending for queries with worst single execution
    let mut top_by_max_time = entries.clone();
    top_by_max_time.sort_by(|a, b| {
        b.1.max_total_time_ms.partial_cmp(&a.1.max_total_time_ms).unwrap_or(std::cmp::Ordering::Equal)
    });
    let top_by_max_time: Vec<_> = top_by_max_time.iter().take(10).collect();
    
    let mut output = String::new();
    
    // Export summary metrics (ranked, no hash needed)
    output.push_str("# Top 10 Most Frequent Queries\n");
    for (rank, (_hash, query_stats)) in top_by_count.iter().enumerate() {
        let rank_str = (rank + 1).to_string();
        let query_preview = query_stats.query_preview
            .replace('"', r#"\""#)
            .replace('\n', r#"\n"#);
        output.push_str(&format!(
            "converter_top_query_by_count{{rank=\"{}\",query_preview=\"{}\"}} {}\n",
            rank_str, query_preview, query_stats.count
        ));
    }
    
    output.push_str("\n# Top 10 Slowest Queries (by average total time)\n");
    for (rank, (_hash, query_stats)) in top_by_avg_time.iter().enumerate() {
        let rank_str = (rank + 1).to_string();
        let avg_total_ms = if query_stats.count > 0 {
            query_stats.total_time_ms / query_stats.count as f64
        } else {
            0.0
        };
        let query_preview = query_stats.query_preview
            .replace('"', r#"\""#)
            .replace('\n', r#"\n"#);
        output.push_str(&format!(
            "converter_top_query_by_avg_time_milliseconds{{rank=\"{}\",query_preview=\"{}\"}} {:.3}\n",
            rank_str, query_preview, avg_total_ms
        ));
    }
    
    output.push_str("\n# Top 10 Queries with Worst Single Execution Time\n");
    for (rank, (_hash, query_stats)) in top_by_max_time.iter().enumerate() {
        let rank_str = (rank + 1).to_string();
        let query_preview = query_stats.query_preview
            .replace('"', r#"\""#)
            .replace('\n', r#"\n"#);
        output.push_str(&format!(
            "converter_top_query_by_max_time_milliseconds{{rank=\"{}\",query_preview=\"{}\"}} {:.3}\n",
            rank_str, query_preview, query_stats.max_total_time_ms
        ));
    }
    
    // Also export detailed metrics with hash (for advanced users)
    output.push_str("\n# Detailed Query Metrics (by hash)\n");
    let mut detailed_entries = entries;
    detailed_entries.sort_by(|a, b| b.1.count.cmp(&a.1.count));
    for (hash, query_stats) in detailed_entries.iter().take(MAX_TRACKED_QUERIES) {
        let avg_total_ms = if query_stats.count > 0 {
            query_stats.total_time_ms / query_stats.count as f64
        } else {
            0.0
        };
        
        let query_preview = query_stats.query_preview
            .replace('"', r#"\""#)
            .replace('\n', r#"\n"#);
        
        output.push_str(&format!(
            "converter_query_count{{query_hash=\"{}\",query_preview=\"{}\"}} {}\n",
            hash, query_preview, query_stats.count
        ));
        output.push_str(&format!(
            "converter_query_avg_total_time_milliseconds{{query_hash=\"{}\"}} {:.3}\n",
            hash, avg_total_ms
        ));
    }
    
    output
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

/// Get metrics in Prometheus text format
pub fn gather_metrics() -> Result<String, prometheus::Error> {
    let encoder = TextEncoder::new();
    let metric_families = REGISTRY.gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer)?;
    let mut output = String::from_utf8(buffer).unwrap();
    
    // Append query statistics
    let query_stats = get_query_stats_metrics();
    if !query_stats.is_empty() {
        output.push_str("\n# Query Statistics (Top queries by frequency)\n");
        output.push_str(&query_stats);
    }
    
    Ok(output)
}

