use prometheus::{
    Counter, Histogram, HistogramOpts, Opts, Registry,
    Encoder, TextEncoder,
};
use once_cell::sync::Lazy;

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
// HELPER FUNCTIONS
// ============================================================================

/// Get metrics in Prometheus text format
pub fn gather_metrics() -> Result<String, prometheus::Error> {
    let encoder = TextEncoder::new();
    let metric_families = REGISTRY.gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer)?;
    let output = String::from_utf8(buffer).unwrap();

    Ok(output)
}

