use once_cell::sync::Lazy;
use reqwest::Client;
use std::time::Duration;

/// Shared HTTP client with connection pooling for all requests
/// This is critical for performance - reusing connections instead of creating new ones
pub static HTTP_CLIENT: Lazy<Client> = Lazy::new(|| {
    // Get timeout from environment if needed (default: 30 seconds)
    let timeout_secs = std::env::var("HTTP_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(30);

    tracing::info!("Creating shared HTTP client with connection pooling (timeout={}s)", timeout_secs);

    Client::builder()
        .pool_max_idle_per_host(10) // Keep 10 idle connections per host for reuse
        .timeout(Duration::from_secs(timeout_secs))
        .connect_timeout(Duration::from_secs(10))
        .build()
        .expect("Failed to create HTTP client")
});

