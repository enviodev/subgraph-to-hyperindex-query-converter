# Subgraph to Hyperindex Query Converter

A standalone rust service intended to run alongside your hyperindex instance that converts TheGraph subgraph GraphQL queries to Hyperindex/Hasura GraphQL format and forwards them to a Hyperindex endpoint. The tool also converts the responses into the same response format as returned by TheGraph subgraphs. This is useful for automatically porting existing ui's or clients to reading from HyperIndex.

The tool is under active development and is not yet ready for production use. It is currently in a proof-of-concept stage and may not work as expected. It will likely never be a perfect map for all use cases however likely to work for most use cases.

## Features

- **Query Conversion**: Converts subgraph GraphQL syntax to Hyperindex format
- **HTTP Forwarding**: Forwards converted queries to Hyperindex endpoints
- **Environment Configuration**: Configurable endpoints & schema's via environment variables
- **Error Handling**: Comprehensive error handling and logging
- **Debug Endpoint**: Optional debug endpoint to inspect query conversion
- **Chain-Specific Queries**: Support for chain-specific queries via `/chainId/{chain_id}` endpoint

## API Endpoints

### Main Endpoint (`/`)

Converts and forwards queries to Hyperindex without adding any chain-specific filters.

```bash
curl -X POST -H "Content-Type: application/json" \
  -d '{"query": "query { streams(first: 2, skip: 10) { category cliff cliffTime chainId } }"}' \
  http://localhost:3000/
```

### Chain-Specific Endpoint (`/chainId/{chain_id}`)

Converts and forwards queries to Hyperindex, automatically adding a `chainId` filter to the where clause.

```bash
curl -X POST -H "Content-Type: application/json" \
  -d '{"query": "query { streams(first: 2, skip: 10) { category cliff cliffTime chainId } }"}' \
  http://localhost:3000/chainId/5
```

This will add `where: {chainId: {_eq: "5"}}` to the converted query.

### Debug Endpoint (`/debug`)

Returns the converted query without forwarding to Hyperindex.

```bash
curl -X POST -H "Content-Type: application/json" \
  -d '{"query": "query { streams(first: 2, skip: 10) { category cliff cliffTime chainId } }"}' \
  http://localhost:3000/debug
```

## Current Conversion Rules

### Entity Name Conversion

- Plural entity names are singularized and capitalized
- Example: `streams` → `Stream`

### Parameter Mapping

| Subgraph Parameter | Hyperindex Parameter | Notes                               |
| ------------------ | -------------------- | ----------------------------------- |
| `first`            | `limit`              | Number of records to return         |
| `skip`             | `offset`             | Number of records to skip           |
| `orderBy`          | `order_by`           | Field to sort by (currently unused) |
| `orderDirection`   | `order_by` direction | Sort direction (currently unused)   |

### Chain ID Handling

- **Default endpoints (`/` and `/debug`)**: No `chainId` filter is added
- **Chain-specific endpoint (`/chainId/{chain_id}`)**: Automatically adds `where: {chainId: {_eq: "{chain_id}"}}` to the query
- **Single Entity by Primary Key**: Singular entity queries with only an `id` parameter are converted to `entity_by_pk(id: ...)` format (no chainId filter)

### Special Handling

- **Selection Sets**: Preserved as-is in the converted query
- **Single Entity by Primary Key**: Singular entity queries with only an `id` parameter are converted to `entity_by_pk(id: ...)` format

### Filter Conversions

The following table shows how TheGraph filter syntax is converted to Hasura equivalents:

| The Graph Filter               | Hasura Equivalent                      | Description                          | Example (The Graph)                  | Example (Hasura)                        |
| ------------------------------ | -------------------------------------- | ------------------------------------ | ------------------------------------ | --------------------------------------- |
| `field`                        | `field: { _eq: val }`                  | Equal                                | `name: "Alice"`                      | `name: { _eq: "Alice" }`                |
| `field_not`                    | `field: { _neq: val }`                 | Not equal                            | `id_not: "0x123"`                    | `id: { _neq: "0x123" }`                 |
| `field_gt`                     | `field: { _gt: val }`                  | Greater than                         | `value_gt: 100`                      | `value: { _gt: 100 }`                   |
| `field_gte`                    | `field: { _gte: val }`                 | Greater than or equal                | `value_gte: 100`                     | `value: { _gte: 100 }`                  |
| `field_lt`                     | `field: { _lt: val }`                  | Less than                            | `timestamp_lt: 1650000000`           | `timestamp: { _lt: 1650000000 }`        |
| `field_lte`                    | `field: { _lte: val }`                 | Less than or equal                   | `timestamp_lte: 1650000000`          | `timestamp: { _lte: 1650000000 }`       |
| `field_in`                     | `field: { _in: [...] }`                | Matches any in array                 | `status_in: ["OPEN", "CLOSED"]`      | `status: { _in: ["OPEN", "CLOSED"] }`   |
| `field_not_in`                 | `field: { _nin: [...] }`               | Excludes values in array             | `id_not_in: ["0x1", "0x2"]`          | `id: { _nin: ["0x1", "0x2"] }`          |
| `field_contains`               | `field: { _ilike: "%val%" }`           | Substring match (case-insensitive)   | `name_contains: "graph"`             | `name: { _ilike: "%graph%" }`           |
| `field_not_contains`           | `field: { _not: { _ilike: "%val%" } }` | Substring mismatch                   | `name_not_contains: "graph"`         | `name: { _not: { _ilike: "%graph%" } }` |
| `field_starts_with`            | `field: { _ilike: "val%" }`            | Starts with                          | `symbol_starts_with: "ETH"`          | `symbol: { _ilike: "ETH%" }`            |
| `field_ends_with`              | `field: { _ilike: "%val" }`            | Ends with                            | `symbol_ends_with: "USD"`            | `symbol: { _ilike: "%USD" }`            |
| `field_not_starts_with`        | `field: { _not: { _ilike: "val%" } }`  | Doesn't start with                   | `name_not_starts_with: "A"`          | `name: { _not: { _ilike: "A%" } }`      |
| `field_not_ends_with`          | `field: { _not: { _ilike: "%val" } }`  | Doesn't end with                     | `name_not_ends_with: "x"`            | `name: { _not: { _ilike: "%x" } }`      |
| `field_contains_nocase`        | `field: { _ilike: "%val%" }`           | Substring match, case-insensitive    | `name_contains_nocase: "alice"`      | `name: { _ilike: "%alice%" }`           |
| `field_not_contains_nocase`    | `field: { _not: { _ilike: "%val%" } }` | Substring mismatch, case-insensitive | `name_not_contains_nocase: "alice"`  | `name: { _not: { _ilike: "%alice%" } }` |
| `field_starts_with_nocase`     | `field: { _ilike: "val%" }`            | Case-insensitive prefix match        | `id_starts_with_nocase: "0xabc"`     | `id: { _ilike: "0xabc%" }`              |
| `field_ends_with_nocase`       | `field: { _ilike: "%val" }`            | Case-insensitive suffix match        | `id_ends_with_nocase: "def"`         | `id: { _ilike: "%def" }`                |
| `field_not_starts_with_nocase` | `field: { _not: { _ilike: "val%" } }`  | Case-insensitive negated prefix      | `name_not_starts_with_nocase: "foo"` | `name: { _not: { _ilike: "foo%" } }`    |
| `field_not_ends_with_nocase`   | `field: { _not: { _ilike: "%val" } }`  | Case-insensitive negated suffix      | `name_not_ends_with_nocase: "bar"`   | `name: { _not: { _ilike: "%bar" } }`    |
| `field_containsAny`            | ❌ No direct equivalent                | Array overlap (string[] fields)      | `tags_containsAny: ["foo", "bar"]`   | ❌ Requires custom SQL                  |
| `field_containsAll`            | ❌ No direct equivalent                | Field contains all values            | `tags_containsAll: ["foo", "bar"]`   | ❌                                      |
| `id (top-level)`               | `entity_by_pk(id: ...)`                | Get by primary key                   | `user(id: "0x123")`                  | `user_by_pk(id: "0x123")`               |

## Setup

### Prerequisites

- Rust (latest stable version)
- Cargo

### Installation

1. Clone the repository:

```bash
git clone <repository-url>
cd subgraph-to-hyperindex-query-converter-poc
```

2. Create environment configuration:

```bash
cp .env.example .env
# Edit .env with your Hyperindex URL
```

3. Build and run:

```bash
cargo run
```

The service will start on `http://localhost:3000`

## Configuration

### Environment Variables

Create a `.env` file in the project root:

```env
# Required
HYPERINDEX_URL=https://indexer.hyperindex.xyz/53b7e25/v1/graphql

# Optional Configuration
PORT=3000                           # Server port (default: 3000)
HTTP_TIMEOUT_SECS=30               # Request timeout in seconds (default: 30)
SCHEMA_REFRESH_COOLDOWN_SECS=30    # Minimum seconds between schema refreshes (default: 30)

# Optional - Debug/Subgraph Comparison
SUBGRAPH_DEBUG_URL=                # URL for subgraph comparison on errors
SUBGRAPH_BEARER_TOKEN=             # Bearer token for subgraph auth
SUBGRAPH_API_KEY=                  # API key for subgraph auth
SUBGRAPH_AUTH_HEADER=              # Custom auth header name
SUBGRAPH_AUTH_VALUE=               # Custom auth header value
```

## Usage

### Main Endpoint

POST requests to `/` will convert and forward queries to Hyperindex without adding chain-specific filters:

```bash
curl -X POST -H "Content-Type: application/json" \
  -d '{"query": "query { streams(first: 2, skip: 10) { category cliff cliffTime chainId } }"}' \
  http://localhost:3000/
```

### Chain-Specific Endpoint

POST requests to `/chainId/{chain_id}` will convert and forward queries to Hyperindex, automatically adding a `chainId` filter:

```bash
curl -X POST -H "Content-Type: application/json" \
  -d '{"query": "query { streams(first: 2, skip: 10) { category cliff cliffTime chainId } }"}' \
  http://localhost:3000/chainId/5
```

### Debug Endpoint

POST requests to `/debug` will return the converted query without forwarding:

```bash
curl -X POST -H "Content-Type: application/json" \
  -d '{"query": "query { streams(first: 2, skip: 10) { category cliff cliffTime chainId } }"}' \
  http://localhost:3000/debug
```

## Metrics

The converter exposes Prometheus metrics at `/metrics` endpoint.

### Quick View (Local Development)

**Option 1: Simple Script**
```bash
./view_metrics.sh
# Or with custom URL:
./view_metrics.sh http://localhost:3000/metrics
```

**Option 2: Direct curl**
```bash
curl http://localhost:3000/metrics
```

**Option 3: Pretty formatted (using jq if available)**
```bash
curl -s http://localhost:3000/metrics | grep -E "^converter_" | sort
```

### Full Monitoring Stack (Docker)

For visual dashboards with Grafana:

1. Start Prometheus + Grafana:
```bash
docker-compose -f docker-compose.metrics.yml up -d
```

2. Access:
   - **Grafana**: http://localhost:3001 (admin/admin)
   - **Prometheus**: http://localhost:9090

3. Import Grafana dashboard:
   - Go to Grafana → Dashboards → Import
   - Use the Prometheus queries from `METRICS_IMPLEMENTATION_GUIDE.md`

4. Stop when done:
```bash
docker-compose -f docker-compose.metrics.yml down
```

### Available Metrics

- `converter_requests_total` - Total requests processed
- `converter_request_duration_seconds` - Total request latency
- `converter_query_conversion_duration_seconds` - Query conversion time
- `converter_query_response_wait_duration_seconds` - Time waiting for Hyperindex
- `converter_response_transform_duration_seconds` - Response transformation time
- `converter_schema_refreshes_total` - Schema refresh count
- `converter_schema_fetch_duration_seconds` - Schema fetch time
- `converter_conversion_errors_total` - Conversion errors
- `converter_query_errors_total` - Query execution errors

## Example Query Conversions

### Collection Query (Default Endpoint)

#### Input (Subgraph Format)

```graphql
query {
  streams(first: 2, skip: 10) {
    category
    cliff
    cliffTime
    chainId
  }
}
```

#### Output (Hyperindex Format)

```graphql
query {
  Stream(limit: 2, offset: 10) {
    category
    cliff
    cliffTime
    chainId
  }
}
```

### Collection Query (Chain-Specific Endpoint)

#### Input (Subgraph Format)

```graphql
query {
  streams(first: 2, skip: 10) {
    category
    cliff
    cliffTime
    chainId
  }
}
```

#### Output (Hyperindex Format) - via `/chainId/5`

```graphql
query {
  Stream(limit: 2, offset: 10, where: { chainId: { _eq: "5" } }) {
    category
    cliff
    cliffTime
    chainId
  }
}
```

### Single Entity Query

#### Input (Subgraph Format)

```graphql
query {
  post(id: "0xabc...") {
    title
  }
}
```

#### Output (Hyperindex Format)

```graphql
query {
  post_by_pk(id: "0xabc...") {
    title
  }
}
```

### Response

```json
{
  "data": {
    "Stream": [
      {
        "category": "LockupDynamic",
        "chainId": "1",
        "cliff": false,
        "cliffTime": null
      },
      {
        "category": "LockupLinear",
        "chainId": "1",
        "cliff": false,
        "cliffTime": null
      }
    ]
  }
}
```

## Current Limitations

### Known Issues

1. **Basic Parsing**: Uses simple string parsing instead of a proper GraphQL parser
2. **Limited Entity Support**: Currently optimized for Stream entities
3. **Order By**: `orderBy` and `orderDirection` parameters are extracted but not used in conversion
4. **No Block Queries**: Time-traveling queries with `block` parameters are not supported as Hyperindex doesn't natively support historical queries
5. **Data Limit**: Unless Hyperindex is configured via environment variables to support 5000 datapoints, the `limit` parameter should be set to a maximum of 1000
6. **\_meta Queries**: Meta queries are limitted only to latest block number

### Planned Improvements

- [ ] Use proper GraphQL parser for robust query handling

- [ ] Add support for variables and directives
- [ ] Implement proper order_by conversion
- [ ] Add comprehensive test coverage

## Development

### Project Structure

```
src/
├── main.rs          # HTTP server and routing
└── conversion.rs    # Query conversion logic
```

### Adding New Conversion Rules

To add support for new entities or conversion rules, modify the `convert_query_structure` function in `src/conversion.rs`.

### Testing

```bash
# Check compilation
cargo check

# Run with debug output
RUST_LOG=debug cargo run

# Test conversion only
cargo test
```

### RUN Docker Locally

build the docker file with a tag
docker build -t subgraph-converter .

Create a .env file based on the `.env.example` file and run the following:
docker run -p 3000:3000 --env-file .env subgraph-converter

test query: 
```
curl -X POST -H "Content-Type: application/json" \
  -d '{"query": "query { streams(first: 2, skip: 10) { category cliff cliffTime chainId } }"}' \
  http://localhost:3000/
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
