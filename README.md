# Subgraph to HyperIndex Query Converter

[![Discord](https://img.shields.io/badge/Discord-Join%20Chat-7289da?logo=discord&logoColor=white)](https://discord.com/invite/envio)

A standalone Rust service that converts [The Graph](https://thegraph.com) subgraph GraphQL queries to [Envio HyperIndex](https://docs.envio.dev/docs/HyperIndex/overview) / Hasura GraphQL format and forwards them to a HyperIndex endpoint. Responses are converted back to The Graph subgraph format, making it a transparent proxy that lets existing frontends and clients read from HyperIndex without code changes.

> **Note:** This tool is under active development and is currently in a proof-of-concept stage. It is not yet ready for production use. It will likely work for most common subgraph query patterns but may not cover all edge cases.

## What Problem Does This Solve?

When migrating from a subgraph to HyperIndex, existing frontends use The Graph's GraphQL query syntax (e.g. `streams(first: 2, orderBy: timestamp)`) which is different from HyperIndex's Hasura-based syntax (e.g. `Stream(limit: 2, order_by: {timestamp: asc})`). This converter acts as a middleware proxy so you can point your existing frontend at it without rewriting all your queries.

See the [HyperIndex query conversion guide](https://docs.envio.dev/docs/HyperIndex/query-conversion) for a manual reference on the differences.

## Who is this for?

Teams migrating from The Graph to HyperIndex who want to keep their existing frontend queries working during the transition, without rewriting all GraphQL calls upfront. Point your frontend at this proxy, migrate your indexer to HyperIndex, and update your queries at your own pace.

## Features

- **Query conversion**: Converts subgraph GraphQL syntax to HyperIndex / Hasura format
- **Response conversion**: Converts responses back to subgraph format
- **HTTP proxy**: Forwards converted queries to your HyperIndex endpoint
- **Chain-specific endpoint**: Automatically adds `chainId` filters via `/chainId/{chain_id}`
- **Debug endpoint**: Inspect converted queries without forwarding (`/debug`)
- **Prometheus metrics**: Request latency, error rates, conversion timing
- **Filter mapping**: Translates all common subgraph filter operators (`_gt`, `_in`, `_contains`, etc.) to Hasura equivalents

## API Endpoints

| Endpoint | Description |
|---|---|
| `POST /` | Convert and forward to HyperIndex (no chain filter) |
| `POST /chainId/{id}` | Convert and forward, auto-adds `chainId` filter |
| `POST /debug` | Return converted query without forwarding |
| `GET /metrics` | Prometheus metrics |

## Setup

### Prerequisites

- Rust (latest stable)

### Install and Run

```bash
git clone <repository-url>
cd subgraph-to-hyperindex-query-converter

cp .env.example .env
# Set HYPERINDEX_URL in .env

cargo run
```

Service starts on `http://localhost:3000`.

### Environment Variables

```env
# Required
HYPERINDEX_URL=https://indexer.hyperindex.xyz/your-deployment/v1/graphql

# Optional
PORT=3000
HTTP_TIMEOUT_SECS=30
# Set to off/false/0 to stop appending `id` to every `orderBy` (see "Ordering")
CONVERTER_ORDER_ID_TIEBREAK=on
```

### Run with Docker

```bash
docker build -t subgraph-converter .
docker run -p 3000:3000 --env-file .env subgraph-converter
```

## Example Conversions

**Subgraph input:**
```graphql
query {
  streams(first: 2, skip: 10) {
    category
    cliff
    chainId
  }
}
```

**HyperIndex output (at `/`):**
```graphql
query {
  Stream(limit: 2, offset: 10) {
    category
    cliff
    chainId
  }
}
```

**HyperIndex output (at `/chainId/1`):**
```graphql
query {
  Stream(limit: 2, offset: 10, where: { chainId: { _eq: "1" } }) {
    category
    cliff
    chainId
  }
}
```

## Filter Conversion Reference

| Subgraph Filter | Hasura Equivalent |
|---|---|
| `field: val` | `field: { _eq: val }` |
| `field_not: val` | `field: { _neq: val }` |
| `field_gt: val` | `field: { _gt: val }` |
| `field_gte: val` | `field: { _gte: val }` |
| `field_lt: val` | `field: { _lt: val }` |
| `field_lte: val` | `field: { _lte: val }` |
| `field_in: [...]` | `field: { _in: [...] }` |
| `field_not_in: [...]` | `field: { _nin: [...] }` |
| `field_contains: val` | `field: { _ilike: "%val%" }` |
| `field_starts_with: val` | `field: { _ilike: "val%" }` |
| `field_ends_with: val` | `field: { _ilike: "%val" }` |

See the full filter table and known limitations in the source code.

## Interfaces and type-conditioned fragments

HyperIndex has no GraphQL interfaces, so an indexer that ports a subgraph `interface` models it as a concrete entity with one nullable link per implementing type (for example `UserTransaction` with `supply: Supply`, `borrow: Borrow`, ...). Hasura silently ignores a subgraph-style `... on Supply { amount }` against such an entity, so every row used to come back with only the shared fields.

The converter now handles this itself, with no per-interface configuration:

- `... on Supply { amount }` is rewritten to an aliased link field, `_on_Supply: supply { amount }`. The link is found from the introspected schema: the single object-valued field on the surrounding type whose type is `Supply`.
- Named fragments work the same way (`...SupplyFields` where `fragment SupplyFields on Supply` becomes `_on_Supply: supply { ...SupplyFields }`), as do fragments inside named fragments and inside nested selections such as `user { userTransactions { ... } }`.
- A fragment whose type condition is the surrounding type, or has none, is flattened into it.
- In the response, each non-null `_on_*` object is merged into its row and the key removed, so rows look exactly like the subgraph's: fragment fields present for the matching type and absent for the others.

A fragment is left untouched, as before, when the surrounding type is unknown, no field links to the fragment's type, or several do. A warning is logged. `__typename` is not translated: it still reports the concrete HyperIndex entity, not the implementing type. Fragments on an interface a type *implements* (`supplies { ... on UserTransaction { id } }`) are not resolved, because the schema carries no interface information.

## Ordering

graph-node orders rows that tie on the sort key by `id`, in the same direction (`ORDER BY timestamp DESC, id DESC`). Hasura leaves ties unordered, so with `first`/`skip` a page boundary inside a run of equal keys could repeat or skip rows. Every explicit `orderBy` now also sorts by `id` in the same direction (`order_by: [{timestamp: desc}, {id: desc}]`), for both literal and variable `orderBy`. Sorting by `id` itself adds nothing. This gives a total, repeatable order, so paging is consistent.

Set `CONVERTER_ORDER_ID_TIEBREAK=off` to emit the sort key alone, for example if the extra sort column is slow on a very large table.

Two things this does not do:

- A query with no `orderBy` is still unordered. graph-node defaults to `id` ascending, but forcing that here could push the planner onto the primary key for selective filters.
- Ties follow the database's string collation. graph-node compares `id` bytewise (C collation), while Postgres usually uses a locale collation that ignores punctuation, so ids that differ only around a `:` boundary (`1:` versus `15:`) can order differently inside a single block. The order is still stable and repeatable. It just may not match the subgraph's within-block order exactly.

## Known Limitations

- Uses string parsing rather than a full GraphQL parser
- `orderBy` and `orderDirection` with variables are not supported (Hasura limitation)
- Block/time-travel queries are not supported
- `_meta` queries only return latest block number
- Default limit should not exceed 1000 unless HyperIndex is configured for higher limits

## Documentation

- [HyperIndex Docs](https://docs.envio.dev/docs/HyperIndex/overview)
- [Subgraph to HyperIndex query conversion guide](https://docs.envio.dev/docs/HyperIndex/query-conversion)
- [Migrate from The Graph to Envio](https://docs.envio.dev/docs/HyperIndex/migration-guide)

## License

MIT

## Support

- [Discord community](https://discord.com/invite/envio)
- [Envio Docs](https://docs.envio.dev)
