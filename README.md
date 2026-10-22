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
- **Filter mapping**: Translates all common subgraph filter operators (`_gt`, `_in`, `_contains`, `and`/`or`, etc.) to Hasura equivalents, on root and nested collections
- **graph-node semantics**: Default page size and order, aliases, `operationName`, `_meta` block number and timestamp, `block: { number_gte }`

## API Endpoints

| Endpoint | Description |
|---|---|
| `POST /` | Convert and forward to HyperIndex (no chain filter) |
| `POST /chainId/{id}` | Convert and forward, auto-adds `chainId` filter (also to a `where: $var` value, and to single-entity lookups) |
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
# Page size for collections without `first` (graph-node's default); off/0 for unlimited
CONVERTER_DEFAULT_FIRST=100
# Set to off/false/0 to stop sorting collections without `orderBy` by `id` (see "Ordering")
CONVERTER_DEFAULT_ORDER_ID=on
# Set to on to add the converter's diagnostics to error responses (see "Errors")
CONVERTER_DEBUG_ERRORS=off
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
  Stream(limit: 2, offset: 10, order_by: {id: asc}) {
    category
    cliff
    chainId
  }
}
```

**HyperIndex output (at `/chainId/1`):**
```graphql
query {
  Stream(limit: 2, offset: 10, order_by: {id: asc}, where: { chainId: { _eq: "1" } }) {
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
| `field_contains: val` | `field: { _like: "%val%" }` |
| `field_starts_with: val` | `field: { _like: "val%" }` |
| `field_ends_with: val` | `field: { _like: "%val" }` |
| `field_contains_nocase: val` (and the other `_nocase` forms) | `field: { _ilike: "%val%" }` |
| `field_not_contains: val` (and the other `_not_` forms) | `_not: { field: { _like: "%val%" } }` |
| `ref_contains: val` (any string filter on an entity reference) | `ref_id: { _like: "%val%" }` |
| `list_contains: [...]` / `list_not_contains: [...]` | `list: { _contains: [...] }` / `_not: { list: { _contains: [...] } }` |
| `field: null` / `field_not: null` | `field: { _is_null: true }` / `field: { _is_null: false }` |
| `ref: null` / `ref_not: null` | `ref_id: { _is_null: true }` / `ref_id: { _is_null: false }` |
| `and: [{...}, {...}]` / `or: [...]` | `_and: [...]` / `_or: [...]` |

The plain string forms are case-sensitive, as in graph-node; only `_nocase` ignores case. Patterns are built exactly as graph-node builds them: `%` and `_` in a value are not escaped, so they act as wildcards, and `_contains` leaves a value that already starts or ends with `%` as it is.

A filter variable that is null for the request, sent as `null` or left out with no default, is read exactly like a literal `null`, as graph-node reads it: under `field: $v` and `field_not: $v` it becomes the null test above, and `$v` is dropped if nothing else uses it. A declared default (`$v: String = "x"`) is used when the variable is left out. Other operators reject null, as in graph-node: a string filter (`name_contains: null`) is refused, inline or in a `where` variable, and `_gt`, `_in` and the like are passed on as `null` for Hasura to reject.

A string filter or null test on an entity reference reads the reference's stored id, the `<field>_id` column, as graph-node does. Without that column it goes through the relation: `ref: { id: { _like: ... } }`, with the `_not_` forms negated inside it (`_nlike`, `_nilike`) so that rows with no reference are not matched, and a null test checks whether the related row exists.

On a list field, `_contains` is array containment (the list holds every given item), as in graph-node; the `_nocase`, `_starts_with` and `_ends_with` forms have no Hasura equivalent there and are refused, inline or in a `where` variable, as are `_containsAny` and `_containsAll`.

A string filter given a variable (`name_contains: $q`) reads a pattern variable, `$q__contains`, whose value is built from `$q`'s at request time; `$q` is dropped if nothing else uses it. Several conditions on one field (`amount_gt` and `amount_lt`) are all kept, inline or in a `where` variable, and each `_not_*` filter negates only its own condition.

Filters, `first`/`skip` and `orderBy`/`orderDirection` are converted the same way on nested collections (`user { swaps(first: 10, orderBy: timestamp, orderDirection: desc) { id } }`) as on root fields. Arguments may be separated by commas or by whitespace alone. As in graph-node, a nested list takes only those arguments; anything else, such as a nested `block:`, is rejected. `@include` and `@skip` are kept on root and nested fields alike.

A `where` variable's declared default (`$w: Token_filter = {symbol: "A"}`) is sent, translated, as the variable's value when the request leaves it out; Hasura could not read it in subgraph syntax. A non-null `where` variable left out is passed to Hasura as missing, which rejects the request, as graph-node does.

See the full filter table and known limitations in the source code.

## Interfaces and type-conditioned fragments

HyperIndex has no GraphQL interfaces, so an indexer that ports a subgraph `interface` models it as a concrete entity with one nullable link per implementing type (for example `UserTransaction` with `supply: Supply`, `borrow: Borrow`, ...). Hasura silently ignores a subgraph-style `... on Supply { amount }` against such an entity, so every row used to come back with only the shared fields.

The converter now handles this itself, with no per-interface configuration:

- `... on Supply { amount }` is rewritten to an aliased link field, `_on_Supply: supply { amount }`. The link is found from the introspected schema: the single object-valued field on the surrounding type whose type is `Supply`.
- Named fragments work the same way (`...SupplyFields` where `fragment SupplyFields on Supply` becomes `_on_Supply: supply { ...SupplyFields }`), as do fragments inside named fragments and inside nested selections such as `user { userTransactions { ... } }`.
- A fragment whose type condition is the surrounding type, or has none, is flattened into it.
- In the response, each non-null `_on_*` object is merged into its row and the key removed, so rows look exactly like the subgraph's: fragment fields present for the matching type and absent for the others.

A fragment is left untouched, as before, when the surrounding type is unknown, no field links to the fragment's type, or several do. A warning is logged. When a row matches a fragment and `__typename` is selected, it reports the implementing type (`Supply`), as the subgraph does; a row that matches no requested fragment keeps the entity's own name. Fragments on an interface a type *implements* (`supplies { ... on UserTransaction { id } }`) are not resolved, because the schema carries no interface information.

## `_meta`

`_meta { block { number timestamp } }` is answered from Hyperindex's indexing progress: Hasura's `_meta.progressBlock` and `progressBlockTime` (as unix seconds), or `chain_metadata.latest_processed_block` on indexers that predate Hasura's `_meta` (those cannot serve `timestamp`). `__typename` is answered locally. On `/chainId/{id}` the progress of that chain is reported; otherwise the lowest chain's, the block every chain has reached.

`deployment` is the deployment hash of `HYPERINDEX_URL`: the path segment before `/v1/graphql` (`b232c6d` in `https://indexer.hyperindex.xyz/b232c6d/v1/graphql`), or the subdomain of a dedicated host (`0xostium-7cfbfa6` in `https://0xostium-7cfbfa6.dedicated.hyperindex.xyz/v1/graphql`). It identifies the Hyperindex endpoint, not graph-node's IPFS id. Where a URL has no hash (`http://localhost:8080/v1/graphql`), `deployment` is rejected. The same value names the subgraph in the `number_gte` "only indexed up to block" error.

`_meta` can be aliased, repeated under different aliases, and combined with entity root fields in one operation.

`block.hash` and `hasIndexingErrors` have no Hyperindex source, so a selection containing any of them is rejected whole rather than answered partially: a response missing `hasIndexingErrors` would read as "no indexing errors".

## Block constraints

`block: { number_gte: N }` (on any root field, or on `_meta`) is answered from the latest state once every chain has indexed block N, and with graph-node's "only indexed up to block" error before then. Literal, `$n` and `$block: Block_height` forms work, with graph-node's reading of the value: a negative N is always met, a null or missing `$n` is an error, and a null `$block` means the latest block.

`block: { number: N }` and `block: { hash: ... }` ask for the state at a past block. Hyperindex does not keep historical state, so these are rejected rather than answered from the latest state. `subgraphError` is accepted and ignored: a Hyperindex indexer stops on a handler error, so there is no failed-but-serving state to choose.

## Errors

Errors take graph-node's shape:
- **A query that can't be run** gets HTTP 200 with `{"errors": [{"message": "..."}]}` and no `data`. That covers a converter rejection, an error from Hyperindex, a `number_gte` block that isn't indexed yet, and an unreachable indexer.
- **A request with no `query` string** gets HTTP 400 with `{"error": "GraphQL server error (client error): ..."}`.

Hyperindex's error messages are passed through, but its `extensions` are dropped. The messages keep Hasura's wording rather than graph-node's (for example, `field 'foo' not found in type: 'Token'` rather than ``Type `Token` has no field `foo` ``). The `number_gte` error uses graph-node's exact wording.

Full details, including the upstream errors, the converted query and the Hyperindex URL, are logged. Set `CONVERTER_DEBUG_ERRORS=on` to also return them in the response under `debug` and `subgraphResponse`, as before. The `/debug` endpoints are unchanged.

## Ordering

graph-node orders rows that tie on the sort key by `id`, in the same direction (`ORDER BY timestamp DESC, id DESC`). Hasura leaves ties unordered, so with `first`/`skip` a page boundary inside a run of equal keys could repeat or skip rows. Every explicit `orderBy` now also sorts by `id` in the same direction (`order_by: [{timestamp: desc}, {id: desc}]`), for both literal and variable `orderBy`. Sorting by `id` itself adds nothing. This gives a total, repeatable order, so paging is consistent.

Set `CONVERTER_ORDER_ID_TIEBREAK=off` to emit the sort key alone, for example if the extra sort column is slow on a very large table.

A collection with no `orderBy` is sorted by `id` ascending, and one with no `first` returns 100 rows, as graph-node does (a lone `orderDirection` is ignored, as there). This applies to root collections and to nested lists alike, so paging with `skip` and no `orderBy` is repeatable. Set `CONVERTER_DEFAULT_ORDER_ID=off` to leave such queries unordered, for example if sorting by `id` pushes the planner onto the primary key for a selective filter, and `CONVERTER_DEFAULT_FIRST` to change or remove (`off`) the page size. The same defaults apply when a `$first` or `$orderBy` variable is sent as null; a `$first` left out gets 100 too unless it is declared non-null or with a default.

One thing this does not do:

- Ties follow the database's string collation. graph-node compares `id` bytewise (C collation), while Postgres usually uses a locale collation that ignores punctuation, so ids that differ only around a `:` boundary (`1:` versus `15:`) can order differently inside a single block. The order is still stable and repeatable. It just may not match the subgraph's within-block order exactly.

## Known Limitations

- Uses string parsing rather than a full GraphQL parser
- Historical (time-travel) queries, `block: { number }` and `block: { hash }`, are rejected; see "Block constraints"
- `_meta` cannot serve `block.hash` or `hasIndexingErrors`, and `deployment` is the Hyperindex endpoint's hash rather than an IPFS id
- `first` above 1000 and `skip` above 5000 are passed through, where graph-node would reject them
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
