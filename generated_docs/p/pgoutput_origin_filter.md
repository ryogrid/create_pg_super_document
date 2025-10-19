# pgoutput_origin_filter

## Location
[src/backend/replication/pgoutput/pgoutput.c:1711-1729](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/pgoutput/pgoutput.c#L1711-L1729)

## Overview
Determines whether to filter out changes based on their replication origin, implementing origin-based filtering for logical replication.

## Definition

```c
static bool
pgoutput_origin_filter(LogicalDecodingContext *ctx,
					   RepOriginId origin_id)
```
## Detailed Description
The  function implements origin-based filtering in PostgreSQL's logical replication system. This function helps prevent replication loops and allows selective replication based on the origin of changes.

The function works by checking the publication configuration and the origin ID of incoming changes:
1. **Origin Filtering Logic**: Returns true if the change should be filtered out (excluded from replication)
2. **No-Origin Publishing**: When  is enabled, changes that have a replication origin are filtered out
3. **Loop Prevention**: Helps prevent infinite replication loops in multi-master or cascading replication scenarios

The filtering decision is based on whether the publication is configured to publish only changes that don't have a replication origin ( setting).

## Parameters / Member Variables
- `*ctx`: LogicalDecodingContext containing output plugin state and configuration
- `origin_id`: RepOriginId representing the replication origin associated with the change (InvalidRepOriginId for local changes)
## Dependencies
- Functions called/Symbols referenced:
  - InvalidRepOriginId (constant for comparison)
- Called from (representative examples):
  - [_PG_output_plugin_init](../P/_PG_output_plugin_init.md) (as callback registration)

## Notes and Other Information
- Returns true when changes should be filtered out (not replicated)
- The  setting controls whether to replicate only local changes (those without a replication origin)
- Critical for preventing replication loops in complex replication topologies
- Changes with  are considered local changes (not replicated from elsewhere)
- This filtering mechanism is essential for bidirectional and multi-master replication setups
- The function is called by the logical decoding framework for each change to determine if it should be processed
- Simple but crucial function for maintaining replication topology integrity

## Simplified Source

```c
static bool pgoutput_origin_filter(LogicalDecodingContext *ctx, RepOriginId origin_id)
{
    PGOutputData *data = (PGOutputData *) ctx->output_plugin_private;

    // Filter out changes with origin when publish_no_origin is enabled
    if (data->publish_no_origin && origin_id != InvalidRepOriginId)
        return true;  // Filter out (exclude from replication)

    return false;     // Don't filter (include in replication)
}
```