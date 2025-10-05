# pgoutput_shutdown

## Location
[src/backend/replication/pgoutput/pgoutput.c:1730-1745](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/pgoutput/pgoutput.c#L1730-L1745)

## Overview
Performs cleanup operations when shutting down the pgoutput logical replication output plugin.

## Definition

```c
static void
pgoutput_shutdown(LogicalDecodingContext *ctx)
```
## Detailed Description
The  function handles the cleanup and shutdown process for PostgreSQL's pgoutput logical replication output plugin. This function is called when the logical decoding session is ending, either due to normal termination or error conditions.

Key cleanup operations include:
1. **Cache Cleanup**: Destroys the RelationSyncCache hash table that stores relation synchronization state
2. **Resource Deallocation**: Ensures proper cleanup of plugin-specific resources
3. **Safety Measures**: Nullifies global pointers to prevent dangling references

The function is designed to be safe to call multiple times and handles cases where resources may already be cleaned up. Most memory contexts are automatically cleaned up by the logical decoding machinery, so only specific plugin resources need manual cleanup.

## Parameters / Member Variables
- `*ctx`: LogicalDecodingContext containing the logical decoding session state (used for context but not directly accessed in cleanup)
## Dependencies
- Functions called/Symbols referenced:
  - [hash_destroy](../h/hash_destroy.md) (for cleaning up RelationSyncCache)
  - RelationSyncCache (global variable)
  - pubctx (global variable)
- Called from (representative examples):
  - [_PG_output_plugin_init](../P/_PG_output_plugin_init.md) (as callback registration)

## Notes and Other Information
- Memory contexts like data->context, data->cachectx, and pubctx are automatically cleaned up by the logical decoding framework
- The RelationSyncCache is a hash table that must be explicitly destroyed to prevent memory leaks
- Setting global pointers to NULL helps prevent use-after-free bugs
- This function is part of the standard logical decoding plugin lifecycle
- Called automatically by the logical decoding infrastructure during shutdown
- Safe to call multiple times due to NULL checks on global resources
- Essential for preventing resource leaks in long-running replication sessions

## Simplified Source

```c
static void
pgoutput_shutdown(LogicalDecodingContext *ctx)
{
    // Clean up the relation synchronization cache
    if (RelationSyncCache)
    {
        hash_destroy(RelationSyncCache);
        RelationSyncCache = NULL;
    }

    // Clear global context pointer for safety
    pubctx = NULL;
}
```