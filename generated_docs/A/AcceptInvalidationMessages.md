# AcceptInvalidationMessages

## Location
[src/backend/utils/cache/inval.c:807-863](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/inval.c#L807-L863)

## Overview
Reads and processes invalidation messages from the shared invalidation message queue, typically called as the first step in transaction processing.

## Definition

```c
void
AcceptInvalidationMessages(void)
```
## Detailed Description
AcceptInvalidationMessages is a critical function that retrieves and processes invalidation messages from the shared invalidation queue to maintain cache consistency across PostgreSQL backends. The function serves as the main entry point for receiving invalidation messages from other backends that have modified shared data structures.

The function calls ReceiveSharedInvalidMessages with two callback functions:
1. LocalExecuteInvalidationMessage - processes individual invalidation messages
2. InvalidateSystemCaches - handles cases where the queue has overflowed

When the shared invalidation queue overflows (indicating that some invalidation messages were lost), the system performs a comprehensive cache flush to ensure consistency. The function also includes optional debug code that can force cache flushes for testing purposes when DISCARD_CACHES_ENABLED is defined.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - ReceiveSharedInvalidMessages
  - [LocalExecuteInvalidationMessage](../L/LocalExecuteInvalidationMessage.md)
  - [InvalidateSystemCaches](../I/InvalidateSystemCaches.md)
  - [InvalidateSystemCachesExtended](../I/InvalidateSystemCachesExtended.md) (in debug builds)
- Called from (representative examples):
  - [AtStart_Cache](AtStart_Cache.md)
  - [relation_openrv](../r/relation_openrv.md)
  - [RangeVarGetRelidExtended](../R/RangeVarGetRelidExtended.md)
  - [LockRelationOid](../L/LockRelationOid.md)
  - [ProcessCatchupInterrupt](../P/ProcessCatchupInterrupt.md)
  - [LogicalRepApplyLoop](../L/LogicalRepApplyLoop.md)

## Notes and Other Information
- Should be called as the first step in processing a transaction to ensure caches are up-to-date
- The function is called from many locking and relation access operations to ensure cache consistency before critical operations
- Debug builds can include aggressive cache flushing controlled by debug_discard_caches parameter
- The debug_discard_caches=1 setting makes the system extremely slow (100x slower) but helps detect cache-flush hazards
- The debug_discard_caches=3 setting provides recursive cache testing but is even slower (10000x factor)
- Critical for maintaining ACID properties and preventing stale cache reads in concurrent environments

## Simplified Source

```c
// Simplified version of AcceptInvalidationMessages
void AcceptInvalidationMessages(void) {
    // Read and process invalidation messages from shared queue
    // This ensures caches are consistent with recent changes by other backends
    ReceiveSharedInvalidMessages(LocalExecuteInvalidationMessage,
                                InvalidateSystemCaches);

    // Debug code: Force additional cache flushes for testing
    // (Only active when DISCARD_CACHES_ENABLED is compiled in)
    #ifdef DISCARD_CACHES_ENABLED
    if (recursion_depth < debug_discard_caches) {
        recursion_depth++;
        InvalidateSystemCachesExtended(true);  // Force comprehensive cache flush
        recursion_depth--;
    }
    #endif
}
```

Key simplifications made:
- Removed extensive debug comments explaining performance implications
- Simplified debug section to essential logic only
- Added high-level comments explaining the main purpose
- Consolidated debug logic into clear conditional block
- Preserved the core invalidation message processing flow