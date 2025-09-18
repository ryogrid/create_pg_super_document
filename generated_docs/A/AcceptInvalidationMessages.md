# AcceptInvalidationMessages

## Location
src/backend/utils/cache/inval.c: 807 - 863

## Overview
Reads and processes invalidation messages from the shared invalidation message queue, typically called as the first step in transaction processing.

## Definition


## Detailed Description
AcceptInvalidationMessages is a critical function that retrieves and processes invalidation messages from the shared invalidation queue to maintain cache consistency across PostgreSQL backends. The function serves as the main entry point for receiving invalidation messages from other backends that have modified shared data structures.

The function calls ReceiveSharedInvalidMessages with two callback functions:
1. LocalExecuteInvalidationMessage - processes individual invalidation messages
2. InvalidateSystemCaches - handles cases where the queue has overflowed

When the shared invalidation queue overflows (indicating that some invalidation messages were lost), the system performs a comprehensive cache flush to ensure consistency. The function also includes optional debug code that can force cache flushes for testing purposes when DISCARD_CACHES_ENABLED is defined.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - ReceiveSharedInvalidMessages
  - LocalExecuteInvalidationMessage
  - InvalidateSystemCaches
  - InvalidateSystemCachesExtended (in debug builds)
- Called from (representative examples):
  - AtStart_Cache
  - relation_openrv
  - RangeVarGetRelidExtended
  - LockRelationOid
  - ProcessCatchupInterrupt
  - LogicalRepApplyLoop

## Notes and Other Information
- Should be called as the first step in processing a transaction to ensure caches are up-to-date
- The function is called from many locking and relation access operations to ensure cache consistency before critical operations
- Debug builds can include aggressive cache flushing controlled by debug_discard_caches parameter
- The debug_discard_caches=1 setting makes the system extremely slow (100x slower) but helps detect cache-flush hazards
- The debug_discard_caches=3 setting provides recursive cache testing but is even slower (10000x factor)
- Critical for maintaining ACID properties and preventing stale cache reads in concurrent environments