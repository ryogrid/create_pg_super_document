# replorigin_get_progress

## Location
src/backend/replication/logical/origin.c: 1014 - 1054

## Overview
Retrieves the current replication progress (remote LSN) for a specified replication origin, with optional WAL flush functionality.

## Definition


## Detailed Description
replorigin_get_progress queries the current replication progress for a given replication origin by searching through the replication_states array. It returns the remote LSN that represents how far replication has progressed from the remote node. The function uses shared locks to safely read the state without blocking concurrent operations. If the flush parameter is true and a valid local LSN exists, it ensures that the local WAL is flushed up to that point, providing durability guarantees for the progress information.

## Parameters / Member Variables
- : RepOriginId identifying the replication origin to query
- : boolean indicating whether to flush WAL up to the local LSN

## Dependencies
- Functions called/Symbols referenced:
  - LWLockAcquire/LWLockRelease
  - [XLogFlush](../X/XLogFlush.md)
  - RepOriginId
  - ReplicationState (struct)
  - LW_SHARED
  - InvalidXLogRecPtr
- Called from (representative examples):
  - [AlterSubscription](../A/AlterSubscription.md) (src/backend/commands/subscriptioncmds.c:1467)
  - [pg_replication_origin_progress](../p/pg_replication_origin_progress.md) (src/backend/replication/logical/origin.c:1506)

## Notes and Other Information
- Returns InvalidXLogRecPtr if the specified replication origin is not found
- Uses shared locks for concurrent read access, allowing multiple readers
- The flush parameter provides a way to ensure durability of the progress information
- Essential for monitoring replication lag and ensuring consistency in logical replication
- The function is safe to call concurrently with replorigin_advance operations
- Commonly used by subscription management and monitoring tools to track replication progress