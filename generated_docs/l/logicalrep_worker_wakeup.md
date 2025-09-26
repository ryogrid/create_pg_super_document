# logicalrep_worker_wakeup

## Location
src/backend/replication/logical/launcher.c: 689 - 708

## Overview
Wakes up a logical replication worker for a specified subscription and relation pair using its latch mechanism.

## Definition

```c
void
logicalrep_worker_wakeup(Oid subid, Oid relid)
```
## Detailed Description
This function provides a public interface for waking up logical replication workers that may be blocked waiting for work or events. It locates the appropriate worker using subscription and relation identifiers, then uses the worker's latch to signal that it should resume processing. This is commonly used to notify workers of new data availability, configuration changes, or other events that require immediate attention.

The function operates under LogicalRepWorkerLock protection to ensure thread-safe access to the worker pool during lookup and wakeup operations.

## Parameters / Member Variables
- : Object ID of the subscription whose worker should be awakened
- : Object ID of the relation being replicated (InvalidOid for subscription-level workers)

## Dependencies
- Functions called/Symbols referenced:
  - LWLockAcquire/LWLockRelease: Acquires LogicalRepWorkerLock for thread-safe worker access
  - logicalrep_worker_find: Locates the worker matching the subscription/relation pair (with include_stopping=true)
  - logicalrep_worker_wakeup_ptr: Performs the actual latch-based worker wakeup

- Called from:
  - pg_attribute_noreturn: Used in table synchronization context for worker coordination
  - apply_handle_stream_start: Wakes workers when new replication streams begin

## Notes and Other Information
- Safe to call even if no worker exists for the given subscription/relation pair
- Uses include_stopping=true in worker lookup to wake even workers that are shutting down
- Provides asynchronous notification mechanism for worker coordination
- Essential for responsive logical replication by minimizing worker idle time
- Thread-safe through proper LWLock usage during the entire operation