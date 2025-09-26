# asyncQueueAdvanceTail

## Location
[src/backend/commands/async.c:2108-2182](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/async.c#L2108-L2182)

## Overview
Advances the shared notification queue tail to the minimum position among all backend processes and truncates old notification data when possible.

## Definition
```c
static void asyncQueueAdvanceTail(void)
```

## Detailed Description
This function performs critical queue maintenance by computing the minimum queue position across all listening backend processes and updating the global queue tail accordingly. It implements a two-phase approach: first updating the logical tail (QUEUE_TAIL) atomically, then performing physical truncation of old SLRU segments when appropriate.

The function operates under exclusive locking to ensure consistency, but releases locks strategically to avoid holding them during potentially expensive SimpleLruTruncate operations. It only truncates data when the tail advances across SLRU segment boundaries, optimizing for both safety and performance.

## Parameters / Member Variables
This function takes no parameters and operates on global shared memory structures.

## Dependencies
- Functions called/Symbols referenced:
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease: Manages exclusive access to queue structures
  - QUEUE_HEAD: Gets the current queue head position  
  - QUEUE_FIRST_LISTENER/QUEUE_NEXT_LISTENER: Iterates through listening backends
  - QUEUE_BACKEND_PID/QUEUE_BACKEND_POS: Accesses per-backend queue state
  - QUEUE_POS_MIN: Finds minimum queue position
  - QUEUE_POS_PAGE: Extracts page number from queue position
  - [asyncQueuePagePrecedes](asyncQueuePagePrecedes.md): Compares page positions for ordering
  - [SimpleLruTruncate](../S/SimpleLruTruncate.md): Physically removes old SLRU segments
- Called from:
  - [AtCommit_Notify](../A/AtCommit_Notify.md): Transaction commit processing
  - [pg_notification_queue_usage](../p/pg_notification_queue_usage.md): Queue usage reporting

## Notes and Other Information
- Called during CommitTransaction(), requiring very low probability of failure
- Maintains separation between logical tail (QUEUE_TAIL) and physical tail (QUEUE_STOP_PAGE)  
- Only truncates when tail crosses SLRU segment boundaries to minimize directory scans
- Uses NotifyQueueTailLock to restrict operation to one backend per cluster
- Pre-v13 required exact QUEUE_TAIL positioning, maintained for prudence in current versions
- Strategic lock release prevents holding locks during expensive truncation operations