# MultiXactAdvanceNextMXact

## Location
[src/backend/access/transam/multixact.c:2503-2527](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/multixact.c#L2503-L2527)

## Overview
Ensures that the next-to-be-assigned MultiXactId and nextOffset values are advanced to at least the specified minimum values during XLog replay.

## Definition


## Detailed Description
This function is used during WAL (Write-Ahead Log) replay to ensure that the MultiXact system's next assignment counters are properly synchronized with the minimum safe values determined from XLog records. It can be called from checkpoint records or MultiXact creation log entries. The function takes an exclusive lock on MultiXactGenLock to protect the shared state even during hot-standby operations, where backends might be examining these values.

The function only advances the counters forward - it will not decrease them if the current values are already higher than the specified minimums. This ensures crash recovery consistency and prevents MultiXact ID conflicts.

## Parameters / Member Variables
- : The minimum MultiXactId value that nextMXact should be set to
- : The minimum MultiXactOffset value that nextOffset should be set to

## Dependencies
- Functions called/Symbols referenced:
  - LWLockAcquire (MultiXactGenLock, LW_EXCLUSIVE)
  - [MultiXactIdPrecedes](MultiXactIdPrecedes.md)
  - [MultiXactOffsetPrecedes](MultiXactOffsetPrecedes.md)  
  - debug_elog3
  - LWLockRelease
- Called from (representative examples):
  - [multixact_redo](../m/multixact_redo.md)
  - [xlog_redo](../x/xlog_redo.md)

## Notes and Other Information
- This function is specifically designed for XLog replay scenarios
- Uses exclusive locking to ensure thread safety during hot-standby operations
- Only advances counters forward, never backwards
- Includes debug logging to track when values are updated
- Critical for maintaining MultiXact consistency during crash recovery