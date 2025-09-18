# AdvanceOldestClogXid

## Location
src/backend/access/transam/varsup.c: 355 - 371

## Overview
Advances the cluster-wide value for the oldest valid commit log (CLOG) entry, providing a mechanism for transaction ID truncation coordination across the database cluster.

## Definition
```c
void AdvanceOldestClogXid(TransactionId oldest_datfrozenxid)
```

## Detailed Description
This function updates the global `oldestClogXid` value in `TransamVariables` to reflect the advancement of the oldest transaction ID that still needs to be preserved in the commit log. The function serves as a critical component in PostgreSQL's transaction ID wraparound prevention mechanism and CLOG cleanup process.

The function acquires an exclusive `XactTruncationLock` before modifying the global state to ensure atomicity when multiple processes might be attempting to advance the oldest CLOG entry. This lock coordination is essential because other code that looks up arbitrary transaction IDs must hold this lock from the time it tests `oldestClogXid` through completion of the CLOG lookup.

The function only advances the `oldestClogXid` forward - it will never move it backward, as verified by the `TransactionIdPrecedes` check.

## Parameters / Member Variables
- `oldest_datfrozenxid`: The new oldest transaction ID that should be preserved in the commit log, typically derived from the minimum `datfrozenxid` across all databases in the cluster

## Dependencies
- Functions called/Symbols referenced:
  - `LWLockAcquire` (XactTruncationLock, LW_EXCLUSIVE)
  - `TransactionIdPrecedes`
  - `LWLockRelease` (XactTruncationLock)
- Called from (representative examples):
  - `TruncateCLOG` (src/backend/access/transam/clog.c:1021)
  - `clog_redo` (src/backend/access/transam/clog.c:1137)
  - `BootStrapXLOG` (src/backend/access/transam/xlog.c:5055)
  - `StartupXLOG` (src/backend/access/transam/xlog.c:5531)

## Notes and Other Information
- This function is part of PostgreSQL's transaction wraparound prevention system
- The `XactTruncationLock` must be held exclusively to ensure safe coordination between CLOG truncation and transaction lookup operations
- The function only moves `oldestClogXid` forward, never backward, maintaining the monotonic property required for safe CLOG management
- Called during recovery and normal CLOG maintenance operations to keep the oldest CLOG boundary up to date