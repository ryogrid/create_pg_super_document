# GetOldestActiveTransactionId

## Location
[src/backend/storage/ipc/procarray.c:2879-2943](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L2879-L2943)

## Overview
GetOldestActiveTransactionId returns the oldest currently active transaction ID across all databases, used primarily for checkpoint operations and transaction visibility management.

## Definition


## Detailed Description
This function provides a simplified version of GetSnapshotData that focuses solely on finding the oldest active transaction ID in the system. It examines all processes with assigned transaction IDs across all databases, including VACUUM processes, but excludes WAL sender processes since they don't affect hot standby conflicts.

The function uses a two-phase locking approach:
1. First acquires XidGenLock to read the next transaction ID as an upper bound
2. Then acquires ProcArrayLock to scan through all active processes

Unlike GetRunningTransactionData, this function doesn't collect subtransaction IDs since the top-level transaction ID is always smaller than any of its subtransactions. This optimization makes the function faster when only the oldest active XID is needed.

The function ensures atomicity by using proper locking and UINT32_ACCESS_ONCE for reading transaction IDs, preventing race conditions during concurrent transaction starts and commits.

## Parameters / Member Variables
This function takes no parameters and returns:
- : The oldest currently active transaction ID in the system

## Dependencies
- Functions called/Symbols referenced:
  - [RecoveryInProgress](../R/RecoveryInProgress.md)
  - LWLockAcquire/LWLockRelease
  - XidFromFullTransactionId
  - TransactionIdIsNormal
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md)
  - UINT32_ACCESS_ONCE
- Called from (representative examples):
  - [CreateCheckPoint](../C/CreateCheckPoint.md) (src/backend/access/transam/xlog.c:6932)

## Notes and Other Information
- Only executed during normal operation, never during recovery
- Does not include WAL sender processes in the analysis
- Optimized for performance by skipping subtransaction examination
- Uses two-phase locking to ensure consistency without holding locks longer than necessary
- Part of the checkpoint infrastructure for determining transaction visibility
- Does not update snapshot counters, keeping the implementation simple
- Assumes top-level XIDs are always smaller than their subtransaction XIDs