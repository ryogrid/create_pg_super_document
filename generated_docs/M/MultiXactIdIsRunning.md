# MultiXactIdIsRunning

## Location
[src/backend/access/transam/multixact.c:598-671](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/multixact.c#L598-L671)

## Overview
MultiXactIdIsRunning determines whether a MultiXactId contains at least one member transaction that is still running.

## Definition
bool MultiXactIdIsRunning(MultiXactId multi, bool isLockOnly)

## Detailed Description
This function checks whether a given MultiXactId is considered "running" by examining if any of its member transactions are still active. The function returns true if at least one member transaction is still running, and false if all members have completed (committed or aborted).

The function implements a two-phase checking strategy:
1. Fast path: First checks if any member belongs to the current transaction (including subtransactions), which is a cheap local check
2. General case: For each remaining member, queries the process array to determine if the transaction is still in progress

A "false" result is guaranteed to remain stable because PostgreSQL does not allow adding new members to existing MultiXactIds. This makes the result reliable for decision-making in calling code.

The function is used primarily in heap tuple visibility checks and transaction conflict resolution to determine if a MultiXactId represents ongoing transaction activity.

## Parameters / Member Variables
- : The MultiXactId to check for running status
- : Flag indicating whether to consider only lock-only operations when retrieving members

## Dependencies
- Functions called/Symbols referenced:
  - [GetMultiXactIdMembers](../G/GetMultiXactIdMembers.md)
  - [TransactionIdIsCurrentTransactionId](../T/TransactionIdIsCurrentTransactionId.md)  
  - [TransactionIdIsInProgress](../T/TransactionIdIsInProgress.md)
  - debug_elog3, debug_elog2, debug_elog4
- Called from (representative examples):
  - [compute_new_xmax_infomask](../c/compute_new_xmax_infomask.md) (src/backend/access/heap/heapam.c:5375)
  - [FreezeMultiXactId](../F/FreezeMultiXactId.md) (src/backend/access/heap/heapam.c:6701)
  - [HeapTupleSatisfiesUpdate](../H/HeapTupleSatisfiesUpdate.md) (multiple locations in src/backend/access/heap/heapam_visibility.c)
  - [HeapTupleSatisfiesVacuumHorizon](../H/HeapTupleSatisfiesVacuumHorizon.md) (src/backend/access/heap/heapam_visibility.c:1323, 1371)

## Notes and Other Information
- The caller is expected to verify that the MultiXactId does not come from a pg_upgraded share-locked tuple
- Performance optimization: checks current transaction membership first as a fast path before expensive shared memory lookups
- Could be optimized further by walking the PGPROC array only once for all members, but current implementation assumes nmembers is typically small
- Critical for heap tuple visibility determination and vacuum operations
- [Result](../R/Result.md) stability guarantee: a false result will never change to true since no new members can be added to existing MultiXactIds