# TransactionIdIsCurrentTransactionId

## Location
[src/backend/access/transam/xact.c:938-1038](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L938-L1038)

## Overview
Determines whether a given transaction ID belongs to the current transaction context, including the main transaction and any subtransactions or parallel worker transactions.

## Definition
bool TransactionIdIsCurrentTransactionId(TransactionId xid)

## Detailed Description
This function checks if a specified transaction ID (XID) belongs to the current transaction context. It handles several complex scenarios:

1. **Special Transaction IDs**: Always returns false for BootstrapTransactionId, InvalidTransactionId, and FrozenTransactionId, as these are never considered "current"
2. **Top-level Transaction**: Checks if the XID matches the current top-level transaction
3. **Parallel Workers**: Uses a sorted array (ParallelCurrentXids) with binary search to efficiently check XIDs in parallel worker contexts
4. **Subtransactions**: Traverses the transaction state stack to check the current subtransaction, its parents, and their subcommitted children, using binary search on the sorted childXids arrays

The function is critical for PostgreSQL's MVCC (Multi-Version Concurrency Control) system, helping determine tuple visibility and transaction ownership throughout the system.

## Parameters / Member Variables
- : The TransactionId to check against the current transaction context

## Dependencies
- Functions called/Symbols referenced:
  - TransactionState (type)
  - TransactionIdIsNormal
  - TransactionIdEquals
  - [GetTopTransactionIdIfAny](../G/GetTopTransactionIdIfAny.md)
  - nParallelCurrentXids, ParallelCurrentXids (parallel processing globals)
  - CurrentTransactionState (global variable)
  - TRANS_ABORT (transaction state constant)
  - FullTransactionIdIsValid, XidFromFullTransactionId
  - [TransactionIdPrecedes](TransactionIdPrecedes.md)
- Called from (representative examples):
  - [heap_delete](../h/heap_delete.md), heap_update, heap_lock_tuple
  - [HeapTupleSatisfiesSelf](../H/HeapTupleSatisfiesSelf.md), HeapTupleSatisfiesUpdate, HeapTupleSatisfiesMVCC
  - [compute_new_xmax_infomask](../c/compute_new_xmax_infomask.md), test_lockmode_for_conflict
  - [FreezeMultiXactId](../F/FreezeMultiXactId.md), DoesMultiXactIdConflict
  - [heapam_tuple_lock](../h/heapam_tuple_lock.md), heapam_relation_copy_for_cluster
  - [ExecCheckTupleVisible](../E/ExecCheckTupleVisible.md), ExecOnConflictUpdate, ExecMergeMatched
  - [MultiXactIdIsRunning](../M/MultiXactIdIsRunning.md), TransactionIdIsInProgress

## Notes and Other Information
- Essential for PostgreSQL's MVCC implementation and tuple visibility determination
- Uses binary search algorithms for efficient lookups in sorted XID arrays
- Handles complex nested transaction hierarchies and parallel processing scenarios
- Special handling during bootstrap mode where all tuples are considered committed
- Skips aborted subtransactions when traversing the transaction state stack
- The childXids arrays are maintained in sorted order to enable binary search optimization
- Critical performance component as it's called frequently during tuple visibility checks and heap operations