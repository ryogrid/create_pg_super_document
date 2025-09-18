# ClearOldPredicateLocks

## Location
src/backend/storage/lmgr/predicate.c: 3687 - 3824

## Overview
Cleans up old predicate locks belonging to committed transactions that are no longer relevant to any in-progress transactions, managing the lifecycle of serializable transaction state.

## Definition
static void ClearOldPredicateLocks(void)

## Detailed Description
This function performs garbage collection of predicate locks and serializable transaction state in PostgreSQL's serializable snapshot isolation implementation. It operates in two main phases:

1. **Finished Transaction Cleanup**: Iterates through the list of finished serializable transactions in commit order. For each transaction, it determines if the transaction can be completely removed or partially cleaned based on whether any active transactions might still need to reference it. Transactions that committed before any currently active transaction took its snapshot can be completely removed.

2. **Predicate Lock Cleanup**: Cleans up predicate locks stored in the dummy OldCommittedSxact transaction that summarizes locks from old transactions. Locks from transactions old enough (based on commitSeqNo) can be safely removed.

The function uses several global tracking variables like SxactGlobalXmin, HavePartialClearedThrough, and CanPartialClearThrough to determine which transactions and locks can be safely cleaned up without affecting the correctness of conflict detection.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - LWLockAcquire
  - LWLockRelease 
  - dlist_foreach_modify
  - dlist_container
  - TransactionIdPrecedesOrEquals
  - dlist_delete_thoroughly
  - ReleaseOneSerializableXact
  - SxactIsReadOnly
  - PredicateLockTargetTagHashCode
  - PredicateLockHashPartitionLock
  - hash_search_with_hash_value
  - RemoveTargetIfNoLongerUsed
- Called from:
  - SerialControl
  - ReleasePredicateLocks

## Notes and Other Information
- This function is critical for preventing memory leaks in long-running systems with many serializable transactions
- The cleanup is done in a careful order to ensure no active transactions lose access to conflict information they might need
- Uses multiple lightweight locks (SerializableFinishedListLock, SerializableXactHashLock, SerializablePredicateListLock) to coordinate with concurrent operations
- Read-only transactions can be completely removed while read-write transactions may only be partially cleaned (keeping SERIALIZABLEXACT structure but removing locks and conflicts)
- Located at src/backend/storage/lmgr/predicate.c:3687