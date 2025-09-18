# ReleaseOneSerializableXact

## Location
src/backend/storage/lmgr/predicate.c: 3825 - 3961

## Overview
Releases and cleans up resources associated with a serializable transaction, including predicate locks, conflicts, and transaction records, with options for partial cleanup or summarization.

## Definition
static void ReleaseOneSerializableXact(SERIALIZABLEXACT *sxact, bool partial, bool summarize)

## Detailed Description
This function is the primary mechanism for cleaning up serializable transaction state in PostgreSQL's serializable snapshot isolation implementation. It operates in several phases:

1. **Predicate Lock Cleanup**: Removes all predicate locks held by the transaction. If summarizing, transfers these locks to the OldCommittedSxact dummy transaction with appropriate commit sequence number handling for duplicate consolidation.

2. **Conflict Cleanup**: Releases all read-write conflicts associated with the transaction. For outConflicts, this is skipped when partial=true. When summarizing, it sets summary conflict flags on related transactions.

3. **Transaction Record Cleanup**: Unless partial=true, removes the transaction ID from SerializableXidHash and releases the SERIALIZABLEXACT structure itself.

The function supports three modes:
- **Full cleanup** (partial=false, summarize=false): Complete removal of transaction and all associated state
- **Partial cleanup** (partial=true): Keeps transaction structure and outConflicts but removes locks and inConflicts  
- **Summarizing cleanup** (summarize=true): Transfers predicate locks to summary transaction and marks conflicts appropriately

## Parameters / Member Variables
- : Pointer to the SERIALIZABLEXACT structure to be released
- : When true, keeps the transaction entry and outConflicts but releases locks and inConflicts
- : When true, transfers predicate locks to OldCommittedSxact for space management

## Dependencies
- Functions called/Symbols referenced:
  - SxactIsRolledBack
  - SxactIsCommitted
  - SxactIsOnFinishedList
  - LWLockHeldByMe
  - LWLockAcquire
  - LWLockRelease
  - IsInParallelMode
  - dlist_foreach_modify
  - dlist_container
  - PredicateLockTargetTagHashCode
  - PredicateLockHashPartitionLock
  - [hash_search_with_hash_value](../h/hash_search_with_hash_value.md)
  - [dlist_delete](../d/dlist_delete.md)
  - [dlist_push_tail](../d/dlist_push_tail.md)
  - [RemoveTargetIfNoLongerUsed](RemoveTargetIfNoLongerUsed.md)
  - [ReleaseRWConflict](ReleaseRWConflict.md)
  - [ReleasePredXact](ReleasePredXact.md)
- Called from:
  - [SerialControl](../S/SerialControl.md)
  - SummarizeOldestCommittedSxact
  - [ReleasePredicateLocks](ReleasePredicateLocks.md)
  - [ClearOldPredicateLocks](../C/ClearOldPredicateLocks.md)

## Notes and Other Information
- Must be called with SerializableFinishedListLock held
- Handles parallel query execution by acquiring per-transaction predicate list locks when needed
- The summarize functionality is crucial for preventing memory exhaustion in systems with many old committed transactions
- When summarizing, duplicate predicate locks on the same target are consolidated by keeping the latest commitSeqNo
- Error handling includes out-of-memory conditions when creating summary predicate locks
- Located at src/backend/storage/lmgr/predicate.c:3825