# CreatePredicateLock

## Location
[src/backend/storage/lmgr/predicate.c:2443-2506](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L2443-L2506)

## Overview
Creates a predicate lock on a specified target for a given transaction, updating both the lock table and transaction's lock list.

## Definition
static void CreatePredicateLock(const PREDICATELOCKTARGETTAG *targettag, uint32 targettaghash, SERIALIZABLEXACT *sxact)

## Detailed Description
This function establishes a predicate lock relationship between a transaction and a target resource by updating the shared predicate lock hash tables. It creates the lock target if it doesn't exist and establishes the bidirectional link between the transaction and the target. The function handles all necessary locking protocols including partition locks and parallel mode considerations.

The function first acquires the appropriate locks (SerializablePredicateListLock, per-transaction lock in parallel mode, and partition lock), then ensures the target exists in the PredicateLockTargetHash. It creates a PREDICATELOCK entry that joins the transaction and target, maintaining doubly-linked lists in both directions for efficient traversal. The function includes robust error handling for out-of-memory conditions.

## Parameters / Member Variables
- : Pointer to the predicate lock target tag identifying the resource to be locked
- : Pre-computed hash value for the target tag for efficient hash table operations
- : Pointer to the serializable transaction that will hold the lock

## Dependencies
- Functions called/Symbols referenced:
  - PredicateLockHashPartitionLock
  - [LWLockAcquire](../L/LWLockAcquire.md), LWLockRelease
  - [IsInParallelMode](../I/IsInParallelMode.md)
  - [hash_search_with_hash_value](../h/hash_search_with_hash_value.md)
  - [dlist_init](../d/dlist_init.md), dlist_push_tail
  - PredicateLockHashCodeFromTargetHashCode
  - [PREDICATELOCKTARGET](../P/PREDICATELOCKTARGET.md), PREDICATELOCK, PREDICATELOCKTAG (structs)
  - InvalidSerCommitSeqNo (constant)
- Called from (representative examples):
  - [PredicateLockAcquire](../P/PredicateLockAcquire.md)
  - [predicatelock_twophase_recover](../p/predicatelock_twophase_recover.md)
  - [SerialControl](../S/SerialControl.md)

## Notes and Other Information
- This function only handles the core lock creation mechanics and does not deal with granularity promotion or local lock table management
- Includes comprehensive locking protocol with partition locks for concurrent access safety
- Handles parallel mode by acquiring per-transaction predicate list locks
- Provides detailed error messages suggesting max_pred_locks_per_transaction tuning when memory is exhausted
- Maintains bidirectional linked lists between targets and transactions for efficient lock management
- Sets commitSeqNo to InvalidSerCommitSeqNo for new locks