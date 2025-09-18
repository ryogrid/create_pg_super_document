# PredicateLockAcquire

## Location
src/backend/storage/lmgr/predicate.c: 2507 - 2565

## Overview
Acquires a predicate lock on a specified target for the current connection, handling granularity promotion and local lock table management.

## Definition
static void PredicateLockAcquire(const PREDICATELOCKTARGETTAG *targettag)

## Detailed Description
This function is the main entry point for acquiring predicate locks in PostgreSQL's serializable snapshot isolation implementation. It implements a sophisticated locking strategy that includes checking for existing locks, evaluating lock coverage by coarser granularity locks, and managing both local and shared lock tables.

The function first checks if the lock already exists or is covered by a coarser lock, returning early if so. If a new lock is needed, it updates the local lock table and calls CreatePredicateLock to establish the shared lock. After acquisition, it attempts lock promotion to coarser granularity and cleans up any finer-granularity locks that are now redundant. This approach optimizes lock granularity to balance concurrency with lock table space usage.

## Parameters / Member Variables
- : Pointer to the predicate lock target tag identifying the specific resource to be locked

## Dependencies
- Functions called/Symbols referenced:
  - PredicateLockExists
  - CoarserLockCovers
  - PredicateLockTargetTagHashCode
  - [hash_search_with_hash_value](../h/hash_search_with_hash_value.md)
  - [CreatePredicateLock](../C/CreatePredicateLock.md)
  - [CheckAndPromotePredicateLockRequest](../C/CheckAndPromotePredicateLockRequest.md)
  - [DeleteChildTargetLocks](../D/DeleteChildTargetLocks.md)
  - GET_PREDICATELOCKTARGETTAG_TYPE
  - LOCALPREDICATELOCK (struct)
  - PREDLOCKTAG_TUPLE (constant)
  - MySerializableXact (global variable)
- Called from (representative examples):
  - [PredicateLockRelation](PredicateLockRelation.md)
  - [PredicateLockPage](PredicateLockPage.md)
  - [PredicateLockTID](PredicateLockTID.md)
  - [CheckAndPromotePredicateLockRequest](../C/CheckAndPromotePredicateLockRequest.md)
  - [SerialControl](../S/SerialControl.md)

## Notes and Other Information
- Central function in PostgreSQL's predicate locking mechanism for serializable transactions
- Implements granularity promotion where multiple fine-grained locks can be consolidated into coarser ones
- Uses both local and shared lock tables: local for quick checks and shared for cross-transaction visibility
- Automatically cleans up redundant finer-granularity locks except for tuple-level locks
- The function is idempotent - calling it multiple times with the same target has no additional effect
- Critical for preventing serialization anomalies in SERIALIZABLE isolation level transactions