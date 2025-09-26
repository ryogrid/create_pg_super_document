# GetPredicateLockStatusData

## Location
[src/backend/storage/lmgr/predicate.c:1435-1492](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L1435-L1492)

## Overview
Returns internal state of the predicate lock manager as a structured table for use in pg_lock_status system view.

## Definition
PredicateLockData *GetPredicateLockStatusData(void)

## Detailed Description
This function provides a snapshot of the current state of PostgreSQL's serializable snapshot isolation predicate lock manager. It extracts and returns information about all active predicate locks in a format suitable for display in the pg_lock_status system view.

The function implements a careful locking protocol to ensure consistency:
1. Acquires all predicate lock partition locks simultaneously in ascending order
2. Acquires the SerializableXactHashLock
3. Scans the PredicateLockHash table to collect lock information
4. Releases locks in reverse order

The returned data structure contains arrays of PREDICATELOCKTARGETTAG and SERIALIZABLEXACT entries, where each predicate lock is represented by its target and associated transaction. Multiple copies of the same target or transaction may appear since multiple locks can reference the same objects.

## Parameters / Member Variables
- Returns: PredicateLockData* containing:
  - : Number of predicate locks found
  - : Array of PREDICATELOCKTARGETTAG structures
  - : Array of SERIALIZABLEXACT structures

## Dependencies
- Functions called/Symbols referenced:
  - PredicateLockData
  - HASH_SEQ_STATUS
  - PREDICATELOCK
  - NUM_PREDICATELOCK_PARTITIONS
  - PredicateLockHashPartitionLockByIndex
  - LW_SHARED
  - hash_get_num_entries
  - hash_seq_init
  - hash_seq_search
  - PREDICATELOCKTARGETTAG
  - SERIALIZABLEXACT
- Called from (representative examples):
  - pg_lock_status (system view function)
  - InvalidSerializableXact

## Notes and Other Information
- Function holds multiple locks simultaneously to ensure consistency of the snapshot
- Similar in design to GetLockStatusData for regular locks
- Memory for the returned structure and arrays is allocated using palloc
- The locking order (ascending for acquisition, descending for release) prevents deadlocks
- Part of PostgreSQL's Serializable Snapshot Isolation (SSI) monitoring infrastructure
- Located in src/backend/storage/lmgr/predicate.c:1435-1492