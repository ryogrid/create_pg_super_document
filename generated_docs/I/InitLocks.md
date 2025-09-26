# InitLocks

## Location
src/backend/storage/lmgr/lock.c: 392 - 473

## Overview
InitLocks initializes the PostgreSQL lock manager's data structures, including shared memory hash tables for locks and proclocks, as well as per-backend local lock hash tables.

## Definition

```c
structs.  This stores per-locked-object
	 * information.
	 */
	info.keysize = sizeof(LOCKTAG);
```
## Detailed Description
InitLocks is responsible for setting up the core data structures used by PostgreSQL's lock manager. It creates three main hash tables:

1. **LOCK hash table** - Stores per-locked-object information in shared memory, keyed by LOCKTAG
2. **PROCLOCK hash table** - Stores per-lock-per-holder information in shared memory, keyed by PROCLOCKTAG  
3. **LOCALLOCK hash table** - Stores lock counts and resource owner information locally per backend, keyed by LOCALLOCKTAG

The function also initializes the fast-path lock mechanism data structures for optimizing relation lock acquisition. In the normal postmaster case, shared hash tables are created here and inherited by backends via fork(). In EXEC_BACKEND case, each backend re-executes this code to obtain pointers to existing shared tables.

The hash table sizes are calculated based on NLOCKENTS() with partitioning across NUM_LOCK_PARTITIONS for concurrency. The PROCLOCK table assumes an average of 2 holders per lock when sizing.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - NLOCKENTS (macro for calculating lock table size)
  - ShmemInitHash (shared memory hash table initialization)
  - ShmemInitStruct (shared memory structure initialization)  
  - SpinLockInit (spinlock initialization)
  - hash_destroy (local hash table destruction)
  - hash_create (local hash table creation)
  - proclock_hash (hash function for PROCLOCK table)
- Called from (representative examples):
  - CreateOrAttachShmemStructs (src/backend/storage/ipc/ipci.c:313)

## Notes and Other Information
- This function must be called during shared memory initialization
- Hash table size calculations must agree with LockShmemSize() function
- The LOCALLOCK hash table is recreated each time to ensure it's clean in case of postmaster restart after backend crash
- Uses hash partitioning for improved concurrency across multiple lock partitions
- Fast-path structures are initialized for optimizing frequently used relation locks
- The function handles both normal postmaster and EXEC_BACKEND execution models