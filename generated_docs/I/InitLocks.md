# InitLocks

## Location
[src/backend/storage/lmgr/lock.c:392-473](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lock.c#L392-L473)

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
  - [ShmemInitStruct](../S/ShmemInitStruct.md) (shared memory structure initialization)  
  - SpinLockInit (spinlock initialization)
  - [hash_destroy](../h/hash_destroy.md) (local hash table destruction)
  - [hash_create](../h/hash_create.md) (local hash table creation)
  - [proclock_hash](../p/proclock_hash.md) (hash function for PROCLOCK table)
- Called from (representative examples):
  - [CreateOrAttachShmemStructs](../C/CreateOrAttachShmemStructs.md) (src/backend/storage/ipc/ipci.c:313)

## Notes and Other Information
- This function must be called during shared memory initialization
- [Hash](../H/Hash.md) table size calculations must agree with LockShmemSize() function
- The LOCALLOCK hash table is recreated each time to ensure it's clean in case of postmaster restart after backend crash
- Uses hash partitioning for improved concurrency across multiple lock partitions
- Fast-path structures are initialized for optimizing frequently used relation locks
- The function handles both normal postmaster and EXEC_BACKEND execution models

## Simplified Source

```c
// Simplified version of InitLocks
void InitLocks(void) {
    HASHCTL info;
    long init_table_size, max_table_size;
    bool found;

    // Calculate hash table sizes based on expected number of locks
    max_table_size = NLOCKENTS();
    init_table_size = max_table_size / 2;

    // Create shared hash table for LOCK structs (per-object lock info)
    info.keysize = sizeof(LOCKTAG);
    info.entrysize = sizeof(LOCK);
    info.num_partitions = NUM_LOCK_PARTITIONS;

    LockMethodLockHash = ShmemInitHash("LOCK hash",
                                       init_table_size, max_table_size,
                                       &info,
                                       HASH_ELEM | HASH_BLOBS | HASH_PARTITION);

    // Assume 2 holders per lock and adjust sizes
    max_table_size *= 2;
    init_table_size *= 2;

    // Create shared hash table for PROCLOCK structs (per-lock-per-holder info)
    info.keysize = sizeof(PROCLOCKTAG);
    info.entrysize = sizeof(PROCLOCK);
    info.hash = proclock_hash;
    info.num_partitions = NUM_LOCK_PARTITIONS;

    LockMethodProcLockHash = ShmemInitHash("PROCLOCK hash",
                                           init_table_size, max_table_size,
                                           &info,
                                           HASH_ELEM | HASH_FUNCTION | HASH_PARTITION);

    // Initialize fast-path lock structures for relation locks
    FastPathStrongRelationLocks =
        ShmemInitStruct("Fast Path Strong Relation Lock Data",
                        sizeof(FastPathStrongRelationLockData), &found);
    if (!found)
        SpinLockInit(&FastPathStrongRelationLocks->mutex);

    // Create local (per-backend) hash table for LOCALLOCK structs
    if (LockMethodLocalHash)
        hash_destroy(LockMethodLocalHash);

    info.keysize = sizeof(LOCALLOCKTAG);
    info.entrysize = sizeof(LOCALLOCK);

    LockMethodLocalHash = hash_create("LOCALLOCK hash", 16, &info,
                                      HASH_ELEM | HASH_BLOBS);
}
```

Key simplifications made:
- Removed detailed comments while preserving essential documentation
- Consolidated variable declarations at the top
- Simplified hash table creation parameters formatting for readability
- Maintained the core logic flow: size calculation → LOCK table → PROCLOCK table → fast-path → local table
- Preserved all essential initialization steps and error handling
- Focused on the main execution path without platform-specific details