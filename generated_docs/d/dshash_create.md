# dshash_create

## Location
[src/backend/lib/dshash.c:206-269](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/dshash.c#L206-L269)

## Overview
Creates a new dynamic shared hash table backed by a specified dynamic shared area, initializing both the backend-local representation and the shared control structure.

## Definition

```c
structs. */
	hash_table->area = area;
```
## Detailed Description
The dshash_create function establishes a brand new dynamic shared hash table within the provided dynamic shared area. It performs comprehensive initialization including allocating both the backend-local dshash_table object and the shared dshash_table_control structure. The function sets up the initial bucket array with a size equal to the number of partitions and initializes all lock partitions with their associated LWLocks. The hash table starts with a minimal configuration that can grow dynamically as needed.

The function ensures proper memory management by using the dynamic shared area allocator for shared structures while using the current MemoryContext for backend-local allocations. Error handling includes cleanup of allocated shared memory if bucket allocation fails.

## Parameters / Member Variables
- : Dynamic shared area where the hash table's shared components will be allocated
- : Configuration parameters specifying hash function, comparison function, key size, entry size, and tranche ID for locks
- : User-provided context argument that will be passed to hash, compare, and copy functions

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md)
  - dsa_allocate
  - [dsa_get_address](dsa_get_address.md)  
  - [dsa_allocate_extended](dsa_allocate_extended.md)
  - [dsa_free](dsa_free.md)
  - [LWLockInitialize](../L/LWLockInitialize.md)
  - DsaPointerIsValid
  - ereport
- Called from (representative examples):
  - [logicalrep_launcher_attach_dshmem](../l/logicalrep_launcher_attach_dshmem.md)
  - [init_dsm_registry](../i/init_dsm_registry.md)
  - [StatsShmemInit](../S/StatsShmemInit.md)
  - [SharedRecordTypmodRegistryInit](../S/SharedRecordTypmodRegistryInit.md)

## Notes and Other Information
- The initial hash table size is set to DSHASH_NUM_PARTITIONS (16 buckets by default)
- Each partition gets its own LWLock initialized with the specified tranche ID
- The function reports an ERROR with ERRCODE_OUT_OF_MEMORY if bucket allocation fails
- The control structure is marked with DSHASH_MAGIC for validation purposes
- Memory allocation uses DSA_ALLOC_NO_OOM and DSA_ALLOC_ZERO flags for the bucket array

## Simplified Source

```c
// Simplified version of dshash_create
dshash_table *dshash_create(dsa_area *area, const dshash_parameters *params, void *arg) {
    dshash_table *hash_table;
    dsa_pointer control;

    // Core logic step 1: Allocate backend-local hash table object
    hash_table = palloc(sizeof(dshash_table));

    // Core logic step 2: Allocate shared control structure
    control = dsa_allocate(area, sizeof(dshash_table_control));

    // Core logic step 3: Set up local and shared hash table structures
    hash_table->area = area;
    hash_table->params = *params;
    hash_table->arg = arg;
    hash_table->control = dsa_get_address(area, control);
    hash_table->control->handle = control;
    hash_table->control->magic = DSHASH_MAGIC;
    hash_table->control->lwlock_tranche_id = params->tranche_id;

    // Core logic step 4: Initialize lock partitions
    dshash_partition *partitions = hash_table->control->partitions;
    for (int i = 0; i < DSHASH_NUM_PARTITIONS; ++i) {
        LWLockInitialize(&partitions[i].lock, params->tranche_id);
        partitions[i].count = 0;
    }

    // Core logic step 5: Set up initial bucket array
    hash_table->control->size_log2 = DSHASH_NUM_PARTITIONS_LOG2;
    hash_table->control->buckets = dsa_allocate_extended(area,
                                                         sizeof(dsa_pointer) * DSHASH_NUM_PARTITIONS,
                                                         DSA_ALLOC_NO_OOM | DSA_ALLOC_ZERO);

    // Handle allocation failure
    if (!DsaPointerIsValid(hash_table->control->buckets)) {
        dsa_free(area, control);
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
    }

    // Core logic step 6: Set up local bucket access
    hash_table->buckets = dsa_get_address(area, hash_table->control->buckets);
    hash_table->size_log2 = hash_table->control->size_log2;

    return hash_table;
}
```

Key simplifications made:
- Consolidated variable declarations where possible
- Removed detailed error message about DSA request size
- Simplified partition initialization loop
- Focused on the six main steps: allocate local, allocate shared, link structures, init partitions, create buckets, finalize
- Maintained essential error handling logic