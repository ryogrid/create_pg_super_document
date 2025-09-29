# pgstat_setup_shared_refs

## Location
[src/backend/utils/activity/pgstat_shmem.c:326-341](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_shmem.c#L326-L341)

## Overview
Initializes the local hash table for caching references to shared memory statistics entries, setting up the infrastructure needed for efficient entry reference management.

## Definition

```c
static void
pgstat_setup_shared_refs(void)
```
## Detailed Description
The `pgstat_setup_shared_refs` function performs one-time initialization of the local entry reference hash table used for caching shared memory statistics entry references. This setup is crucial for performance optimization in PostgreSQL's statistics collection system.

The function uses a likely() optimization hint to quickly return if the hash table has already been initialized, avoiding redundant setup calls. When initialization is needed, it:

1. **Hash table creation**: Creates a new hash table using `pgstat_entry_ref_hash_create` with a predefined size
2. **Context assignment**: Uses the `pgStatEntryRefHashContext` memory context for allocation
3. **Generation tracking**: Records the current garbage collection request count as a baseline for cache invalidation decisions

The initialization is designed to be called multiple times safely, with the likely() branch optimization ensuring minimal overhead on subsequent calls.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - likely (compiler optimization hint)
  - pgstat_entry_ref_hash_create
  - [pg_atomic_read_u64](pg_atomic_read_u64.md)
- Called from (representative examples):
  - [pgstat_get_entry_ref](pgstat_get_entry_ref.md)

## Notes and Other Information
- This is a static function, only accessible within the pgstat_shmem.c module
- Uses likely() compiler hint for branch prediction optimization, assuming the hash table is usually already initialized
- The PGSTAT_ENTRY_REF_HASH_SIZE constant determines the initial size of the reference hash table
- The `pgStatSharedRefAge` is set from the garbage collection request count to track when the cache might need refreshing
- Essential for the local caching mechanism that improves performance when repeatedly accessing the same statistics entries
- The Assert ensures that the garbage collection counter has been properly initialized (non-zero)

## Simplified Source

```c
static void
pgstat_setup_shared_refs(void)
{
    // Quick return if hash table already exists (common case)
    if (likely(pgStatEntryRefHash != NULL))
        return;

    // Create the entry reference hash table
    pgStatEntryRefHash =
        pgstat_entry_ref_hash_create(pgStatEntryRefHashContext,
                                    PGSTAT_ENTRY_REF_HASH_SIZE, NULL);

    // Record current GC request count as baseline for cache invalidation
    pgStatSharedRefAge = pg_atomic_read_u64(&pgStatLocal.shmem->gc_request_count);
    Assert(pgStatSharedRefAge != 0);
}
```