# pgstat_drop_entry_internal

## Location
[src/backend/utils/activity/pgstat_shmem.c:826-865](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_shmem.c#L826-L865)

## Overview
Internal helper function that handles the dropping of shared statistics entries, managing reference counting and coordinating entry deletion across multiple backends.

## Definition

```c
static bool
pgstat_drop_entry_internal(PgStatShared_HashEntry *shent,
						   dshash_seq_status *hstat)
```
## Detailed Description
This function serves as the core implementation for dropping shared statistics entries in PostgreSQL's statistics system. It handles the complex process of safely removing entries from the shared hash table while coordinating with other backends that may still hold references to the entry.

The function implements a two-phase deletion strategy: first marking the entry as dropped to signal other backends to release their references, then checking if the reference count reaches zero to determine if immediate deletion is possible. If other backends still hold references, the entry remains in the hash table marked as dropped until all references are released.

The function includes comprehensive safety checks, including verification that local references have been released and protection against double-deletion attempts. It supports both direct deletion and iteration-safe deletion patterns through the optional  parameter.

## Parameters / Member Variables
- `*shent`: Pointer to the shared statistics hash entry to be dropped, which must be already locked by the caller
- `*hstat`: Optional pointer to sequential iteration status. If non-NULL, indicates the function is called during hash table iteration and affects the deletion method used
## Dependencies
- Functions called/Symbols referenced:
  - : Verifies no local references exist before dropping
  - : Retrieves kind information for error reporting
  - : Atomically reads 32-bit values for reference count and generation
  - : Atomically decrements and returns the reference count
  - : Frees the entry if reference count reaches zero
  - : Releases the hash entry lock when not immediately freed

- Called from (representative examples):
  - : Called during database drop operations
  - : Called for explicit single entry drops
  - : Called during bulk entry removal operations

## Notes and Other Information
- This is a static function used internally within the statistics shared memory module
- Returns  if the entry was immediately freed,  if it remains marked as dropped
- The function enforces that local references must be released before calling this function
- Includes comprehensive error checking to prevent double-deletion attempts
- Uses atomic operations for thread-safe reference counting across multiple backends
- The  flag serves as a signal for other backends to release their references
- When reference count reaches zero, the entry is immediately freed and removed
- If references remain, the entry stays in the hash table but marked as dropped for eventual cleanup
- Critical component of PostgreSQL's cooperative statistics entry lifecycle management
- Supports both direct hash deletion and iteration-safe deletion patterns

## Simplified Source

```c
static bool
pgstat_drop_entry_internal(PgStatShared_HashEntry *shent,
                           dshash_seq_status *hstat)
{
    Assert(shent->body != InvalidDsaPointer);

    // Verify local references have been released
    if (pgStatEntryRefHash)
        Assert(!pgstat_entry_ref_hash_lookup(pgStatEntryRefHash, shent->key));

    // Check for double-deletion attempts
    if (shent->dropped)
        elog(ERROR,
             "trying to drop stats entry already dropped: kind=%s dboid=%u objoid=%u refcount=%u generation=%u",
             pgstat_get_kind_info(shent->key.kind)->name,
             shent->key.dboid, shent->key.objoid,
             pg_atomic_read_u32(&shent->refcount),
             pg_atomic_read_u32(&shent->generation));

    // Mark entry as dropped to signal other backends
    shent->dropped = true;

    // Decrement reference count and check if we can free immediately
    if (pg_atomic_sub_fetch_u32(&shent->refcount, 1) == 0)
    {
        // No more references - safe to free the entry
        pgstat_free_entry(shent, hstat);
        return true;
    }
    else
    {
        // Other backends still hold references - just release lock
        if (!hstat)
            dshash_release_lock(pgStatLocal.shared_hash, shent);
        return false;
    }
}
```