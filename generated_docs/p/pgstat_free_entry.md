# pgstat_free_entry

## Location
[src/backend/utils/activity/pgstat_shmem.c:801-825](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_shmem.c#L801-L825)

## Overview
Frees a shared statistics entry by deleting it from the shared hash table and releasing its associated dynamic shared memory.

## Definition

```c
static void
pgstat_free_entry(PgStatShared_HashEntry *shent, dshash_seq_status *hstat)
```
## Detailed Description
This function handles the complete deallocation of a shared statistics entry from PostgreSQL's statistics system. It performs a two-step cleanup process: first removing the entry from the shared hash table, then freeing the dynamic shared memory allocated for the entry's data.

The function carefully preserves the DSA (Dynamic Shared Array) pointer before deleting the hash entry to avoid accessing freed memory. It supports two deletion modes: direct deletion using  when called outside of iteration, or sequential deletion using  when called during hash table iteration.

After removing the entry from the hash table and releasing any associated locks, the function frees the actual statistics data stored in the dynamic shared memory area using .

## Parameters / Member Variables
- `*shent`: Pointer to the shared statistics hash entry to be freed, containing the entry key and body data pointer
- `*hstat`: Optional pointer to a sequential iteration status structure. If non-NULL, indicates the function is being called during hash table iteration and  should be used
## Dependencies
- Functions called/Symbols referenced:
  - : Deletes an entry from the shared hash table by direct reference
  - : Deletes the current entry during hash table iteration
  - : Frees memory from the dynamic shared array allocator
  - : Global shared hash table for statistics entries
  - : Global dynamic shared allocator for statistics data

- Called from (representative examples):
  - : Called when releasing the last reference to an entry
  - : Called during explicit entry deletion operations

## Notes and Other Information
- This is a static function used internally within the statistics shared memory module
- The function safely handles memory deallocation by preserving the DSA pointer before hash deletion
- Supports both direct deletion and iteration-safe deletion modes
- The  field contains the DSA pointer to the actual statistics data
- Memory management follows PostgreSQL's dynamic shared memory patterns
- Critical for preventing memory leaks in the shared statistics system
- The function assumes proper locking has been handled by the caller
- Part of the statistics entry lifecycle management in PostgreSQL's shared memory architecture

## Simplified Source

```c
// Simplified version of pgstat_free_entry
static void pgstat_free_entry(PgStatShared_HashEntry *shent, dshash_seq_status *hstat) {
    // Save the DSA pointer before deleting the hash entry
    dsa_pointer pdsa = shent->body;

    // Delete the entry from hash table (two modes available)
    if (!hstat) {
        // Direct deletion mode
        dshash_delete_entry(pgStatLocal.shared_hash, shent);
    } else {
        // Iteration-safe deletion mode
        dshash_delete_current(hstat);
    }

    // Free the actual statistics data memory
    dsa_free(pgStatLocal.dsa, pdsa);
}
```

Key simplifications made:
- Combined variable declaration with assignment for clarity
- Added descriptive comments explaining the two-phase cleanup
- Clarified the two deletion modes with inline comments
- Preserved the essential memory management logic
- Removed detailed comments while maintaining readability