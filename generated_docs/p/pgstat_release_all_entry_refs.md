# pgstat_release_all_entry_refs

## Location
[src/backend/utils/activity/pgstat_shmem.c:767-778](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_shmem.c#L767-L778)

## Overview
Releases all local references to shared stats entries in the current process to allow proper cleanup during process exit.

## Definition

```c
static void
pgstat_release_all_entry_refs(bool discard_pending)
```
## Detailed Description
This function is responsible for releasing all local references to shared statistics entries that a process has acquired during its lifetime. It's a critical cleanup function that prevents memory leaks in the shared statistics system. When a process exits, it must release all references to shared stats entries; otherwise, those entries could never be freed from the shared memory segment.

The function works by delegating to  with NULL match criteria, meaning all entries are released. After releasing all references, it verifies that the hash table is empty and destroys the local entry reference hash table structure.

## Parameters / Member Variables
- `discard_pending`: Boolean flag indicating whether pending statistics updates should be discarded rather than flushed to shared memory before releasing references
## Dependencies
- Functions called/Symbols referenced:
  - : Core function that performs the actual reference release logic
  - : Destroys the local hash table structure
  - : Global hash table containing local references to shared stats entries

- Called from (representative examples):
  - : Called during process shutdown to clean up statistics references

## Notes and Other Information
- This is a static function only used within the statistics shared memory module
- The function includes safety checks - it returns early if the reference hash table doesn't exist
- After completion,  is set to NULL, indicating no local references remain
- The Assert statement ensures that all references were properly released before destroying the hash table
- This function is essential for preventing shared memory leaks in PostgreSQL's statistics collection system

## Simplified Source

```c
// Simplified version of pgstat_release_all_entry_refs
static void pgstat_release_all_entry_refs(bool discard_pending) {
    // Early return if no reference hash table exists
    if (pgStatEntryRefHash == NULL)
        return;

    // Release all entry references with no specific filter
    pgstat_release_matching_entry_refs(discard_pending, NULL, 0);

    // Verify all references were released
    Assert(pgStatEntryRefHash->members == 0);

    // Clean up and destroy the hash table
    pgstat_entry_ref_hash_destroy(pgStatEntryRefHash);
    pgStatEntryRefHash = NULL;
}
```

Key simplifications made:
- Added explanatory comments for each major operation
- Preserved the safety check for null hash table
- Maintained the assertion for verification
- Kept the cleanup sequence intact