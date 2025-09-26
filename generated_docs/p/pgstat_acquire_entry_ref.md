# pgstat_acquire_entry_ref

## Location
src/backend/utils/activity/pgstat_shmem.c: 342 - 361

## Overview
Helper function that safely acquires a reference to a shared memory statistics entry by incrementing its reference count, releasing locks, and setting up the local reference structure.

## Definition


## Detailed Description
The `pgstat_acquire_entry_ref` function is a critical helper that establishes a safe reference to a shared memory statistics entry. This function is designed to work within the locking protocol of PostgreSQL's statistics system and ensures that:

1. **Reference count management**: Atomically increments the shared entry's reference count to prevent the entry from being deallocated while in use
2. **Lock protocol compliance**: Releases the dshash partition lock that was held during entry lookup, allowing other processes to access the hash table
3. **Local reference setup**: Populates the local `PgStat_EntryRef` structure with pointers and generation information needed for subsequent access
4. **Generation tracking**: Captures the current generation number for cache coherency checks

The function includes safety assertions to verify the entry is properly initialized (magic number check) and has a valid reference count before proceeding.

## Parameters / Member Variables
- `entry_ref`: Local reference structure to be populated with pointers and metadata for the shared entry
- `shhashent`: Pointer to the shared hash entry containing reference count and generation information  
- `shheader`: Pointer to the actual shared statistics data structure

## Dependencies
- Functions called/Symbols referenced:
  - pg_atomic_read_u32
  - pg_atomic_fetch_add_u32  
  - dshash_release_lock
- Called from (representative examples):
  - pgstat_get_entry_ref (multiple locations)

## Notes and Other Information
- This is a static helper function, only accessible within the pgstat_shmem.c module
- Must be called while holding the appropriate dshash partition lock (which it releases)
- The magic number check (0xdeadbeef) ensures the entry has been properly initialized
- Reference count must be > 0 before calling, indicating the entry is valid and not dropped
- The generation number captured here is used later for detecting stale cached references
- Critical for the lock-free reference counting mechanism that allows safe concurrent access to statistics entries