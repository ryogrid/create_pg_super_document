# hash_seq_init

## Location
[src/backend/utils/hash/dynahash.c:1388-1397](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/hash/dynahash.c#L1388-L1397)

## Overview
Initializes a sequential scan over a PostgreSQL dynamic hash table, setting up the iteration state to traverse all entries in the table.

## Definition

```c
void
hash_seq_init(HASH_SEQ_STATUS *status, HTAB *hashp)
```
## Detailed Description
This function initializes a HASH_SEQ_STATUS structure to begin a sequential scan through all entries in a hash table. It sets up the initial state for iteration, starting from bucket 0 with no current entry. The function also handles registration of the sequential scan for non-frozen hash tables, which is important for proper cleanup and concurrency control.

Sequential scanning allows traversal of all entries in a hash table without needing to know their keys. This is particularly useful for operations that need to examine every entry, such as cleanup routines, statistics collection, or bulk operations.

The function supports both regular and partitioned hash tables. For partitioned tables, the caller must hold at least shared locks on all partitions throughout the scan to ensure consistency and prevent issues with concurrent modifications by other backends.

## Parameters / Member Variables
- : Pointer to a HASH_SEQ_STATUS structure that will track the scan state
- : Pointer to the hash table (HTAB) to be scanned

## Dependencies
- Functions called/Symbols referenced:
  - [register_seq_scan](../r/register_seq_scan.md) (registers the scan for cleanup tracking on non-frozen tables)
- Called from (representative examples):
  - [LockReleaseAll](../L/LockReleaseAll.md) (in lock manager for releasing locks)
  - [RelationCacheInvalidate](../R/RelationCacheInvalidate.md) (for invalidating relation cache entries)
  - [GetLockStatusData](../G/GetLockStatusData.md) (for collecting lock status information)
  - [DropAllPreparedStatements](../D/DropAllPreparedStatements.md) (for cleaning up prepared statements)
  - [AtEOXact_RelationCache](../A/AtEOXact_RelationCache.md) (for end-of-transaction cleanup)
  - [compute_array_stats](../c/compute_array_stats.md) (for statistical analysis of arrays)
  - Various other cleanup and administrative functions

## Notes and Other Information
- Must be followed by hash_seq_search() calls to iterate through entries
- [hash_seq_term](hash_seq_term.md)() should be called if the scan is abandoned before completion
- If hash_seq_search() returns NULL, end-of-scan cleanup is automatic
- The caller may delete the currently returned element during iteration
- Deleting other elements during scan is undefined behavior
- Adding elements during scan may or may not visit the new elements
- For frozen hash tables, hash_seq_term cleanup is not necessary
- Widely used throughout PostgreSQL for bulk operations and system maintenance
- Critical for implementing various cleanup and administrative operations