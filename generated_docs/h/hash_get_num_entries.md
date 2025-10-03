# hash_get_num_entries

## Location
[src/backend/utils/hash/dynahash.c:1344-1387](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/hash/dynahash.c#L1344-L1387)

## Overview
Returns the total number of entries currently stored in a PostgreSQL dynamic hash table, handling both partitioned and non-partitioned tables.

## Definition

```c
long
hash_get_num_entries(HTAB *hashp)
```
## Detailed Description
This function provides a way to query the current number of entries in a hash table. For non-partitioned tables, it simply returns the nentries count from the single freelist. For partitioned tables, it sums the nentries counts across all freelists (NUM_FREELISTS) to provide the total count.

The function is designed to be called when the caller has appropriate locks on the table partitions, as it does not acquire mutexes internally for performance reasons. This design assumes that the caller has ensured exclusive access or is comfortable with potentially reading slightly inconsistent intermediate values during concurrent modifications.

## Parameters / Member Variables
- `*hashp`: Pointer to the hash table structure (HTAB) whose entry count should be returned
## Dependencies
- Functions called/Symbols referenced:
  - IS_PARTITIONED (macro to determine if the hash table is partitioned)
  - NUM_FREELISTS (constant defining the number of freelists in partitioned tables)
- Called from (representative examples):
  - [XLogHaveInvalidPages](../X/XLogHaveInvalidPages.md) (in transaction log utilities)
  - [GetLockStatusData](../G/GetLockStatusData.md) (in lock manager for status reporting)
  - [GetPredicateLockStatusData](../G/GetPredicateLockStatusData.md) (in predicate locking system)
  - [hash_stats](hash_stats.md) (for hash table statistics reporting)
  - [compute_array_stats](../c/compute_array_stats.md) (in array type analysis)
  - [compute_tsvector_stats](../c/compute_tsvector_stats.md) (in text search statistics)
  - Various estimation functions for space planning

## Notes and Other Information
- Returns a long integer representing the total entry count
- Does not acquire mutexes for performance reasons - caller must ensure appropriate locking
- For partitioned tables, iterates through all freelists to sum their entry counts
- Widely used throughout PostgreSQL for statistics, monitoring, and space estimation
- The function assumes the caller has established proper synchronization if exact counts are required
- Used extensively in system administration and monitoring functions

## Simplified Source

```c
// Simplified version of hash_get_num_entries
long hash_get_num_entries(HTAB *hashp) {
    // Start with entries from the first freelist
    long total_entries = hashp->hctl->freeList[0].nentries;

    // If table is partitioned, sum entries from all partitions
    if (IS_PARTITIONED(hashp->hctl)) {
        for (int i = 1; i < NUM_FREELISTS; i++) {
            total_entries += hashp->hctl->freeList[i].nentries;
        }
    }

    return total_entries;
}
```

Key simplifications made:
- Removed detailed comment about mutex handling for clarity
- Used more descriptive variable name (total_entries instead of sum)
- Added inline comments explaining the core logic steps
- Consolidated variable declaration with initialization
- Focused on the main algorithm: sum entries from all relevant freelists