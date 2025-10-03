# hash_seq_search

## Location
[src/backend/utils/hash/dynahash.c:1398-1473](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/hash/dynahash.c#L1398-L1473)

## Overview
Retrieves the next entry during a sequential scan of a PostgreSQL dynamic hash table, returning NULL when the scan is complete.

## Definition

```c
void *
hash_seq_search(HASH_SEQ_STATUS *status)
```
## Detailed Description
This function continues a sequential scan initiated by hash_seq_init(), returning pointers to successive hash table entries. It efficiently traverses the hash table's segmented structure by tracking the current bucket and entry within each bucket's collision chain.

The function implements an optimized iteration strategy:
1. If there's a current entry in the collision chain, it returns the next entry in that chain
2. When a bucket's chain is exhausted, it advances to the next non-empty bucket
3. It navigates the segmented directory structure to find the correct segment and index
4. It automatically handles end-of-scan cleanup when all entries have been visited

The function is designed to handle sparse hash tables efficiently by quickly skipping empty buckets rather than examining each one individually. This optimization is particularly important for nearly empty hash tables.

## Parameters / Member Variables
- `*status`: Pointer to HASH_SEQ_STATUS structure containing the current scan state
## Dependencies
- Functions called/Symbols referenced:
  - ELEMENTKEY (macro to extract key from hash element)
  - [hash_seq_term](hash_seq_term.md) (called automatically at end of scan for cleanup)
  - MOD (macro for modulo operation)
- Called from (representative examples):
  - [LockReleaseAll](../L/LockReleaseAll.md) (for releasing all locks in lock manager)
  - [RelationCacheInvalidate](../R/RelationCacheInvalidate.md) (for cache invalidation)
  - [GetLockStatusData](../G/GetLockStatusData.md) (for collecting lock information)
  - [DropAllPreparedStatements](../D/DropAllPreparedStatements.md) (for statement cleanup)
  - [compute_array_stats](../c/compute_array_stats.md) (for statistical analysis)
  - Various cache invalidation and cleanup functions throughout PostgreSQL

## Notes and Other Information
- Returns void* pointer to the entry's key, or NULL when scan is complete
- Automatically calls hash_seq_term() when reaching end of table
- The caller may safely delete the returned entry before the next call
- Deleting other entries during scan may cause undefined behavior
- Optimized for sparse tables by skipping empty buckets efficiently
- Handles the segmented directory structure transparently
- Used extensively throughout PostgreSQL for bulk operations and cleanup routines
- Critical component for implementing transaction cleanup, cache management, and administrative functions

## Simplified Source

```c
// Simplified version of hash_seq_search
void *hash_seq_search(HASH_SEQ_STATUS *status) {
    HTAB *hashp;
    HASHHDR *hctl;
    uint32 max_bucket;
    long ssize;
    long segment_num;
    long segment_ndx;
    HASHSEGMENT segp;
    uint32 curBucket;
    HASHELEMENT *curElem;

    // Continue scan within current bucket's collision chain
    if ((curElem = status->curEntry) != NULL) {
        status->curEntry = curElem->link;
        if (status->curEntry == NULL)  // End of bucket chain
            ++status->curBucket;
        return (void *) ELEMENTKEY(curElem);
    }

    // Search for next non-empty bucket
    curBucket = status->curBucket;
    hashp = status->hashp;
    hctl = hashp->hctl;
    ssize = hashp->ssize;
    max_bucket = hctl->max_bucket;

    // Check if scan is complete
    if (curBucket > max_bucket) {
        hash_seq_term(status);
        return NULL;
    }

    // Find the correct segment in the directory
    segment_num = curBucket >> hashp->sshift;
    segment_ndx = MOD(curBucket, ssize);
    segp = hashp->dir[segment_num];

    // Skip empty buckets efficiently
    while ((curElem = segp[segment_ndx]) == NULL) {
        if (++curBucket > max_bucket) {
            status->curBucket = curBucket;
            hash_seq_term(status);
            return NULL;
        }
        if (++segment_ndx >= ssize) {
            segment_num++;
            segment_ndx = 0;
            segp = hashp->dir[segment_num];
        }
    }

    // Begin scan of new bucket
    status->curEntry = curElem->link;
    if (status->curEntry == NULL)  // Single element bucket
        ++curBucket;
    status->curBucket = curBucket;

    return (void *) ELEMENTKEY(curElem);
}
```

Key simplifications made:
- Preserved all essential logic flow and algorithm structure
- Added explanatory comments for each major section
- Maintained the optimization for skipping empty buckets
- Kept the segmented directory navigation logic intact
- Preserved the collision chain traversal mechanism
- Maintained proper cleanup calls to hash_seq_term()