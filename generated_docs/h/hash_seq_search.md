# hash_seq_search

## Location
src/backend/utils/hash/dynahash.c: 1398 - 1473

## Overview
Retrieves the next entry during a sequential scan of a PostgreSQL dynamic hash table, returning NULL when the scan is complete.

## Definition


## Detailed Description
This function continues a sequential scan initiated by hash_seq_init(), returning pointers to successive hash table entries. It efficiently traverses the hash table's segmented structure by tracking the current bucket and entry within each bucket's collision chain.

The function implements an optimized iteration strategy:
1. If there's a current entry in the collision chain, it returns the next entry in that chain
2. When a bucket's chain is exhausted, it advances to the next non-empty bucket
3. It navigates the segmented directory structure to find the correct segment and index
4. It automatically handles end-of-scan cleanup when all entries have been visited

The function is designed to handle sparse hash tables efficiently by quickly skipping empty buckets rather than examining each one individually. This optimization is particularly important for nearly empty hash tables.

## Parameters / Member Variables
- : Pointer to HASH_SEQ_STATUS structure containing the current scan state

## Dependencies
- Functions called/Symbols referenced:
  - ELEMENTKEY (macro to extract key from hash element)
  - hash_seq_term (called automatically at end of scan for cleanup)
  - MOD (macro for modulo operation)
- Called from (representative examples):
  - LockReleaseAll (for releasing all locks in lock manager)
  - RelationCacheInvalidate (for cache invalidation)
  - GetLockStatusData (for collecting lock information)
  - DropAllPreparedStatements (for statement cleanup)
  - compute_array_stats (for statistical analysis)
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