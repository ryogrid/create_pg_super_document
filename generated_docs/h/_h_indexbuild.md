# _h_indexbuild

## Location
[src/backend/access/hash/hashsort.c:120-157](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashsort.c#L120-L157)

## Overview
Creates the entire hash index by processing sorted tuples from the spool and inserting them into the index structure.

## Definition

```c
void
_h_indexbuild(HSpool *hspool, Relation heapRel)
```
## Detailed Description
This function completes the hash index construction process by first performing the sort operation on all spooled tuples, then retrieving them in sorted order and inserting each tuple into the hash index. The sorting is primarily a performance optimization to improve locality of access during insertion. The function includes assertion checking to verify that tuples are indeed returned in hash key order, ensuring the tuplesort subsystem correctly handles hash index tuple sorting. Progress is tracked and reported during the insertion phase, and the operation can be interrupted.

## Parameters / Member Variables
- : Pointer to the HSpool structure containing the sorted tuple data
- : The heap relation being indexed (used for insertion context)

## Dependencies
- Functions called/Symbols referenced:
  - [HSpool](../H/HSpool.md) (structure type)
  - [tuplesort_performsort](../t/tuplesort_performsort.md) (executes the sort operation)
  - [tuplesort_getindextuple](../t/tuplesort_getindextuple.md) (retrieves sorted tuples)
  - [_hash_hashkey2bucket](_hash_hashkey2bucket.md) (computes bucket for hash key)
  - [_hash_get_indextuple_hashkey](_hash_get_indextuple_hashkey.md) (extracts hash key from tuple)
  - [_hash_doinsert](_hash_doinsert.md) (inserts tuple into hash index)
  - [pgstat_progress_update_param](../p/pgstat_progress_update_param.md) (reports progress)
  - PROGRESS_CREATEIDX_TUPLES_DONE (progress tracking constant)
- Called from (representative examples):
  - [hashbuild](hashbuild.md)

## Notes and Other Information
- Sorting is primarily for performance optimization (locality of access) rather than correctness
- Includes assertion checking to validate tuplesort's hash index tuple ordering
- Supports progress reporting for long-running index builds
- Can be interrupted via CHECK_FOR_INTERRUPTS() calls
- The 'sorted' parameter to  is always true since tuples come pre-sorted
- Part of the final phase of hash index construction after all tuples have been spooled

## Simplified Source

```c
void _h_indexbuild(HSpool *hspool, Relation heapRel)
{
    IndexTuple itup;
    int64 tups_done = 0;

    // Sort all spooled tuples for better insertion locality
    tuplesort_performsort(hspool->sortstate);

    // Insert each sorted tuple into the hash index
    while ((itup = tuplesort_getindextuple(hspool->sortstate, true)) != NULL)
    {
        // Insert tuple into hash index (sorted=true for performance)
        _hash_doinsert(hspool->index, itup, heapRel, true);

        // Allow interruption and track progress
        CHECK_FOR_INTERRUPTS();
        pgstat_progress_update_param(PROGRESS_CREATEIDX_TUPLES_DONE, ++tups_done);
    }
}
```