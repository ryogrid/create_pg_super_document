# bottomup_sort_and_shrink_cmp

## Location
[src/backend/access/heap/heapam.c:8580-8652](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L8580-L8652)

## Overview
A qsort comparison function for bottomup_sort_and_shrink() that implements a sophisticated multi-level sorting strategy for IndexDeleteCounts structures using power-of-two bucketing.

## Definition
```c
static int bottomup_sort_and_shrink_cmp(const void *arg1, const void *arg2)
```

## Detailed Description
This function implements a three-tier comparison strategy for sorting IndexDeleteCounts structures in bottom-up index deletion processing:

1. **Primary Sort**: npromisingtids field in descending order (most promising first)
   - Uses pre-normalized power-of-two bucket values
   - Prioritizes blocks with more promising tuple deletions

2. **Secondary Sort**: ntids field in descending order (most TIDs first)
   - Applies power-of-two rounding using pg_nextpower2_32() for bucketing
   - Groups blocks with similar TID counts together

3. **Tertiary Sort**: ifirsttid field in ascending order (heap block number order)
   - Sorts by offset into deltids array (equivalent to heap block number order)
   - Avoids accessing out-of-line TID data by relying on pre-sorted deltids array
   - Maintains spatial locality for otherwise equivalent block groups

The design ensures that the most valuable deletion opportunities (high npromisingtids) are processed first, while maintaining good spatial locality through the final heap block ordering.

## Parameters / Member Variables
- `arg1`: Pointer to first IndexDeleteCounts structure (cast from void*)
- `arg2`: Pointer to second IndexDeleteCounts structure (cast from void*)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_nextpower2_32](../p/pg_nextpower2_32.md)
  - pg_unreachable
  - [IndexDeleteCounts](../I/IndexDeleteCounts.md) (structure type)
- Called from (representative examples):
  - [bottomup_sort_and_shrink](bottomup_sort_and_shrink.md)

## Notes and Other Information
- Uses power-of-two bucketing scheme for npromisingtids (pre-normalized by caller)
- Applies power-of-two rounding to ntids during comparison for consistent bucketing
- Final tiebreaker assumes deltids array was pre-sorted in ascending heap TID order
- Uses pg_unreachable() to indicate all comparison cases should be handled by the three tiers
- The multi-level sort optimizes for both deletion efficiency (promising TIDs first) and I/O efficiency (spatial locality)
- Located in src/backend/access/heap/heapam.c:8580-8652

## Simplified Source

```c
static int bottomup_sort_and_shrink_cmp(const void *arg1, const void *arg2)
{
    const IndexDeleteCounts *group1 = (const IndexDeleteCounts *) arg1;
    const IndexDeleteCounts *group2 = (const IndexDeleteCounts *) arg2;

    // Primary sort: Most promising TIDs first (descending order)
    // npromisingtids should already be power-of-two normalized by caller
    if (group1->npromisingtids > group2->npromisingtids)
        return -1;
    if (group1->npromisingtids < group2->npromisingtids)
        return 1;

    // Secondary sort: Most TIDs first (descending order with power-of-two bucketing)
    if (group1->ntids != group2->ntids)
    {
        uint32 ntids1 = pg_nextpower2_32((uint32) group1->ntids);
        uint32 ntids2 = pg_nextpower2_32((uint32) group2->ntids);

        if (ntids1 > ntids2)
            return -1;
        if (ntids1 < ntids2)
            return 1;
    }

    // Tertiary sort: Ascending heap block order (spatial locality)
    // Uses offset into deltids array as proxy for block number
    if (group1->ifirsttid > group2->ifirsttid)
        return 1;
    if (group1->ifirsttid < group2->ifirsttid)
        return -1;

    // Should never reach here with proper input
    pg_unreachable();
    return 0;
}
```