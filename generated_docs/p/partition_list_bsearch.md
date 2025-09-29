# partition_list_bsearch

## Location
[src/backend/partitioning/partbounds.c:3607-3652](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partbounds.c#L3607-L3652)

## Overview
Performs binary search on list partition bounds to find the greatest bound datum that is less than or equal to a given value.

## Definition
```c
int partition_list_bsearch(FmgrInfo *partsupfunc, Oid *partcollation,
                          PartitionBoundInfo boundinfo,
                          Datum value, bool *is_equal)
```

## Detailed Description
This function implements a binary search algorithm specifically designed for list partitioning. It searches through the partition bound datums to find the index of the greatest bound datum that is less than or equal to the given input value. If all bound datums are greater than the input value, it returns -1. The function also sets an output parameter to indicate whether an exact match was found. This is essential for list partition pruning and tuple routing operations.

## Parameters / Member Variables
- `partsupfunc`: Array of function manager info for partition comparison functions
- `partcollation`: Array of collation OIDs for partition key columns  
- `boundinfo`: Structure containing partition boundary information including datums array
- `value`: The input datum value to search for
- `is_equal`: Output parameter set to true if an exact match is found

## Dependencies
- Functions called/Symbols referenced:
  - [PartitionBoundInfo](../P/PartitionBoundInfo.md) (partition boundary structure)
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md) (calls comparison function with collation)
  - [DatumGetInt32](../D/DatumGetInt32.md) (extracts int32 from comparison result)
- Called from:
  - [get_partition_for_tuple](../g/get_partition_for_tuple.md) (at src/backend/executor/execPartition.c:1472)
  - [check_new_partition_bound](../c/check_new_partition_bound.md) (at src/backend/partitioning/partbounds.c:3073)
  - [get_matching_list_bounds](../g/get_matching_list_bounds.md) (at src/backend/partitioning/partprune.c:2809, 2840, 2857, 2892)
  - partition_bound_has_default (at src/include/partitioning/partbounds.h:135)

## Notes and Other Information
- Returns the index of the matching bound or -1 if no suitable bound is found
- Uses a binary search algorithm with O(log n) time complexity
- The search finds the rightmost position where value could be inserted while maintaining sorted order
- Critical for efficient partition pruning in list-partitioned tables
- The `is_equal` parameter allows callers to distinguish between exact matches and range positions

## Simplified Source
```c
int partition_list_bsearch(FmgrInfo *partsupfunc, Oid *partcollation,
                          PartitionBoundInfo boundinfo,
                          Datum value, bool *is_equal)
{
    int lo = -1;
    int hi = boundinfo->ndatums - 1;

    // Binary search through list partition bounds
    while (lo < hi) {
        int mid = (lo + hi + 1) / 2;

        // Compare bound datum with target value using partition comparison function
        int32 cmpval = DatumGetInt32(FunctionCall2Coll(&partsupfunc[0],
                                                       partcollation[0],
                                                       boundinfo->datums[mid][0],
                                                       value));

        if (cmpval <= 0) {
            lo = mid;
            *is_equal = (cmpval == 0);
            if (*is_equal)
                break;  // Found exact match
        } else {
            hi = mid - 1;
        }
    }

    return lo;  // Index of greatest bound <= value, or -1 if none found
}
```