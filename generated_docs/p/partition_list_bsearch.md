# partition_list_bsearch

## Location
src/backend/partitioning/partbounds.c: 3607 - 3652

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