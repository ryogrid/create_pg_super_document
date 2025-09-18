# partition_range_datum_bsearch

## Location
src/backend/partitioning/partbounds.c: 3695 - 3737

## Overview
Performs binary search on range partition bounds to find the greatest range bound that is less than or equal to a given tuple of values.

## Definition
```c
int partition_range_datum_bsearch(FmgrInfo *partsupfunc, Oid *partcollation,
                                 PartitionBoundInfo boundinfo,
                                 int nvalues, Datum *values, bool *is_equal)
```

## Detailed Description
This function implements a binary search algorithm for range partition bounds when given a tuple of data values rather than a complete range bound structure. It searches through the partition bounds to find the index of the greatest bound that is less than or equal to the input tuple. This is essential for tuple routing in range-partitioned tables where we need to determine which partition a specific row belongs to. The function also indicates whether an exact match was found, which helps distinguish between inclusive and exclusive range boundaries.

## Parameters / Member Variables
- `partsupfunc`: Array of function manager info for partition comparison functions
- `partcollation`: Array of collation OIDs for partition key columns
- `boundinfo`: Structure containing partition boundary information
- `nvalues`: Number of values in the input tuple
- `values`: Array of datum values representing the tuple to search for
- `is_equal`: Output parameter set to true if an exact boundary match is found

## Dependencies
- Functions called/Symbols referenced:
  - [PartitionBoundInfo](../P/PartitionBoundInfo.md) (partition boundary structure)
  - [partition_rbound_datum_cmp](partition_rbound_datum_cmp.md) (compares range bound with datum values)
- Called from:
  - [get_partition_for_tuple](../g/get_partition_for_tuple.md) (at src/backend/executor/execPartition.c:1544)
  - [get_matching_range_bounds](../g/get_matching_range_bounds.md) (at src/backend/partitioning/partprune.c:3017, 3144, 3225)
  - partition_bound_has_default (at src/include/partitioning/partbounds.h:139)

## Notes and Other Information
- Returns the index of the matching bound or -1 if no suitable bound is found
- Uses binary search for O(log n) time complexity
- Critical for efficient tuple routing in range-partitioned tables
- Differs from partition_range_bsearch by accepting raw datum values instead of PartitionRangeBound structures
- The `is_equal` parameter helps determine whether a tuple falls exactly on a partition boundary
- Used extensively in partition pruning operations for query optimization