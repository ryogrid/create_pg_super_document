# create_list_bounds

## Location
src/backend/partitioning/partbounds.c: 462 - 676

## Overview
Creates a PartitionBoundInfo structure specifically for list partitioned tables, converting list partition bound specifications into the internal representation with support for null, default, and interleaved partitions.

## Definition
```c
static PartitionBoundInfo create_list_bounds(PartitionBoundSpec **boundspecs, int nparts, PartitionKey key, int **mapping)
```

## Detailed Description
This function implements list partition bounds creation by processing an array of list partition specifications and building the comprehensive internal PartitionBoundInfo structure. The function handles the complexities of list partitioning including:

1. Processing non-null values from all partitions into a unified sorted array
2. Handling special partitions: NULL-accepting and DEFAULT partitions
3. Creating canonical index mappings for efficient partition lookup
4. Detecting and marking interleaved partitions for optimization purposes
5. Building the datums array with properly copied partition values

The function first counts non-null datums, creates a unified PartitionListValue array, sorts it using the partition key's comparison function, and then builds the final PartitionBoundInfo structure with proper index mappings. It also performs sophisticated analysis to detect interleaved partitions where multiple partitions may contain overlapping or out-of-order values.

## Parameters / Member Variables
- `boundspecs`: Array of PartitionBoundSpec pointers containing list partition specifications with their listdatums
- `nparts`: Number of list partitions to process
- `key`: PartitionKey containing the partitioning strategy, type information, and comparison functions
- `mapping`: Output parameter - array mapping original partition indexes to canonical sorted indexes

## Dependencies
- Functions called/Symbols referenced:
  - get_non_null_list_datum_count
  - palloc0
  - palloc
  - pfree
  - qsort_arg
  - qsort_partition_list_value_cmp
  - datumCopy
  - partition_bound_accepts_nulls
  - partition_bound_has_default
  - bms_add_member
  - lfirst_node
  - foreach
  - PARTITION_STRATEGY_LIST
  - PartitionListValue
  - PartitionBoundInfoData
- Called from (representative examples):
  - partition_bounds_create (src/backend/partitioning/partbounds.c:332)

## Notes and Other Information
- Static function, only accessible within partbounds.c
- Supports NULL partitions (partitions that accept NULL values) with null_index tracking
- Supports DEFAULT partitions (catch-all partitions) with default_index tracking
- Implements sophisticated interleaved partition detection for query optimization
- Uses single large memory allocation for boundDatums array for efficiency
- Validates that only one partition can accept NULL values
- Performs deep copying of partition values using datumCopy to ensure memory safety
- The interleaved_parts bitmap identifies partitions that may contain overlapping value ranges
- Canonical indexing enables efficient binary search and partition elimination during query planning
- Essential component for list partition constraint checking and partition pruning