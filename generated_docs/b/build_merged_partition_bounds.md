# build_merged_partition_bounds

## Location
[src/backend/partitioning/partbounds.c:2518-2580](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partbounds.c#L2518-L2580)

## Overview
Creates a PartitionBoundInfo struct from merged partition bounds, constructing the final boundary information structure needed for partition-wise operations.

## Definition
```c
static PartitionBoundInfo build_merged_partition_bounds(char strategy, List *merged_datums,
                                                       List *merged_kinds, List *merged_indexes,
                                                       int null_index, int default_index)
```

## Detailed Description
The `build_merged_partition_bounds` function constructs a complete PartitionBoundInfo structure from the component lists that have been built during partition bound merging operations. This function serves as the final assembly step in partition boundary creation for join operations.

The function handles both range and list partitioning strategies differently:

**For Range Partitioning:**
- Processes both datums and kinds arrays since range partitions need boundary kind information
- Adds an extra -1 index to merged_indexes to account for the additional boundary in range partitioning (n+1 boundaries for n partitions)
- Allocates and populates the kind array with PartitionRangeDatumKind information

**For List Partitioning:**
- Only processes datums as list partitions don't need boundary kind information
- Sets the kind field to NULL since it's not applicable

The function also handles special partition indexes for NULL values and default partitions, and sets interleaved_parts to NULL as it's not used for join relations.

## Parameters / Member Variables
- `strategy`: Character indicating the partitioning strategy (PARTITION_STRATEGY_RANGE or PARTITION_STRATEGY_LIST)
- `merged_datums`: List of Datum arrays representing the merged partition boundary values
- `merged_kinds`: List of PartitionRangeDatumKind arrays (only used for range partitioning, NIL for list)
- `merged_indexes`: List of integers representing partition indexes for each boundary
- `null_index`: Integer index for the partition that handles NULL values (-1 if none)
- `default_index`: Integer index for the default partition (-1 if none)

## Dependencies
- Functions called/Symbols referenced:
  - [PartitionBoundInfo](../P/PartitionBoundInfo.md) (return type)
  - PartitionBoundInfoData (struct for allocation)
  - [PartitionRangeDatumKind](../P/PartitionRangeDatumKind.md) (enum type for range boundary kinds)
  - PARTITION_STRATEGY_RANGE (constant)
  - PARTITION_STRATEGY_LIST (constant)
  - lappend_int (list manipulation)
  - lfirst_int (list access)
  - lfirst (list access)
  - list_length (list utility)
  - [palloc](../p/palloc.md) (memory allocation)
- Called from (representative examples):
  - compare_range_bounds
  - [merge_list_bounds](../m/merge_list_bounds.md)
  - [merge_range_bounds](../m/merge_range_bounds.md)

## Notes and Other Information
- This is a static function, accessible only within partbounds.c
- The function properly handles the difference in index count between range and list partitioning (range has n+1 indexes for n partitions)
- Memory allocation is handled for all dynamic arrays (datums, kind, indexes)
- The interleaved_parts field is always set to NULL for join relations as mentioned in the comment
- For range partitioning, the function appends an additional -1 index to handle the upper boundary
- The function validates input consistency with assertions (merged_kinds length matches ndatums for range strategy)
- All allocated memory becomes part of the returned PartitionBoundInfo structure