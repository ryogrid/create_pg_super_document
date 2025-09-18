# fix_merged_indexes

## Location
[src/backend/partitioning/partbounds.c:2385-2438](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partbounds.c#L2385-L2438)

## Overview
Adjusts merged indexes of re-merged partitions during partition bound merging operations to maintain correct index mapping after partition restructuring.

## Definition


## Detailed Description
The  function is responsible for updating the merged indexes list when partitions are re-merged during partition bound operations. When partition bounds are merged, the original indexes may become invalid due to restructuring, and this function creates a mapping from old merged indexes to new merged indexes and applies this mapping to fix the provided merged_indexes list.

The function operates in two main phases:
1. **Mapping Construction**: It builds a mapping array by examining both outer and inner partition maps' remapping information, creating a translation table from old to new merged indexes.
2. **Index Fixing**: It iterates through the merged_indexes list and updates each index using the constructed mapping, ensuring that all references point to the correct new positions.

## Parameters / Member Variables
- : PartitionMap pointer containing information about the outer partition mapping, including remapping details
- : PartitionMap pointer containing information about the inner partition mapping, including remapping details  
- : Integer representing the number of merged partitions to process
- : List of integer indexes that need to be adjusted based on the new partition structure

## Dependencies
- Functions called/Symbols referenced:
  - [PartitionMap](../P/PartitionMap.md) (struct type)
  - lfirst_int (list access macro)
  - [palloc](../p/palloc.md) (memory allocation)
  - [pfree](../p/pfree.md) (memory deallocation)
- Called from (representative examples):
  - compare_range_bounds
  - [merge_list_bounds](../m/merge_list_bounds.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the partbounds.c file
- The function assumes nmerged > 0 and includes an assertion to validate this
- Memory management is handled properly with palloc/pfree for the temporary new_indexes array
- The function handles cases where either outer_map or inner_map (or both) have performed remapping operations
- Index values of -1 are used to indicate invalid or unset indexes in the mapping process