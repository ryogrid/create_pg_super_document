# merge_partition_with_dummy

## Location
[src/backend/partitioning/partbounds.c:2367-2384](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/partitioning/partbounds.c#L2367-L2384)

## Overview
Assigns a given partition a new partition index in a join relation by merging it with a conceptual "dummy" partition from the other side.

## Definition
static int merge_partition_with_dummy(PartitionMap *map, int index, int *next_index)

## Detailed Description
This function is a utility used in partitionwise join processing when a partition from one side of the join needs to be included in the result but has no corresponding partition on the other side to match with. Instead of a real matching partition, it creates a merged partition by conceptually pairing the given partition with a "dummy" partition.

This scenario commonly occurs in:
1. **Outer joins**: Where partitions from the outer side must be preserved even if they have no matches on the inner side
2. **Full joins**: Where partitions from either side must be preserved when they have no matches on the other side
3. **Situations involving default or NULL partitions**: Where one side has a special partition but the other side doesn't

The function assigns the partition a new merged index and increments the next available index counter. Importantly, it deliberately does NOT set the merged flag for the partition, allowing for potential future adjustments if a real matching partition is found later in the processing.

## Parameters / Member Variables
- `map`: The PartitionMap structure containing the partition to be merged
- `index`: The index of the partition within the map that needs to be merged with a dummy (must be valid and not already merged)
- `next_index`: Pointer to the next available index for merged partitions (incremented by this function)

## Dependencies
- Functions called/Symbols referenced:
  - [PartitionMap](../P/PartitionMap.md) (data type)
- Called from:
  - compare_range_bounds (src/backend/partitioning/partbounds.c:164)
  - [process_outer_partition](../p/process_outer_partition.md) (src/backend/partitioning/partbounds.c:2046)
  - [process_inner_partition](../p/process_inner_partition.md) (src/backend/partitioning/partbounds.c:2128)
  - [merge_null_partitions](merge_null_partitions.md) (src/backend/partitioning/partbounds.c:2199, 2216)
  - [merge_default_partitions](merge_default_partitions.md) (src/backend/partitioning/partbounds.c:2300, 2325)

## Notes and Other Information
- Returns the newly assigned merged partition index
- The function includes assertions to ensure the partition index is valid and the partition hasn't been merged yet
- The merged flag is intentionally left unset (as noted in the comment "Leave the merged flag alone!") to allow for future adjustments
- This is a key building block used by higher-level partition merging functions when handling asymmetric join scenarios
- The "dummy" partition is conceptual - no actual dummy partition structure is created; instead, the function simply assigns the partition to a new slot in the merged partition space
- This function enables partitionwise join optimization to handle cases where partitions don't have exact matches on both sides, which is essential for complete join processing

## Simplified Source

```c
static int merge_partition_with_dummy(PartitionMap *map, int index, int *next_index) {
    int merged_index = *next_index;

    // Validate input parameters
    Assert(index >= 0 && index < map->nparts);
    Assert(map->merged_indexes[index] == -1);  // Not already merged
    Assert(!map->merged[index]);               // Not marked as merged

    // Assign this partition to the next available merged partition slot
    map->merged_indexes[index] = merged_index;

    // Intentionally leave the merged flag unset to allow future adjustments
    // if a real matching partition is found later

    // Advance to next available index
    *next_index = *next_index + 1;

    return merged_index;
}
```