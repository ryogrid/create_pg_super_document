# adjust_partition_colnos

## Location
[src/backend/executor/execPartition.c:1699-1715](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execPartition.c#L1699-L1715)

## Overview
Adjusts a list of UPDATE target column numbers to account for attribute differences between a parent partitioned table and its child partition.

## Definition
```c
static List *adjust_partition_colnos(List *colnos, ResultRelInfo *leaf_part_rri)
```

## Detailed Description
This function handles column number translation when performing UPDATE operations on partitioned tables where the partition has a different column layout than the parent table. It uses the tuple conversion map obtained from ExecGetChildToRootMap() to translate column numbers from the parent table's attribute numbering to the partition's attribute numbering.

The function serves as a wrapper around adjust_partition_colnos_using_map(), obtaining the necessary attribute mapping from the ResultRelInfo and delegating the actual column number adjustment logic to the more general mapping function.

This translation is necessary because:
1. Partitions may have different column orders than the parent
2. Partitions may have additional or fewer columns (e.g., inheritance hierarchies)
3. Column attribute numbers may not match between parent and child due to schema evolution

## Parameters / Member Variables
- `colnos`: List of column numbers (AttrNumber values) that need to be adjusted from parent table numbering to partition numbering
- `leaf_part_rri`: ResultRelInfo for the leaf partition, which contains the tuple conversion map needed for translation

## Dependencies
- Functions called/Symbols referenced:
  - [ExecGetChildToRootMap](../E/ExecGetChildToRootMap.md) (retrieves the tuple conversion map for parent-to-child translation)
  - [adjust_partition_colnos_using_map](adjust_partition_colnos_using_map.md) (performs the actual column number adjustment using the attribute map)
- Called from (representative examples):
  - [ExecInitPartitionInfo](../E/ExecInitPartitionInfo.md) (during partition initialization for UPDATE operations)

## Notes and Other Information
- This is a static function used internally within the partition routing subsystem
- The function includes an assertion that the conversion map must not be NULL, indicating that it should only be called when column adjustment is actually required
- The comment explicitly states that this function "mustn't be called if no adjustment is required", emphasizing that callers should check whether adjustment is needed before calling
- This function is part of the infrastructure that allows UPDATE operations to work transparently across partitioned tables despite schema differences
- The returned List contains the adjusted column numbers that can be used for operations on the specific partition

## Simplified Source

```c
static List *
adjust_partition_colnos(List *colnos, ResultRelInfo *leaf_part_rri)
{
    // Get the attribute mapping from child to root
    TupleConversionMap *map = ExecGetChildToRootMap(leaf_part_rri);

    // Must have a conversion map if this function is called
    Assert(map != NULL);

    // Delegate to the general mapping function
    return adjust_partition_colnos_using_map(colnos, map->attrMap);
}
```