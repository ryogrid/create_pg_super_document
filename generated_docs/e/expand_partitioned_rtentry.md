# expand_partitioned_rtentry

## Location
[src/backend/optimizer/util/inherit.c:318-460](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/inherit.c#L318-L460)

## Overview
Recursively expands a range table entry for a partitioned table by creating child RTEs for live partitions and building necessary planner data structures.

## Definition

```c
static void
expand_partitioned_rtentry(PlannerInfo *root, RelOptInfo *relinfo,
						   RangeTblEntry *parentrte,
						   Index parentRTindex, Relation parentrel,
						   Bitmapset *parent_updatedCols,
						   PlanRowMark *top_parentrc, LOCKMODE lockmode)
```
## Detailed Description
This static function handles the recursive expansion of partitioned tables by discovering live partitions through pruning and creating necessary planner structures for each surviving partition. Key operations include:

1. **Partition Discovery**: Uses PartitionDirectoryLookup to get the partition descriptor and identifies live partitions through prune_append_rel_partitions.

2. **Partition Key Analysis**: Checks if any partition key columns are being updated and sets root->partColsUpdated accordingly.

3. **Child RTE Creation**: For each live partition, creates a child RTE and AppendRelInfo through expand_single_inheritance_child, and builds RelOptInfo structures.

4. **Recursive Processing**: If a child partition is itself partitioned, recursively calls itself with translated column privileges.

5. **Memory Management**: Handles cases where partitions may have been dropped by gracefully removing them from the live partition set.

Unlike traditional inheritance, partitioned tables don't need RTEs for the parent table itself since it contains no data.

## Parameters / Member Variables
- : PlannerInfo structure containing global planner state
- : RelOptInfo for the parent partitioned relation  
- : RangeTblEntry for the parent partitioned table
- : Index of parent RTE in the range table
- : Open Relation structure for the parent table
- : Bitmapset of columns being updated in the parent
- : PlanRowMark for row locking if needed
- : Lock mode to use when opening child relations

## Dependencies
- Functions called/Symbols referenced:
  - [PartitionDirectoryLookup](../P/PartitionDirectoryLookup.md)
  - [prune_append_rel_partitions](../p/prune_append_rel_partitions.md)  
  - [has_partition_attrs](../h/has_partition_attrs.md)
  - [expand_single_inheritance_child](expand_single_inheritance_child.md)
  - [build_simple_rel](../b/build_simple_rel.md)
  - [translate_col_privs](../t/translate_col_privs.md)
  - [try_table_open](../t/try_table_open.md)
  - [expand_planner_arrays](expand_planner_arrays.md)
  - [check_stack_depth](../c/check_stack_depth.md)
- Called from (representative examples):
  - [expand_inherited_rtentry](expand_inherited_rtentry.md)
  - [expand_partitioned_rtentry](expand_partitioned_rtentry.md) (recursive)

## Notes and Other Information
- Uses try_table_open to gracefully handle recently dropped partitions by removing them from live_parts
- Maintains relinfo->part_rels array to store RelOptInfo pointers for each partition
- Updates relinfo->all_partrels with relids of all partition relations
- Handles temporary partitions from other sessions as errors (should not occur)
- The function is recursive to handle multi-level partitioning hierarchies
- Column privilege translation ensures proper handling of column references across partition boundaries