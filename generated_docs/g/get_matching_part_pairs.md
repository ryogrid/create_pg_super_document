# get_matching_part_pairs

## Location
[src/backend/optimizer/path/joinrels.c:1881-1971](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/joinrels.c#L1881-L1971)

## Overview
Generates pairs of matching partitions from input relations to be joined when their partition bounds have been merged.

## Definition
static void get_matching_part_pairs(PlannerInfo *root, RelOptInfo *joinrel, RelOptInfo *rel1, RelOptInfo *rel2, List **parts1, List **parts2)

## Detailed Description
This function creates lists of corresponding partition pairs that need to be joined when the join relation has merged partition bounds (i.e., when input relations don't have identical partitioning schemes). It examines each partition segment in the join relation and determines which specific partitions from each input relation contribute to that segment.

The process works as follows for each join segment:

1. **Empty segment handling**: If a join segment is empty (child_joinrel is NULL), indicating it was previously ignored due to empty inputs, the function adds NULL entries to both output lists to maintain positional correspondence.

2. **Partition identification**: For non-empty segments, it determines which specific partitions from each input relation participate in the join by:
   - Intersecting the child join's relids with each input relation's all_partrels
   - Ensuring the intersection contains the same number of relations as the input

3. **Child relation lookup**: Based on whether each input is a simple base relation or a join relation:
   - For simple relations: Uses bms_singleton_member to get the partition's varno and finds the base relation
   - For join relations: Uses find_join_rel to locate the appropriate child join relation

4. **Pair generation**: Adds the identified child relations to the corresponding output lists, creating matched pairs for partitionwise join processing.

This function is essential when partition bounds have been merged, as it establishes the correct mapping between input partitions and output join segments.

## Parameters / Member Variables
- : PlannerInfo containing global planner state and relation information
- : The target join relation containing merged partition bounds and child join segments
- : First input relation whose partitions need to be paired
- : Second input relation whose partitions need to be paired
- : Output parameter - list of rel1's partitions corresponding to each join segment
- : Output parameter - list of rel2's partitions corresponding to each join segment

## Dependencies
- Functions called/Symbols referenced:
  - [bms_intersect](../b/bms_intersect.md)
  - [bms_num_members](../b/bms_num_members.md)
  - [bms_singleton_member](../b/bms_singleton_member.md)
  - [find_base_rel](../f/find_base_rel.md)
  - [find_join_rel](../f/find_join_rel.md)
  - [lappend](../l/lappend.md)
  - IS_SIMPLE_REL
  - Assert
- Called from (representative examples):
  - [compute_partition_bounds](../c/compute_partition_bounds.md)

## Notes and Other Information
- Only called when joinrel->partbounds_merged is true, indicating partition bounds required merging
- Maintains NULL entries in output lists for empty join segments to preserve positional correspondence
- Handles both simple base relations and complex join relations as inputs
- Relies on the assumption that matching partitions should exist due to prior partitionwise join planning
- Critical for establishing correct partition pairing when input relations have different partitioning schemes
- Output lists have the same length as joinrel->nparts, with each position corresponding to a join segment

## Simplified Source

```c
static void
get_matching_part_pairs(PlannerInfo *root, RelOptInfo *joinrel,
                        RelOptInfo *rel1, RelOptInfo *rel2,
                        List **parts1, List **parts2)
{
    bool rel1_is_simple = IS_SIMPLE_REL(rel1);
    bool rel2_is_simple = IS_SIMPLE_REL(rel2);
    int cnt_parts;

    *parts1 = NIL;
    *parts2 = NIL;

    // Process each partition segment in the join relation
    for (cnt_parts = 0; cnt_parts < joinrel->nparts; cnt_parts++)
    {
        RelOptInfo *child_joinrel = joinrel->part_rels[cnt_parts];
        RelOptInfo *child_rel1;
        RelOptInfo *child_rel2;
        Relids child_relids1;
        Relids child_relids2;

        // Handle empty segments - add NULLs to maintain position correspondence
        if (!child_joinrel)
        {
            *parts1 = lappend(*parts1, NULL);
            *parts2 = lappend(*parts2, NULL);
            continue;
        }

        // Find rel1 partitions involved in this join segment
        child_relids1 = bms_intersect(child_joinrel->relids, rel1->all_partrels);

        // Get the corresponding child relation from rel1
        if (rel1_is_simple)
        {
            int varno = bms_singleton_member(child_relids1);
            child_rel1 = find_base_rel(root, varno);
        }
        else
            child_rel1 = find_join_rel(root, child_relids1);

        // Find rel2 partitions involved in this join segment
        child_relids2 = bms_intersect(child_joinrel->relids, rel2->all_partrels);

        // Get the corresponding child relation from rel2
        if (rel2_is_simple)
        {
            int varno = bms_singleton_member(child_relids2);
            child_rel2 = find_base_rel(root, varno);
        }
        else
            child_rel2 = find_join_rel(root, child_relids2);

        // Add the matched pair to output lists
        *parts1 = lappend(*parts1, child_rel1);
        *parts2 = lappend(*parts2, child_rel2);
    }
}
```