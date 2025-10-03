# generate_partitionwise_join_paths

## Location
[src/backend/optimizer/path/allpaths.c:4291-4362](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/allpaths.c#L4291-L4362)

## Overview
Creates partitionwise join paths for partitioned relations by recursively building paths for child partitions and combining them into an append relation.

## Definition

```c
void
generate_partitionwise_join_paths(PlannerInfo *root, RelOptInfo *rel)
```
## Detailed Description
This function implements partitionwise join optimization, where joins between partitioned tables are performed by joining corresponding partitions separately and then appending the results. This approach can be significantly more efficient than materializing all partitions before joining.

The function operates recursively on partition hierarchies, processing each child partition to generate join paths. It collects non-dummy child relations, ensures each has viable paths, and then combines them using add_paths_to_append_rel. If any child partition fails to produce viable paths, the entire partitionwise join is abandoned and the relation is marked as unpartitioned.

Key safety measures include stack depth checking to prevent infinite recursion in deep partition hierarchies, and proper handling of pruned partitions that may be NULL.

## Parameters / Member Variables
- `*root`: PlannerInfo containing global planner state and join search information
- `*rel`: RelOptInfo for the partitioned join relation to generate paths for
## Dependencies
- Functions called/Symbols referenced:
  - IS_JOIN_REL (macro checking if relation is a join)
  - IS_PARTITIONED_REL (macro checking if relation is partitioned)
  - [check_stack_depth](../c/check_stack_depth.md) (guards against stack overflow)
  - [generate_partitionwise_join_paths](generate_partitionwise_join_paths.md) (recursive call for child partitions)
  - [set_cheapest](../s/set_cheapest.md) (identifies cheapest path for child relations)
  - IS_DUMMY_REL (macro checking if relation produces no rows)
  - [pprint](../p/pprint.md) (debug printing function)
  - [mark_dummy_rel](../m/mark_dummy_rel.md) (marks relation as producing no rows)
  - [add_paths_to_append_rel](../a/add_paths_to_append_rel.md) (combines child paths into append paths)
  - [list_free](../l/list_free.md) (deallocates list memory)
- Called from (representative examples):
  - [merge_clump](../m/merge_clump.md) (in GEQO genetic query optimization)
  - [standard_join_search](../s/standard_join_search.md) (in standard dynamic programming join search)
  - [generate_partitionwise_join_paths](generate_partitionwise_join_paths.md) (recursive calls for nested partitions)

## Notes and Other Information
- Must be called after all child-join paths are complete to avoid path deletion issues
- Recursively handles nested partition hierarchies
- If any child partition lacks viable paths, the entire partitionwise join is abandoned
- Dummy child relations (those producing no rows) are excluded from the final append
- When all children are dummy, the parent join is also marked as dummy
- The function modifies rel->nparts to 0 if partitionwise join fails, allowing later functions to handle it correctly
- Stack depth checking prevents issues with deeply nested partition hierarchies
- Consider_partitionwise_join flag must be set on the relation before calling

## Simplified Source

```c
void generate_partitionwise_join_paths(PlannerInfo *root, RelOptInfo *rel) {
    List *live_children = NIL;
    int cnt_parts, num_parts;
    RelOptInfo **part_rels;

    // Only process join relations that are partitioned
    if (!IS_JOIN_REL(rel) || !IS_PARTITIONED_REL(rel))
        return;

    // Guard against deep recursion
    check_stack_depth();

    num_parts = rel->nparts;
    part_rels = rel->part_rels;

    // Process each child partition
    for (cnt_parts = 0; cnt_parts < num_parts; cnt_parts++) {
        RelOptInfo *child_rel = part_rels[cnt_parts];

        // Skip pruned partitions
        if (child_rel == NULL)
            continue;

        // Recursively generate paths for child partition
        generate_partitionwise_join_paths(root, child_rel);

        // If child has no paths, abandon partitionwise join
        if (child_rel->pathlist == NIL) {
            rel->nparts = 0;  // Mark as unpartitioned
            return;
        }

        // Find cheapest path for this child
        set_cheapest(child_rel);

        // Collect non-dummy children for final append
        if (!IS_DUMMY_REL(child_rel))
            live_children = lappend(live_children, child_rel);
    }

    // Handle case where all children are dummy
    if (!live_children) {
        mark_dummy_rel(rel);
        return;
    }

    // Build append paths from live children
    add_paths_to_append_rel(root, rel, live_children);
    list_free(live_children);
}
```