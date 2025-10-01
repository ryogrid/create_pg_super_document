# get_loop_count

## Location
[src/backend/optimizer/path/indxpath.c:1829-1881](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/indxpath.c#L1829-L1881)

## Overview
Estimates the loop iteration count for costing parameterized index paths by finding the smallest row count among outer relations.

## Definition
```c
static double get_loop_count(PlannerInfo *root, Index cur_relid, Relids outer_relids)
```

## Detailed Description
This function provides a heuristic estimate for how many times a parameterized path will be executed in a nested loop join. Since parameterized paths are generated before join relations are created, the exact iteration count cannot be determined precisely. The function implements a conservative approach by:

1. Examining all outer relations referenced in the parameterized path
2. Calculating row counts for each outer relation, accounting for semijoins
3. Returning the smallest row count as the loop count estimate

This heuristic assumes the nested loop will be driven by the smallest outer relation, which provides a reasonable approximation for costing purposes. The function also handles edge cases like empty relations and adjusts for semijoin effects where relations may be unique-ified.

## Parameters / Member Variables
- `root`: PlannerInfo containing global planning context and relation information
- `cur_relid`: Index of the current relation being processed
- `outer_relids`: Bitmap set of relation IDs that parameterize this path

## Dependencies
- Functions called/Symbols referenced:
  - [bms_next_member](../b/bms_next_member.md) (to iterate through outer relation IDs)
  - IS_DUMMY_REL (to check if a relation is proven empty)
  - [adjust_rowcount_for_semijoins](../a/adjust_rowcount_for_semijoins.md) (to account for semijoin effects on row counts)
- Called from (representative examples):
  - ec_member_matches_arg
  - [create_index_paths](../c/create_index_paths.md)
  - [build_index_paths](../b/build_index_paths.md)
  - [bitmap_scan_cost_est](../b/bitmap_scan_cost_est.md)

## Notes and Other Information
- Returns 1.0 for non-parameterized paths (when outer_relids is NULL)
- Uses the smallest outer relation's row count as a conservative estimate
- Accounts for semijoin unique-ification effects through adjust_rowcount_for_semijoins
- Requires that base relation size estimates be established before path computation begins
- Falls back to 1.0 if no valid outer relations are found (defensive programming)

## Simplified Source

```c
static double
get_loop_count(PlannerInfo *root, Index cur_relid, Relids outer_relids)
{
    double result;
    int outer_relid;

    // Non-parameterized path: executed once
    if (outer_relids == NULL)
        return 1.0;

    result = 0.0;
    outer_relid = -1;

    // Find smallest row count among outer relations
    while ((outer_relid = bms_next_member(outer_relids, outer_relid)) >= 0)
    {
        RelOptInfo *outer_rel;
        double rowcount;

        // Skip invalid relation indexes
        if (outer_relid >= root->simple_rel_array_size)
            continue;
        outer_rel = root->simple_rel_array[outer_relid];
        if (outer_rel == NULL)
            continue;

        // Skip empty relations
        if (IS_DUMMY_REL(outer_rel))
            continue;

        // Adjust for semijoin effects - use unique-ified count if needed
        rowcount = adjust_rowcount_for_semijoins(root,
                                               cur_relid,
                                               outer_relid,
                                               outer_rel->rows);

        // Keep track of smallest row count (conservative estimate)
        if (result == 0.0 || result > rowcount)
            result = rowcount;
    }

    // Fallback if no valid relations found
    return (result > 0.0) ? result : 1.0;
}
```