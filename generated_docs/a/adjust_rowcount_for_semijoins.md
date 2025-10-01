# adjust_rowcount_for_semijoins

## Location
[src/backend/optimizer/path/indxpath.c:1882-1925](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/indxpath.c#L1882-L1925)

## Overview
Adjusts row count estimates for relations that participate in semijoins by accounting for unique-ification effects.

## Definition
```c
static double adjust_rowcount_for_semijoins(PlannerInfo *root, Index cur_relid, Index outer_relid, double rowcount)
```

## Detailed Description
This function examines whether a relation is on the inside (right-hand side) of any semijoin where another relation is on the outside (left-hand side). In semijoin operations, the right-hand side relation effectively gets unique-ified, meaning duplicate values are eliminated when joining. 

The function iterates through all special joins in the query, identifies semijoins where the current relation is on the left and the outer relation is on the right, and estimates the number of unique rows that would result. If this unique count is smaller than the original row count, it replaces the row count with the more accurate estimate.

This adjustment is crucial for accurate cost estimation in parameterized paths, as using the raw row count would overestimate costs when semijoin unique-ification reduces the effective number of iterations.

## Parameters / Member Variables
- `root`: PlannerInfo containing global planning context and join information
- `cur_relid`: Index of the current relation (left side of potential semijoin)
- `outer_relid`: Index of the outer relation (right side of potential semijoin)  
- `rowcount`: Original row count estimate to potentially adjust

## Dependencies
- Functions called/Symbols referenced:
  - [bms_is_member](../b/bms_is_member.md) (to check relation membership in join sides)
  - [approximate_joinrel_size](approximate_joinrel_size.md) (to estimate raw size of semijoin RHS)
  - [estimate_num_groups](../e/estimate_num_groups.md) (to estimate number of unique groups after semijoin)
  - JOIN_SEMI (constant for semijoin type identification)
  - [SpecialJoinInfo](../S/SpecialJoinInfo.md) (struct containing join metadata)
- Called from (representative examples):
  - ec_member_matches_arg
  - [get_loop_count](../g/get_loop_count.md)

## Notes and Other Information
- Only processes semijoins (JOIN_SEMI) from the join_info_list
- Uses crude but reasonable estimates given the early stage of planning
- Takes the minimum of original rowcount and estimated unique count
- Critical for accurate costing of parameterized paths involving semijoins
- Helps prevent overestimation of nested loop iteration costs

## Simplified Source

```c
static double
adjust_rowcount_for_semijoins(PlannerInfo *root, Index cur_relid,
                              Index outer_relid, double rowcount)
{
    ListCell *lc;

    // Check each special join in the query
    foreach(lc, root->join_info_list) {
        SpecialJoinInfo *sjinfo = (SpecialJoinInfo *) lfirst(lc);

        // Look for semijoins where cur_relid is on left, outer_relid on right
        if (sjinfo->jointype == JOIN_SEMI &&
            bms_is_member(cur_relid, sjinfo->syn_lefthand) &&
            bms_is_member(outer_relid, sjinfo->syn_righthand)) {

            // Estimate unique-ified rows from semijoin RHS
            double raw_rows = approximate_joinrel_size(root, sjinfo->syn_righthand);
            double unique_rows = estimate_num_groups(root, sjinfo->semi_rhs_exprs,
                                                   raw_rows, NULL, NULL);

            // Use the smaller estimate (semijoin reduces rows)
            if (rowcount > unique_rows)
                rowcount = unique_rows;
        }
    }

    return rowcount;
}
```