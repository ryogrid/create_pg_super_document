# add_paths_to_joinrel

## Location
src/backend/optimizer/path/joinpath.c: 124 - 362

## Overview
Considers all possible join paths between two component relations and adds the best paths to the join relation's pathlist, serving as the main driver for join path generation in PostgreSQL's query optimizer.

## Definition


## Detailed Description
This function is the central hub for generating all types of join paths between two relations in PostgreSQL's cost-based optimizer. It systematically evaluates different join algorithms (nested loop, merge join, hash join) and path configurations to find the most efficient ways to combine the outer and inner relations.

The function performs several key operations:
1. Determines if the inner relation is provably unique for cost estimation optimizations
2. Identifies potential merge join clauses when merge joins are enabled
3. Computes correction factors for semi/anti joins and unique joins
4. Establishes parameterization constraints based on join ordering restrictions
5. Generates paths using various join algorithms (sort-merge, nested loop, hash)
6. Allows foreign data wrappers and extensions to contribute additional paths

The function handles special join types including semi-joins, anti-joins, and unique joins with appropriate logic for each case.

## Parameters / Member Variables
- : PlannerInfo structure containing global optimizer state and configuration
- : Target RelOptInfo representing the result of joining outerrel and innerrel
- : RelOptInfo for the outer (left) side of the join
- : RelOptInfo for the inner (right) side of the join  
- : JoinType specifying the type of join (INNER, LEFT, RIGHT, FULL, SEMI, ANTI, etc.)
- : SpecialJoinInfo containing join ordering constraints and metadata
- : List of RestrictInfo nodes representing join conditions

## Dependencies
- Functions called/Symbols referenced:
  - innerrel_is_unique
  - select_mergejoin_clauses
  - compute_semi_anti_join_factors
  - sort_inner_and_outer
  - match_unsorted_outer
  - hash_inner_and_outer
  - bms_is_subset
  - bms_overlap
  - bms_join
  - bms_difference
  - bms_add_members
- Called from (representative examples):
  - populate_joinrel_with_paths

## Notes and Other Information
The function supports special JoinTypes JOIN_UNIQUE_OUTER and JOIN_UNIQUE_INNER which indicate that a relation should be unique-ified before applying a regular inner join. These values are internal to this module and don't propagate outside.

The function includes logic to handle partitioned tables by using top_parent_relids for RELOPT_OTHER_JOINREL relations. It also considers LATERAL subquery dependencies when determining parameterization constraints.

For full outer joins, the function overrides disabled join methods (merge join, hash join) since they may be the only feasible implementation approach.