# add_paths_to_joinrel

## Location
[src/backend/optimizer/path/joinpath.c:124-362](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/joinpath.c#L124-L362)

## Overview
Considers all possible join paths between two component relations and adds the best paths to the join relation's pathlist, serving as the main driver for join path generation in PostgreSQL's query optimizer.

## Definition

```c
void
add_paths_to_joinrel(PlannerInfo *root,
					 RelOptInfo *joinrel,
					 RelOptInfo *outerrel,
					 RelOptInfo *innerrel,
					 JoinType jointype,
					 SpecialJoinInfo *sjinfo,
					 List *restrictlist)
```
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
  - [innerrel_is_unique](../i/innerrel_is_unique.md)
  - [select_mergejoin_clauses](../s/select_mergejoin_clauses.md)
  - [compute_semi_anti_join_factors](../c/compute_semi_anti_join_factors.md)
  - [sort_inner_and_outer](../s/sort_inner_and_outer.md)
  - [match_unsorted_outer](../m/match_unsorted_outer.md)
  - [hash_inner_and_outer](../h/hash_inner_and_outer.md)
  - [bms_is_subset](../b/bms_is_subset.md)
  - [bms_overlap](../b/bms_overlap.md)
  - [bms_join](../b/bms_join.md)
  - [bms_difference](../b/bms_difference.md)
  - [bms_add_members](../b/bms_add_members.md)
- Called from (representative examples):
  - [populate_joinrel_with_paths](../p/populate_joinrel_with_paths.md)

## Notes and Other Information
The function supports special JoinTypes JOIN_UNIQUE_OUTER and JOIN_UNIQUE_INNER which indicate that a relation should be unique-ified before applying a regular inner join. These values are internal to this module and don't propagate outside.

The function includes logic to handle partitioned tables by using top_parent_relids for RELOPT_OTHER_JOINREL relations. It also considers LATERAL subquery dependencies when determining parameterization constraints.

For full outer joins, the function overrides disabled join methods (merge join, hash join) since they may be the only feasible implementation approach.