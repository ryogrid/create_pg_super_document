# try_mergejoin_path

## Location
[src/backend/optimizer/path/joinpath.c:920-1025](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/joinpath.c#L920-L1025)

## Overview
Evaluates and potentially adds a merge join path to the joinrel's pathlist, handling both regular and partial execution modes with comprehensive validation and cost estimation.

## Definition

```c
static void
try_mergejoin_path(PlannerInfo *root,
				   RelOptInfo *joinrel,
				   Path *outer_path,
				   Path *inner_path,
				   List *pathkeys,
				   List *mergeclauses,
				   List *outersortkeys,
				   List *innersortkeys,
				   JoinType jointype,
				   JoinPathExtraData *extra,
				   bool is_partial)
```
## Detailed Description
This function serves as the main entry point for considering merge join strategies during query planning. It handles both regular and partial execution modes, delegating partial merge joins to try_partial_mergejoin_path when appropriate. The function performs validation checks specific to merge joins, including parameterization validation and sort key optimization.

Key features include checking if explicit sorting can be skipped when input paths are already appropriately ordered, validating parameterization constraints for non-nestloop joins, and using the two-phase optimization approach with initial cost estimation followed by full path creation only for promising candidates.

## Parameters / Member Variables
- : PlannerInfo structure containing global planning context and configuration
- : Target RelOptInfo representing the join relation where the path will be added
- : Path structure for the outer (left) relation in the merge join
- : Path structure for the inner (right) relation in the merge join
- : List of PathKey structures representing the required output ordering
- : List of merge join clauses that define the join conditions
- : List of PathKey structures for required outer relation sorting (NULL if no sort needed)
- : List of PathKey structures for required inner relation sorting (NULL if no sort needed)
- : JoinType enumeration specifying the type of join (INNER, LEFT, etc.)
- : JoinPathExtraData containing additional join-specific information and constraints
- : Boolean flag indicating whether to create a partial path for parallel execution

## Dependencies
- Functions called/Symbols referenced:
  - [try_partial_mergejoin_path](try_partial_mergejoin_path.md)
  - [bms_is_member](../b/bms_is_member.md)
  - PATH_REQ_OUTER
  - [calc_non_nestloop_required_outer](../c/calc_non_nestloop_required_outer.md)
  - [bms_overlap](../b/bms_overlap.md)
  - [pathkeys_contained_in](../p/pathkeys_contained_in.md)
  - [initial_cost_mergejoin](../i/initial_cost_mergejoin.md)
  - [add_path_precheck](../a/add_path_precheck.md)
  - [create_mergejoin_path](../c/create_mergejoin_path.md)
  - [add_path](../a/add_path.md)
  - [bms_free](../b/bms_free.md)
- Called from (representative examples):
  - [sort_inner_and_outer](../s/sort_inner_and_outer.md)
  - [generate_mergejoin_paths](../g/generate_mergejoin_paths.md)

## Notes and Other Information
- Optimizes sort operations by checking if input paths are already appropriately ordered
- Uses calc_non_nestloop_required_outer for parameterization validation specific to non-nestloop joins
- Delegates to try_partial_mergejoin_path for partial execution modes to handle parallel-specific constraints
- Implements the same two-phase cost optimization approach as other join path functions
- Critical for generating efficient merge join execution plans, especially when input relations are pre-sorted