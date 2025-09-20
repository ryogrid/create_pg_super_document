# create_mergejoin_path

## Location
[src/backend/optimizer/util/pathnode.c:2553-2618](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L2553-L2618)

## Overview
Creates a pathnode corresponding to a mergejoin between two relations, setting up all necessary metadata and cost information for the PostgreSQL query optimizer.

## Definition

```c
MergePath *
create_mergejoin_path(PlannerInfo *root,
					  RelOptInfo *joinrel,
					  JoinType jointype,
					  JoinCostWorkspace *workspace,
					  JoinPathExtraData *extra,
					  Path *outer_path,
					  Path *inner_path,
					  List *restrict_clauses,
					  List *pathkeys,
					  Relids required_outer,
					  List *mergeclauses,
					  List *outersortkeys,
					  List *innersortkeys)
```
## Detailed Description
This function constructs a MergePath node that represents a merge join execution plan. Merge joins are used when both input relations can be sorted on the join keys, allowing for an efficient merge operation. The function initializes all path metadata including cost estimates, parallelism settings, and join-specific information. It calls final_cost_mergejoin to compute accurate cost estimates based on the provided workspace and extra data.

## Parameters / Member Variables
- : PlannerInfo structure containing global planning information
- : RelOptInfo representing the result relation of the join
- : Type of join operation (inner, left outer, etc.)
- : Pre-computed cost workspace from initial_cost_mergejoin
- : Additional join-specific information and flags
- : Path representing the outer (left) input relation
- : Path representing the inner (right) input relation  
- : List of RestrictInfo nodes for join conditions
- : Ordering specification for the resulting path
- : Set of outer relations required for parameterized plans
- : Subset of restrict_clauses used as merge conditions
- : Sort keys needed for the outer relation
- : Sort keys needed for the inner relation

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - get_joinrel_parampathinfo
  - [final_cost_mergejoin](../f/final_cost_mergejoin.md)
- Called from (representative examples):
  - [try_mergejoin_path](../t/try_mergejoin_path.md)
  - [try_partial_mergejoin_path](../t/try_partial_mergejoin_path.md)

## Notes and Other Information
The function sets up the basic MergePath structure but defers final cost calculation to final_cost_mergejoin. Some fields like skip_mark_restore and materialize_inner are set later during cost calculation. The parallel_workers estimation uses a simple heuristic that copies from the outer path, which the code comments acknowledge as suboptimal.