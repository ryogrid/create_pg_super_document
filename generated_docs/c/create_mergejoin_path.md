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
- `*root`: PlannerInfo structure containing global planning information
- `*joinrel`: RelOptInfo representing the result relation of the join
- `jointype`: Type of join operation (inner, left outer, etc.)
- `*workspace`: Pre-computed cost workspace from initial_cost_mergejoin
- `*extra`: Additional join-specific information and flags
- `*outer_path`: Path representing the outer (left) input relation
- `*inner_path`: Path representing the inner (right) input relation
- `*restrict_clauses`: List of RestrictInfo nodes for join conditions
- `*pathkeys`: Ordering specification for the resulting path
- `required_outer`: Set of outer relations required for parameterized plans
- `*mergeclauses`: Subset of restrict_clauses used as merge conditions
- `*outersortkeys`: Sort keys needed for the outer relation
- `*innersortkeys`: Sort keys needed for the inner relation
## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - [get_joinrel_parampathinfo](../g/get_joinrel_parampathinfo.md)
  - [final_cost_mergejoin](../f/final_cost_mergejoin.md)
- Called from (representative examples):
  - [try_mergejoin_path](../t/try_mergejoin_path.md)
  - [try_partial_mergejoin_path](../t/try_partial_mergejoin_path.md)

## Notes and Other Information
The function sets up the basic MergePath structure but defers final cost calculation to final_cost_mergejoin. Some fields like skip_mark_restore and materialize_inner are set later during cost calculation. The parallel_workers estimation uses a simple heuristic that copies from the outer path, which the code comments acknowledge as suboptimal.

## Simplified Source

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
{
    MergePath *pathnode = makeNode(MergePath);

    // Initialize basic path properties
    pathnode->jpath.path.pathtype = T_MergeJoin;
    pathnode->jpath.path.parent = joinrel;
    pathnode->jpath.path.pathtarget = joinrel->reltarget;
    pathnode->jpath.path.param_info = get_joinrel_parampathinfo(root, joinrel,
                                                                outer_path, inner_path,
                                                                extra->sjinfo, required_outer,
                                                                &restrict_clauses);

    // Set parallel execution properties
    pathnode->jpath.path.parallel_safe = joinrel->consider_parallel &&
                                         outer_path->parallel_safe &&
                                         inner_path->parallel_safe;
    pathnode->jpath.path.parallel_workers = outer_path->parallel_workers;
    pathnode->jpath.path.pathkeys = pathkeys;

    // Set join-specific properties
    pathnode->jpath.jointype = jointype;
    pathnode->jpath.inner_unique = extra->inner_unique;
    pathnode->jpath.outerjoinpath = outer_path;
    pathnode->jpath.innerjoinpath = inner_path;
    pathnode->jpath.joinrestrictinfo = restrict_clauses;

    // Set merge-specific properties
    pathnode->path_mergeclauses = mergeclauses;
    pathnode->outersortkeys = outersortkeys;
    pathnode->innersortkeys = innersortkeys;
    // Note: skip_mark_restore and materialize_inner set by final_cost_mergejoin

    // Compute final costs and optimization flags
    final_cost_mergejoin(root, pathnode, workspace, extra);

    return pathnode;
}
```