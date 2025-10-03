# try_partial_mergejoin_path

## Location
[src/backend/optimizer/path/joinpath.c:1026-1095](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/joinpath.c#L1026-L1095)

## Overview
Evaluates and potentially adds a partial merge join path for parallel query execution to the joinrel's partial pathlist with simplified parameterization constraints.

## Definition

```c
static void
try_partial_mergejoin_path(PlannerInfo *root,
						   RelOptInfo *joinrel,
						   Path *outer_path,
						   Path *inner_path,
						   List *pathkeys,
						   List *mergeclauses,
						   List *outersortkeys,
						   List *innersortkeys,
						   JoinType jointype,
						   JoinPathExtraData *extra)
```
## Detailed Description
This function is specialized for creating partial merge join paths that can be executed in parallel. It implements stricter parameterization requirements than regular merge joins, rejecting any inner path that has parameterization since parameterized partial paths are not supported. The function optimizes sort operations by checking if input paths are already appropriately ordered and skipping explicit sorting when possible.

Like other partial path functions, it uses simplified validation logic due to the constraints of parallel execution. The function performs initial cost estimation and uses add_partial_path_precheck for early elimination of poor paths before creating the full path structure.

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing global planning context and configuration
- `*joinrel`: Target RelOptInfo representing the join relation where the partial path will be added
- `*outer_path`: Path structure for the outer (left) relation in the partial merge join
- `*inner_path`: Path structure for the inner (right) relation in the partial merge join
- `*pathkeys`: List of PathKey structures representing the required output ordering
- `*mergeclauses`: List of merge join clauses that define the join conditions
- `*outersortkeys`: List of PathKey structures for required outer relation sorting (NULL if no sort needed)
- `*innersortkeys`: List of PathKey structures for required inner relation sorting (NULL if no sort needed)
- `jointype`: JoinType enumeration specifying the type of join (INNER, LEFT, etc.)
- `*extra`: JoinPathExtraData containing additional join-specific information and constraints
## Dependencies
- Functions called/Symbols referenced:
  - bms_is_empty
  - [pathkeys_contained_in](../p/pathkeys_contained_in.md)
  - [initial_cost_mergejoin](../i/initial_cost_mergejoin.md)
  - [add_partial_path_precheck](../a/add_partial_path_precheck.md)
  - [add_partial_path](../a/add_partial_path.md)
  - [create_mergejoin_path](../c/create_mergejoin_path.md)
- Called from (representative examples):
  - [try_mergejoin_path](try_mergejoin_path.md)
  - [sort_inner_and_outer](../s/sort_inner_and_outer.md)

## Notes and Other Information
- Specifically designed for parallel query execution with no support for parameterized inner paths
- Optimizes performance by skipping explicit sorts when input paths are already appropriately ordered
- Creates paths with NULL required_outer since partial paths cannot be parameterized
- Simpler validation logic compared to regular merge join paths due to parallel execution constraints
- Essential component of PostgreSQL's parallel merge join capability in the query optimizer

## Simplified Source

```c
static void try_partial_mergejoin_path(PlannerInfo *root,
                                      RelOptInfo *joinrel,
                                      Path *outer_path,
                                      Path *inner_path,
                                      List *pathkeys,
                                      List *mergeclauses,
                                      List *outersortkeys,
                                      List *innersortkeys,
                                      JoinType jointype,
                                      JoinPathExtraData *extra)
{
    // Validate parallel execution constraints
    Assert(bms_is_empty(joinrel->lateral_relids));

    // Reject parameterized inner paths (not supported in partial joins)
    if (inner_path->param_info != NULL)
    {
        Relids inner_paramrels = inner_path->param_info->ppi_req_outer;
        if (!bms_is_empty(inner_paramrels))
            return;
    }

    // Optimize sorting - skip if paths are already ordered
    if (outersortkeys &&
        pathkeys_contained_in(outersortkeys, outer_path->pathkeys))
        outersortkeys = NIL;
    if (innersortkeys &&
        pathkeys_contained_in(innersortkeys, inner_path->pathkeys))
        innersortkeys = NIL;

    // Get initial cost estimate
    JoinCostWorkspace workspace;
    initial_cost_mergejoin(root, &workspace, jointype, mergeclauses,
                          outer_path, inner_path, outersortkeys, innersortkeys, extra);

    // Early cost-based elimination
    if (!add_partial_path_precheck(joinrel, workspace.total_cost, pathkeys))
        return;

    // Create and add the partial merge join path
    add_partial_path(joinrel, (Path *)
                    create_mergejoin_path(root, joinrel, jointype, &workspace, extra,
                                         outer_path, inner_path, extra->restrictlist,
                                         pathkeys, NULL, mergeclauses,
                                         outersortkeys, innersortkeys));
}
```