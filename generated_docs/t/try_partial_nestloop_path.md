# try_partial_nestloop_path

## Location
[src/backend/optimizer/path/joinpath.c:843-919](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/joinpath.c#L843-L919)

## Overview
Evaluates and potentially adds a partial nestloop join path for parallel query execution to the joinrel's partial pathlist.

## Definition

```c
static void
try_partial_nestloop_path(PlannerInfo *root,
						  RelOptInfo *joinrel,
						  Path *outer_path,
						  Path *inner_path,
						  List *pathkeys,
						  JoinType jointype,
						  JoinPathExtraData *extra)
```
## Detailed Description
This function is specialized for creating partial nestloop join paths that can be executed in parallel. It performs validation specific to partial paths, including stricter parameterization requirements since parameterized partial paths are not supported. The function ensures that any inner path parameterization is fully satisfied by the outer path and validates that the path can be reparameterized if needed.

Unlike the regular nestloop path creation, this function is simpler as it doesn't need to handle complex parameterization scenarios that aren't supported in partial execution. It performs a quick cost estimation and uses add_partial_path_precheck for early elimination of poor paths.

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing global planning context and configuration
- `*joinrel`: Target RelOptInfo representing the join relation where the partial path will be added
- `*outer_path`: Path structure for the outer (driving) relation in the partial nestloop join
- `*inner_path`: Path structure for the inner (driven) relation in the partial nestloop join
- `*pathkeys`: List of PathKey structures representing the required output ordering
- `jointype`: JoinType enumeration specifying the type of join (INNER, LEFT, etc.)
- `*extra`: JoinPathExtraData containing additional join-specific information and constraints
## Dependencies
- Functions called/Symbols referenced:
  - bms_is_empty
  - [bms_is_subset](../b/bms_is_subset.md)
  - PATH_PARAM_BY_PARENT
  - path_is_reparameterizable_by_child
  - [initial_cost_nestloop](../i/initial_cost_nestloop.md)
  - [add_partial_path_precheck](../a/add_partial_path_precheck.md)
  - [add_partial_path](../a/add_partial_path.md)
  - [create_nestloop_path](../c/create_nestloop_path.md)
- Called from (representative examples):
  - [consider_parallel_nestloop](../c/consider_parallel_nestloop.md)

## Notes and Other Information
- Specifically designed for parallel query execution with stricter parameterization constraints
- Does not support parameterized partial paths - inner path parameterization must be fully satisfied by outer path
- Simpler validation logic compared to regular nestloop paths due to partial execution limitations
- Creates paths with NULL required_outer since partial paths cannot be parameterized
- Essential for enabling parallel nestloop joins in PostgreSQL's parallel query execution framework

## Simplified Source

```c
static void
try_partial_nestloop_path(PlannerInfo *root,
                          RelOptInfo *joinrel,
                          Path *outer_path,
                          Path *inner_path,
                          List *pathkeys,
                          JoinType jointype,
                          JoinPathExtraData *extra)
{
    JoinCostWorkspace workspace;

    // Verify no lateral dependencies (partial paths don't support this)
    Assert(bms_is_empty(joinrel->lateral_relids));

    // Check parameterization constraints for partial paths
    if (inner_path->param_info != NULL)
    {
        Relids inner_paramrels = inner_path->param_info->ppi_req_outer;
        RelOptInfo *outerrel = outer_path->parent;
        Relids outerrelids;

        // Use top-level parent relids for parameterization tests
        if (outerrel->top_parent_relids)
            outerrelids = outerrel->top_parent_relids;
        else
            outerrelids = outerrel->relids;

        // Inner parameterization must be fully satisfied by outer path
        if (!bms_is_subset(inner_paramrels, outerrelids))
            return;
    }

    // Check if parameterized inner path can be reparameterized
    if (PATH_PARAM_BY_PARENT(inner_path, outer_path->parent) &&
        !path_is_reparameterizable_by_child(inner_path, outer_path->parent))
        return;

    // Quick cost estimation to avoid expensive path creation
    initial_cost_nestloop(root, &workspace, jointype, outer_path, inner_path, extra);
    if (!add_partial_path_precheck(joinrel, workspace.total_cost, pathkeys))
        return;

    // Path looks promising, create and add it
    add_partial_path(joinrel, (Path *)
                     create_nestloop_path(root, joinrel, jointype, &workspace, extra,
                                           outer_path, inner_path, extra->restrictlist,
                                           pathkeys, NULL));
}
```