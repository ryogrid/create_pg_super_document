# create_nestloop_path

## Location
[src/backend/optimizer/util/pathnode.c:2457-2552](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L2457-L2552)

## Overview
Creates a path node corresponding to a nested loop join between two relations, handling parameterization and cost estimation for the join operation.

## Definition
```c
NestPath *create_nestloop_path(PlannerInfo *root,
                              RelOptInfo *joinrel,
                              JoinType jointype,
                              JoinCostWorkspace *workspace,
                              JoinPathExtraData *extra,
                              Path *outer_path,
                              Path *inner_path,
                              List *restrict_clauses,
                              List *pathkeys,
                              Relids required_outer)
```

## Detailed Description
This function creates a NestPath node representing a nested loop join operation. It handles complex parameterization scenarios where the inner path may be parameterized by the outer path, requiring careful management of restriction clauses that should be pushed down to the inner path rather than applied at the join level. The function sets up all path properties including cost estimates, parallel execution capabilities, and join-specific information. It also calls final_cost_nestloop to compute accurate cost estimates based on the workspace calculations.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing planner state and context
- `joinrel`: RelOptInfo representing the join relation being created
- `jointype`: Type of join (INNER, LEFT, RIGHT, FULL, etc.)
- `workspace`: Pre-computed cost workspace from initial_cost_nestloop
- `extra`: Additional join information including special join info and uniqueness
- `outer_path`: Path for the outer (left) side of the nested loop
- `inner_path`: Path for the inner (right) side of the nested loop
- `restrict_clauses`: List of RestrictInfo nodes to apply at the join
- `pathkeys`: Sort order specification for the join result
- `required_outer`: Set of relations that must be available as parameters

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create NestPath)
  - PATH_REQ_OUTER (to extract required outer relations)
  - [bms_overlap](../b/bms_overlap.md) (to check parameterization relationships)
  - [get_param_path_clause_serials](../g/get_param_path_clause_serials.md) (to identify enforced clauses)
  - [bms_is_member](../b/bms_is_member.md) (to check clause enforcement)
  - [get_joinrel_parampathinfo](../g/get_joinrel_parampathinfo.md) (to set up parameterization info)
  - [final_cost_nestloop](../f/final_cost_nestloop.md) (to compute final costs)
- Called from (representative examples):
  - [try_nestloop_path](../t/try_nestloop_path.md) (in joinpath.c)
  - [try_partial_nestloop_path](../t/try_partial_nestloop_path.md) (in joinpath.c)

## Notes and Other Information
- The function handles parameterized inner paths by removing restrict clauses already enforced in the inner path to avoid double-application
- Parallel execution support is determined by the parallel safety of both input paths and the join relation
- The parallel_workers estimate simply inherits from the outer path, which is noted as a simplistic approach
- Top-level parent relation IDs are used for parameterization tests to handle partitioned tables correctly
- The function distinguishes between clauses that should be applied at the join level versus those pushed to the inner path
- This is a core function in PostgreSQL's nested loop join planning, handling the most complex join scenarios with parameterization

## Simplified Source

```c
NestPath *
create_nestloop_path(PlannerInfo *root,
                     RelOptInfo *joinrel,
                     JoinType jointype,
                     JoinCostWorkspace *workspace,
                     JoinPathExtraData *extra,
                     Path *outer_path,
                     Path *inner_path,
                     List *restrict_clauses,
                     List *pathkeys,
                     Relids required_outer)
{
    NestPath *pathnode = makeNode(NestPath);
    Relids inner_req_outer = PATH_REQ_OUTER(inner_path);
    Relids outerrelids;

    // Use top-level parent relids for parameterization tests
    if (outer_path->parent->top_parent_relids)
        outerrelids = outer_path->parent->top_parent_relids;
    else
        outerrelids = outer_path->parent->relids;

    // Remove restrict clauses already enforced in parameterized inner path
    if (bms_overlap(inner_req_outer, outerrelids))
    {
        Bitmapset *enforced_serials = get_param_path_clause_serials(inner_path);
        List *jclauses = NIL;
        ListCell *lc;

        foreach(lc, restrict_clauses)
        {
            RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);
            if (!bms_is_member(rinfo->rinfo_serial, enforced_serials))
                jclauses = lappend(jclauses, rinfo);
        }
        restrict_clauses = jclauses;
    }

    // Initialize path node structure
    pathnode->jpath.path.pathtype = T_NestLoop;
    pathnode->jpath.path.parent = joinrel;
    pathnode->jpath.path.pathtarget = joinrel->reltarget;
    pathnode->jpath.path.param_info = get_joinrel_parampathinfo(root, joinrel,
                                                                outer_path, inner_path,
                                                                extra->sjinfo, required_outer,
                                                                &restrict_clauses);
    pathnode->jpath.path.parallel_safe = joinrel->consider_parallel &&
                                         outer_path->parallel_safe &&
                                         inner_path->parallel_safe;
    pathnode->jpath.path.parallel_workers = outer_path->parallel_workers;
    pathnode->jpath.path.pathkeys = pathkeys;

    // Set join-specific fields
    pathnode->jpath.jointype = jointype;
    pathnode->jpath.inner_unique = extra->inner_unique;
    pathnode->jpath.outerjoinpath = outer_path;
    pathnode->jpath.innerjoinpath = inner_path;
    pathnode->jpath.joinrestrictinfo = restrict_clauses;

    // Compute final costs
    final_cost_nestloop(root, pathnode, workspace, extra);

    return pathnode;
}
```