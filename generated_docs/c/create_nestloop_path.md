# create_nestloop_path

## Location
src/backend/optimizer/util/pathnode.c: 2457 - 2552

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
  - get_param_path_clause_serials (to identify enforced clauses)
  - [bms_is_member](../b/bms_is_member.md) (to check clause enforcement)
  - get_joinrel_parampathinfo (to set up parameterization info)
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