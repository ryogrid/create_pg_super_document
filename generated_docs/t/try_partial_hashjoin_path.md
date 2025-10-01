# try_partial_hashjoin_path

## Location
[src/backend/optimizer/path/joinpath.c:1173-1233](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/joinpath.c#L1173-L1233)

## Overview
Considers a partial hashjoin join path and adds it to the joinrel's partial pathlist if it appears useful for parallel query execution.

## Definition

```c
static void
try_partial_hashjoin_path(PlannerInfo *root,
						  RelOptInfo *joinrel,
						  Path *outer_path,
						  Path *inner_path,
						  List *hashclauses,
						  JoinType jointype,
						  JoinPathExtraData *extra,
						  bool parallel_hash)
```
## Detailed Description
This function evaluates the feasibility of creating a partial hash join path for parallel query execution. The outer side must be partial, while the inner path's requirements depend on the  parameter. When  is true, the inner path must also be partial and will run in parallel to create shared hash tables. When false, the inner path must be complete and a copy runs in every process to create separate identical private hash tables.

The function performs several validation checks:
1. Ensures parameterized partial paths are not used (not supported)
2. Validates that inner path parameterization is satisfied by the outer path
3. Performs a quick cost estimation to avoid obviously poor choices
4. Creates and adds the hash join path if it passes all checks

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and configuration
- : RelOptInfo for the join relation being planned
- : Path for the outer side of the join (must be partial)
- : Path for the inner side of the join
- : List of hash join clauses
- : Type of join operation (INNER, LEFT, etc.)
- : Additional join path data and restrictions
- : Boolean indicating whether to use parallel hash tables

## Dependencies
- Functions called/Symbols referenced:
  - bms_is_empty
  - [initial_cost_hashjoin](../i/initial_cost_hashjoin.md)
  - [add_partial_path_precheck](../a/add_partial_path_precheck.md)
  - [add_partial_path](../a/add_partial_path.md)
  - [create_hashjoin_path](../c/create_hashjoin_path.md)
- Called from (representative examples):
  - [hash_inner_and_outer](../h/hash_inner_and_outer.md)

## Notes and Other Information
- This is a static function within joinpath.c, specifically designed for partial path planning
- Parameterized partial paths are explicitly not supported
- The function includes early bailout logic based on cost estimation to avoid expensive path creation for obviously poor choices
- The parallel_hash parameter determines the execution model: shared hash tables vs. replicated private hash tables
- Part of PostgreSQL's parallel query execution infrastructure for hash joins

## Simplified Source

```c
static void
try_partial_hashjoin_path(PlannerInfo *root,
                          RelOptInfo *joinrel,
                          Path *outer_path,
                          Path *inner_path,
                          List *hashclauses,
                          JoinType jointype,
                          JoinPathExtraData *extra,
                          bool parallel_hash)
{
    JoinCostWorkspace workspace;

    // Validate parameterization - partial paths cannot be parameterized
    if (inner_path->param_info != NULL)
    {
        Relids inner_paramrels = inner_path->param_info->ppi_req_outer;
        if (!bms_is_empty(inner_paramrels))
            return;
    }

    // Get quick cost estimate to eliminate obviously poor paths
    initial_cost_hashjoin(root, &workspace, jointype, hashclauses,
                          outer_path, inner_path, extra, parallel_hash);

    // Quick precheck to avoid expensive path creation
    if (!add_partial_path_precheck(joinrel, workspace.total_cost, NIL))
        return;

    // Create and add the partial hash join path
    add_partial_path(joinrel, (Path *)
                     create_hashjoin_path(root,
                                          joinrel,
                                          jointype,
                                          &workspace,
                                          extra,
                                          outer_path,
                                          inner_path,
                                          parallel_hash,
                                          extra->restrictlist,
                                          NULL,  // no required_outer for partial paths
                                          hashclauses));
}
```