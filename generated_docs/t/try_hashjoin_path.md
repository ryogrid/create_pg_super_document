# try_hashjoin_path

## Location
[src/backend/optimizer/path/joinpath.c:1096-1172](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/joinpath.c#L1096-L1172)

## Overview
Evaluates and potentially adds a hash join path to the joinrel's pathlist, performing validation checks and cost estimation for hash-based join strategies.

## Definition

```c
static void
try_hashjoin_path(PlannerInfo *root,
				  RelOptInfo *joinrel,
				  Path *outer_path,
				  Path *inner_path,
				  List *hashclauses,
				  JoinType jointype,
				  JoinPathExtraData *extra)
```
## Detailed Description
This function is the main entry point for considering hash join strategies during query planning. It performs validation checks specific to hash joins, including parameterization validation using calc_non_nestloop_required_outer, and ensures that outer join parameterization constraints are met. The function implements the standard two-phase optimization approach with initial cost estimation followed by full path creation only for promising candidates.

A key characteristic of hash joins is that they never produce any output pathkeys (sorted output), which is reflected in the NIL pathkeys parameter passed to add_path_precheck. The function creates non-parallel hash join paths, with parallel hash join functionality handled by separate mechanisms.

## Parameters / Member Variables
- : PlannerInfo structure containing global planning context and configuration
- : Target RelOptInfo representing the join relation where the path will be added
- : Path structure for the outer (build) relation in the hash join
- : Path structure for the inner (probe) relation in the hash join
- : List of hash join clauses that define the equijoin conditions for hashing
- : JoinType enumeration specifying the type of join (INNER, LEFT, etc.)
- : JoinPathExtraData containing additional join-specific information and constraints

## Dependencies
- Functions called/Symbols referenced:
  - [bms_is_member](../b/bms_is_member.md)
  - PATH_REQ_OUTER
  - [calc_non_nestloop_required_outer](../c/calc_non_nestloop_required_outer.md)
  - [bms_overlap](../b/bms_overlap.md)
  - [initial_cost_hashjoin](../i/initial_cost_hashjoin.md)
  - [add_path_precheck](../a/add_path_precheck.md)
  - [create_hashjoin_path](../c/create_hashjoin_path.md)
  - [add_path](../a/add_path.md)
  - [bms_free](../b/bms_free.md)
- Called from (representative examples):
  - [hash_inner_and_outer](../h/hash_inner_and_outer.md)

## Notes and Other Information
- [Hash](../H/Hash.md) joins never produce sorted output, so pathkeys are always NIL for add_path_precheck
- Uses calc_non_nestloop_required_outer for parameterization validation, same as merge joins
- Creates non-parallel hash join paths only; parallel hash joins are handled separately
- The parallel_hash parameter is explicitly set to false in create_hashjoin_path call
- Essential for generating hash join execution plans, particularly efficient for large equijoins
- Performs the same outer join parameterization validation as other join types

## Simplified Source

```c
static void
try_hashjoin_path(PlannerInfo *root,
                  RelOptInfo *joinrel,
                  Path *outer_path,
                  Path *inner_path,
                  List *hashclauses,
                  JoinType jointype,
                  JoinPathExtraData *extra)
{
    Relids required_outer;
    JoinCostWorkspace workspace;

    // Reject if outer join parameterization is invalid
    if (extra->sjinfo->ojrelid != 0 &&
        (bms_is_member(extra->sjinfo->ojrelid, PATH_REQ_OUTER(inner_path)) ||
         bms_is_member(extra->sjinfo->ojrelid, PATH_REQ_OUTER(outer_path))))
        return;

    // Calculate required outer relations for parameterization
    required_outer = calc_non_nestloop_required_outer(outer_path, inner_path);

    // Reject if parameterization doesn't make sense
    if (required_outer &&
        !bms_overlap(required_outer, extra->param_source_rels))
    {
        bms_free(required_outer);
        return;
    }

    // Get initial cost estimate for hash join
    initial_cost_hashjoin(root, &workspace, jointype, hashclauses,
                          outer_path, inner_path, extra, false);

    // Check if this path is worth considering before creating full path
    if (add_path_precheck(joinrel,
                          workspace.startup_cost, workspace.total_cost,
                          NIL, required_outer))  // Hash joins have no pathkeys
    {
        // Create and add the hash join path
        add_path(joinrel, (Path *)
                 create_hashjoin_path(root,
                                      joinrel,
                                      jointype,
                                      &workspace,
                                      extra,
                                      outer_path,
                                      inner_path,
                                      false,    /* parallel_hash */
                                      extra->restrictlist,
                                      required_outer,
                                      hashclauses));
    }
    else
    {
        bms_free(required_outer);
    }
}
```