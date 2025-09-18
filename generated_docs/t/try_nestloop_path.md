# try_nestloop_path

## Location
src/backend/optimizer/path/joinpath.c: 721 - 842

## Overview
Evaluates and potentially adds a nestloop join path to the joinrel's pathlist, performing comprehensive validation checks and cost estimation before path creation.

## Definition


## Detailed Description
This function is the main entry point for considering a nestloop join strategy during query planning. It performs several crucial validation steps before creating and adding a nestloop path to the join relation. The function implements a two-phase optimization approach: first doing a quick precheck with cost estimation, then creating the full path structure only if the path shows promise.

The function validates parameterization constraints, checks for dangerous outer join references, ensures reparameterizability requirements are met, and performs initial cost estimation. It uses the add_path_precheck mechanism to quickly eliminate obviously inferior paths before the expensive path creation step.

## Parameters / Member Variables
- : PlannerInfo structure containing global planning context and configuration
- : Target RelOptInfo representing the join relation where the path will be added
- : Path structure for the outer (driving) relation in the nestloop join
- : Path structure for the inner (driven) relation in the nestloop join
- : List of PathKey structures representing the required output ordering
- : JoinType enumeration specifying the type of join (INNER, LEFT, etc.)
- : JoinPathExtraData containing additional join-specific information and constraints

## Dependencies
- Functions called/Symbols referenced:
  - [bms_is_member](../b/bms_is_member.md)
  - [calc_nestloop_required_outer](../c/calc_nestloop_required_outer.md)
  - [bms_overlap](../b/bms_overlap.md)
  - [allow_star_schema_join](../a/allow_star_schema_join.md)
  - [have_dangerous_phv](../h/have_dangerous_phv.md)
  - [have_unsafe_outer_join_ref](../h/have_unsafe_outer_join_ref.md)
  - PATH_PARAM_BY_PARENT
  - path_is_reparameterizable_by_child
  - [initial_cost_nestloop](../i/initial_cost_nestloop.md)
  - [add_path_precheck](../a/add_path_precheck.md)
  - [create_nestloop_path](../c/create_nestloop_path.md)
  - [add_path](../a/add_path.md)
  - [bms_free](../b/bms_free.md)
- Called from (representative examples):
  - [match_unsorted_outer](../m/match_unsorted_outer.md)

## Notes and Other Information
- The function implements sophisticated parameterization validation to ensure the resulting join path is valid and efficient
- Uses a two-phase approach for performance: quick cost estimation followed by full path creation only for promising paths
- Handles complex scenarios involving outer joins and ensures no unsafe outer join references exist
- Manages memory efficiently by freeing bitmapsets when paths are rejected
- Critical for the query optimizer's ability to generate efficient nestloop join execution plans