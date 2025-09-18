# try_partial_nestloop_path

## Location
src/backend/optimizer/path/joinpath.c: 843 - 919

## Overview
Evaluates and potentially adds a partial nestloop join path for parallel query execution to the joinrel's partial pathlist.

## Definition


## Detailed Description
This function is specialized for creating partial nestloop join paths that can be executed in parallel. It performs validation specific to partial paths, including stricter parameterization requirements since parameterized partial paths are not supported. The function ensures that any inner path parameterization is fully satisfied by the outer path and validates that the path can be reparameterized if needed.

Unlike the regular nestloop path creation, this function is simpler as it doesn't need to handle complex parameterization scenarios that aren't supported in partial execution. It performs a quick cost estimation and uses add_partial_path_precheck for early elimination of poor paths.

## Parameters / Member Variables
- : PlannerInfo structure containing global planning context and configuration
- : Target RelOptInfo representing the join relation where the partial path will be added
- : Path structure for the outer (driving) relation in the partial nestloop join
- : Path structure for the inner (driven) relation in the partial nestloop join
- : List of PathKey structures representing the required output ordering
- : JoinType enumeration specifying the type of join (INNER, LEFT, etc.)
- : JoinPathExtraData containing additional join-specific information and constraints

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