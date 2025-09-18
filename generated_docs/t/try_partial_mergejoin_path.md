# try_partial_mergejoin_path

## Location
src/backend/optimizer/path/joinpath.c: 1026 - 1095

## Overview
Evaluates and potentially adds a partial merge join path for parallel query execution to the joinrel's partial pathlist with simplified parameterization constraints.

## Definition


## Detailed Description
This function is specialized for creating partial merge join paths that can be executed in parallel. It implements stricter parameterization requirements than regular merge joins, rejecting any inner path that has parameterization since parameterized partial paths are not supported. The function optimizes sort operations by checking if input paths are already appropriately ordered and skipping explicit sorting when possible.

Like other partial path functions, it uses simplified validation logic due to the constraints of parallel execution. The function performs initial cost estimation and uses add_partial_path_precheck for early elimination of poor paths before creating the full path structure.

## Parameters / Member Variables
- : PlannerInfo structure containing global planning context and configuration
- : Target RelOptInfo representing the join relation where the partial path will be added
- : Path structure for the outer (left) relation in the partial merge join
- : Path structure for the inner (right) relation in the partial merge join
- : List of PathKey structures representing the required output ordering
- : List of merge join clauses that define the join conditions
- : List of PathKey structures for required outer relation sorting (NULL if no sort needed)
- : List of PathKey structures for required inner relation sorting (NULL if no sort needed)
- : JoinType enumeration specifying the type of join (INNER, LEFT, etc.)
- : JoinPathExtraData containing additional join-specific information and constraints

## Dependencies
- Functions called/Symbols referenced:
  - bms_is_empty
  - pathkeys_contained_in
  - initial_cost_mergejoin
  - add_partial_path_precheck
  - add_partial_path
  - create_mergejoin_path
- Called from (representative examples):
  - try_mergejoin_path
  - sort_inner_and_outer

## Notes and Other Information
- Specifically designed for parallel query execution with no support for parameterized inner paths
- Optimizes performance by skipping explicit sorts when input paths are already appropriately ordered
- Creates paths with NULL required_outer since partial paths cannot be parameterized
- Simpler validation logic compared to regular merge join paths due to parallel execution constraints
- Essential component of PostgreSQL's parallel merge join capability in the query optimizer