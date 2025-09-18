# try_partial_hashjoin_path

## Location
src/backend/optimizer/path/joinpath.c: 1173 - 1233

## Overview
Considers a partial hashjoin join path and adds it to the joinrel's partial pathlist if it appears useful for parallel query execution.

## Definition


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