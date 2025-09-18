# is_parallel_safe

## Location
[src/backend/optimizer/util/clauses.c:753-793](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L753-L793)

## Overview
Determines whether a given expression contains only parallel-safe functions, enabling the PostgreSQL query planner to decide if an expression can be safely executed in parallel workers.

## Definition


## Detailed Description
The  function analyzes an expression tree to determine if it contains only functions and operations that are safe to execute in parallel worker processes. This is a critical optimization component that enables PostgreSQL to parallelize query execution while maintaining correctness.

The function performs an optimized check by first examining the global parallel hazard level (). If the entire query was already determined to be parallel-safe and no PARAM_EXEC parameters were generated during planning, it can immediately return true without traversing the expression tree.

When a deeper analysis is required, the function sets up a context for hazard detection and uses the  to traverse the expression tree. It specifically looks for parallel-restricted or parallel-unsafe operations, stopping as soon as any are found.

The function also handles parameter safety by building a list of 'safe' parameter IDs from init plans at the current and parent query levels, as these parameters are computed at Gather nodes and passed to workers.

## Parameters / Member Variables
- : PlannerInfo structure containing planning context and global parallel hazard information
- : Expression tree node to analyze for parallel safety

## Dependencies
- Functions called/Symbols referenced:
  - max_parallel_hazard_context
  - [max_parallel_hazard_walker](../m/max_parallel_hazard_walker.md)
  - [list_concat](../l/list_concat.md)
  - PROPARALLEL_SAFE
  - PROPARALLEL_RESTRICTED
  - SubPlan
- Called from (representative examples):
  - [set_rel_consider_parallel](../s/set_rel_consider_parallel.md)
  - [query_planner](../q/query_planner.md)
  - [grouping_planner](../g/grouping_planner.md)
  - [create_projection_path](../c/create_projection_path.md)
  - [apply_projection_to_path](../a/apply_projection_to_path.md)

## Notes and Other Information
- This function requires that  be previously set by calling  on the entire query
- PARAM_EXEC parameters are considered parallel-restricted and require special handling
- The function implements an optimization where if the global query is parallel-safe and no execution parameters exist, it can skip the expensive tree walk
- Parameters from the same or parent query levels are considered safe as they are computed at Gather nodes
- Located in src/backend/optimizer/util/clauses.c:753-793