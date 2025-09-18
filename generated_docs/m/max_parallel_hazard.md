# max_parallel_hazard

## Location
src/backend/optimizer/util/clauses.c: 734 - 752

## Overview
Analyzes a query tree to determine the worst parallel-hazard level, which indicates whether the query can be safely parallelized.

## Definition
```c
char max_parallel_hazard(Query *parse)
```

## Detailed Description
This function performs a comprehensive analysis of a query tree to determine its parallel safety characteristics. It identifies the most restrictive parallel hazard level present anywhere in the query, which determines whether and how the query can be executed in parallel.

The function evaluates three levels of parallel safety (from most to least restrictive):
1. **PROPARALLEL_UNSAFE**: Cannot be executed in parallel workers at all
2. **PROPARALLEL_RESTRICTED**: Can be executed in parallel workers but with restrictions  
3. **PROPARALLEL_SAFE**: Can be safely executed in parallel workers without restrictions

The analysis is performed by setting up a context structure and delegating the actual tree traversal to `max_parallel_hazard_walker()`. The context tracks the maximum (worst) hazard level encountered during traversal, starting with the assumption that everything is safe until proven otherwise.

The result is used by the planner to determine parallel execution feasibility and is stored in PlannerGlobal for efficient access during subsequent planning phases.

## Parameters / Member Variables
- `parse`: The Query node representing the query tree to analyze

## Dependencies
- Functions called/Symbols referenced:
  - `max_parallel_hazard_context`: Context structure for tracking analysis state
  - `PROPARALLEL_SAFE`: Constant representing safe parallel execution
  - `PROPARALLEL_UNSAFE`: Constant representing unsafe parallel execution  
  - `[max_parallel_hazard_walker](max_parallel_hazard_walker.md)`: Performs the actual tree traversal and analysis
- Called from (representative examples):
  - `[standard_planner](../s/standard_planner.md)` (at planner.c:357)
  - `WindowFuncLists` (referenced in clauses.h:35)

## Notes and Other Information
- Returns a char value representing the most restrictive parallel hazard level found
- The analysis is conservative - any unsafe element makes the entire query unsafe
- Results are cached in PlannerGlobal to optimize repeated checks during planning
- Critical for PostgreSQL's parallel query execution infrastructure
- The function examines all aspects of the query including functions, operators, and special constructs
- Used as a gating check before attempting parallel plan generation