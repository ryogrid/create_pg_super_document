# standard_planner

## Location
src/backend/optimizer/plan/planner.c: 288 - 628

## Overview
The standard_planner function is PostgreSQL's core query planning implementation that converts a parsed Query structure into an optimized executable PlannedStmt, handling parallelism assessment, cost optimization, and plan finalization.

## Definition
```c
PlannedStmt *standard_planner(Query *parse, const char *query_string, int cursorOptions, ParamListInfo boundParams)
```

## Detailed Description
The standard_planner function implements PostgreSQL's complete query planning pipeline. It begins by setting up global planner state (PlannerGlobal) and assessing parallel execution feasibility based on query characteristics, system configuration, and safety constraints. The function determines the optimal tuple fraction for cursor operations, delegates to subquery_planner for the core planning work, selects the best execution path, and converts it to a Plan tree.

Key responsibilities include:
- Initializing global planner state and configuration
- Evaluating parallel execution opportunities and constraints  
- Handling cursor-specific optimizations (fast-start vs. complete results)
- Managing scrollable cursor requirements with materialization
- Optionally adding debug Gather nodes for testing
- Finalizing parameter handling and plan references
- Configuring JIT compilation flags based on cost thresholds
- Building the final PlannedStmt result structure

The function implements sophisticated parallel planning logic, considering factors like backend mode, command type, parallel hazard levels, and debug settings.

## Parameters / Member Variables
- `parse`: Pointer to the Query structure containing the parsed and analyzed SQL statement
- `query_string`: Original SQL query string for debugging and logging purposes
- `cursorOptions`: Bitmask controlling cursor behavior (CURSOR_OPT_FAST_PLAN, CURSOR_OPT_SCROLL, CURSOR_OPT_PARALLEL_OK)
- `boundParams`: ParamListInfo containing bound parameter values for parameterized queries

## Dependencies
- Functions called/Symbols referenced:
  - [subquery_planner](subquery_planner.md) (core recursive planning function)
  - fetch_upper_rel (retrieve final relation)
  - [get_cheapest_fractional_path](../g/get_cheapest_fractional_path.md) (path selection)
  - [create_plan](../c/create_plan.md) (path to plan conversion)
  - [ExecSupportsBackwardScan](../E/ExecSupportsBackwardScan.md) (scrollability check)
  - [materialize_finished_plan](../m/materialize_finished_plan.md) (materialization for scrolling)
  - [SS_finalize_plan](../S/SS_finalize_plan.md) (parameter finalization)
  - [set_plan_references](set_plan_references.md) (reference resolution)
  - [max_parallel_hazard](../m/max_parallel_hazard.md) (parallel safety assessment)
- Called from (representative examples):
  - [planner](../p/planner.md) (main entry point)
  - [delay_execution_planner](../d/delay_execution_planner.md) (test module)

## Notes and Other Information
- Modifies the input Query structure, requiring copying for multiple planning attempts
- Implements comprehensive parallel execution assessment with multiple safety checks
- Handles special cases for CREATE TABLE AS, SELECT INTO, and CREATE MATERIALIZED VIEW in parallel mode
- Supports debug modes for parallel query testing (debug_parallel_query settings)
- Configures JIT compilation based on cost thresholds and enabled features
- Manages complex cursor optimization strategies based on expected result set usage
- Located in src/backend/optimizer/plan/planner.c:288-628