# planner

## Location
src/backend/optimizer/plan/planner.c: 275 - 287

## Overview
The planner function serves as the main entry point to PostgreSQL's query optimizer, providing a hook mechanism that allows loadable plugins to monitor or modify planner behavior before delegating to the standard planning process.

## Definition


## Detailed Description
The planner function acts as a gateway to PostgreSQL's query optimization system. It implements a plugin architecture through the planner_hook mechanism, allowing external modules to intercept and potentially modify the planning process. If no hook is installed, it delegates directly to standard_planner(). This design enables extensibility while maintaining the core planning functionality intact.

The function is designed to be the primary interface for converting a parsed query (Query struct) into an executable plan (PlannedStmt). It provides plugin authors with the flexibility to implement custom planning logic, query transformation, or monitoring capabilities.

## Parameters / Member Variables
- : Pointer to the Query structure containing the parsed SQL query to be planned
- : String representation of the original SQL query for debugging/logging purposes
- : Integer flags controlling cursor behavior and execution options
- : ParamListInfo structure containing bound parameter values for parameterized queries

## Dependencies
- Functions called/Symbols referenced:
  - [standard_planner](../s/standard_planner.md) (fallback planning function)
  - planner_hook (function pointer for plugin extension)
- Called from (representative examples):
  - [pg_plan_query](pg_plan_query.md)
  - [DebugParallelMode](../D/DebugParallelMode.md)

## Notes and Other Information
- Plugin authors should note that standard_planner() modifies its Query input, so copying the data structure is necessary for multiple planning attempts
- The hook mechanism allows for pre and post-processing of planning operations
- This function is a critical component in PostgreSQL's extensibility architecture
- Located in src/backend/optimizer/plan/planner.c:275-287