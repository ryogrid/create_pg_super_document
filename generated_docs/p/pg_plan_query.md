# pg_plan_query

## Location
src/backend/tcop/postgres.c: 890 - 975

## Overview
Generates an execution plan for a single already-rewritten query by serving as a thin wrapper around the PostgreSQL planner with additional debugging and performance monitoring capabilities.

## Definition


## Detailed Description
This function is the main entry point for query planning in PostgreSQL, transforming a rewritten Query structure into an executable PlannedStmt. It serves as a wrapper around the core  function while providing additional functionality:

1. **Utility Command Handling**: Returns NULL immediately for utility commands (DDL) since they don't require execution plans.

2. **Snapshot Validation**: Ensures an active snapshot exists before planning, as the planner may need to call user-defined functions that require transaction visibility.

3. **Performance Monitoring**: Optionally collects planner statistics when  is enabled.

4. **Debugging Support**: Includes extensive debugging infrastructure for development:
   - Optional plan tree copying verification (COPY_PARSE_PLAN_TREES)
   - Optional serialization/deserialization testing (WRITE_READ_PARSE_PLAN_TREES)
   - Debug output for generated plans

5. **DTrace Integration**: Provides tracing points for query plan generation monitoring.

The function is essential in PostgreSQL's query processing pipeline, taking queries that have been parsed and rewritten and converting them into executable plans.

## Parameters / Member Variables
- : Rewritten Query structure ready for planning
- : Original SQL query string for logging and error reporting
- : Cursor-specific options affecting plan generation
- : Parameter values for prepared statement planning

## Dependencies
- Functions called/Symbols referenced:
  - [planner](planner.md)
  - ActiveSnapshotSet
  - ResetUsage
  - ShowUsage
  - copyObject
  - [nodeToStringWithLocations](../n/nodeToStringWithLocations.md)
  - [stringToNodeWithLocations](../s/stringToNodeWithLocations.md)
  - elog_node_display
  - TRACE_POSTGRESQL_QUERY_PLAN_START
  - TRACE_POSTGRESQL_QUERY_PLAN_DONE
- Called from (representative examples):
  - [pg_plan_queries](pg_plan_queries.md)
  - [BeginCopyTo](../B/BeginCopyTo.md)
  - [ExecCreateTableAs](../E/ExecCreateTableAs.md)
  - [standard_ExplainOneQuery](../s/standard_ExplainOneQuery.md)
  - [refresh_matview_datafill](../r/refresh_matview_datafill.md)
  - [PerformCursorOpen](../P/PerformCursorOpen.md)
  - [init_execution_state](../i/init_execution_state.md)

## Notes and Other Information
- Returns NULL for utility commands as they don't require execution plans
- Requires an active snapshot due to potential user-defined function calls during planning
- The planner statistics can be enabled via the  configuration parameter
- Located in src/backend/tcop/postgres.c:890-975
- Includes comprehensive debugging support for plan tree validation and testing
- Critical component in PostgreSQL's cost-based query optimization system
- The equal() function currently lacks support for Plan node comparison, limiting some debugging capabilities
- DTrace/SystemTap integration enables runtime plan generation monitoring