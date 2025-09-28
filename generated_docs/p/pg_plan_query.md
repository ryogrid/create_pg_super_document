# pg_plan_query

## Location
[src/backend/tcop/postgres.c:890-975](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L890-L975)

## Overview
Generates an execution plan for a single already-rewritten query by serving as a thin wrapper around the PostgreSQL planner with additional debugging and performance monitoring capabilities.

## Definition

```c
PlannedStmt *
pg_plan_query(Query *querytree, const char *query_string, int cursorOptions,
			  ParamListInfo boundParams)
```
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
  - [ActiveSnapshotSet](../A/ActiveSnapshotSet.md)
  - [ResetUsage](../R/ResetUsage.md)
  - [ShowUsage](../S/ShowUsage.md)
  - copyObject
  - [nodeToStringWithLocations](../n/nodeToStringWithLocations.md)
  - [stringToNodeWithLocations](../s/stringToNodeWithLocations.md)
  - [elog_node_display](../e/elog_node_display.md)
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

## Simplified Source

```c
// Simplified version of pg_plan_query
PlannedStmt *
pg_plan_query(Query *querytree, const char *query_string, int cursorOptions,
              ParamListInfo boundParams)
{
    PlannedStmt *plan;

    // Utility commands don't need execution plans
    if (querytree->commandType == CMD_UTILITY)
        return NULL;

    // Ensure we have an active snapshot for the planner
    Assert(ActiveSnapshotSet());

    // Start performance and tracing monitoring
    TRACE_POSTGRESQL_QUERY_PLAN_START();
    if (log_planner_stats)
        ResetUsage();

    // Call the core planner to generate the execution plan
    plan = planner(querytree, query_string, cursorOptions, boundParams);

    // Report planner performance if enabled
    if (log_planner_stats)
        ShowUsage("PLANNER STATISTICS");

    // Optional debugging: test plan tree copying
#ifdef COPY_PARSE_PLAN_TREES
    {
        PlannedStmt *new_plan = copyObject(plan);
        plan = new_plan;
    }
#endif

    // Optional debugging: test serialization/deserialization
#ifdef WRITE_READ_PARSE_PLAN_TREES
    {
        char *str = nodeToStringWithLocations(plan);
        PlannedStmt *new_plan = stringToNodeWithLocations(str);
        pfree(str);
        plan = new_plan;
    }
#endif

    // Debug output if enabled
    if (Debug_print_plan)
        elog_node_display(LOG, "plan", plan, Debug_pretty_print);

    TRACE_POSTGRESQL_QUERY_PLAN_DONE();

    return plan;
}
```

Key simplifications made:
- Removed complex debugging comments while preserving debugging code blocks
- Added clear comments explaining each major phase
- Simplified conditional debug sections
- Preserved all performance monitoring and tracing functionality
- Maintained exact logic flow for all execution paths