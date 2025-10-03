# standard_ExplainOneQuery

## Location
[src/backend/commands/explain.c:455-526](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L455-L526)

## Overview
standard_ExplainOneQuery implements the core PostgreSQL logic for explaining a single plannable query, including planning, resource measurement, and output generation.

## Definition

```c
void
standard_ExplainOneQuery(Query *query, int cursorOptions,
						 IntoClause *into, ExplainState *es,
						 const char *queryString, ParamListInfo params,
						 QueryEnvironment *queryEnv)
```
## Detailed Description
standard_ExplainOneQuery performs the standard PostgreSQL EXPLAIN processing for plannable queries (SELECT, INSERT, UPDATE, DELETE). It handles the complete workflow from query planning through resource measurement to output generation. The function measures planning time, buffer usage during planning, and memory consumption when the respective options are enabled.

Key features include: planning the query using pg_plan_query(), measuring planning duration with high-resolution timing, tracking buffer usage during planning if buffers option is enabled, measuring memory consumption in a dedicated memory context if memory option is enabled, and delegating to ExplainOnePlan for actual plan execution and output formatting.

The function carefully manages memory contexts when measuring memory usage, creating a dedicated AllocSet context for accurate measurement and switching back to the original context after planning to capture the memory consumption statistics.

## Parameters / Member Variables
- `*query`: Query structure to be planned and explained
- `cursorOptions`: Cursor options flags affecting plan generation (e.g., parallel execution)
- `*into`: IntoClause for CREATE TABLE AS statements, NULL for regular queries
- `*es`: ExplainState containing EXPLAIN options and output formatting state
- `*queryString`: Original SQL query string for planning context
- `params`: ParamListInfo containing parameter values for parameterized queries
- `*queryEnv`: QueryEnvironment providing additional query execution context
## Dependencies
- Functions called/Symbols referenced:
  - [pg_plan_query](../p/pg_plan_query.md)
  - [ExplainOnePlan](../E/ExplainOnePlan.md)
  - [BufferUsageAccumDiff](../B/BufferUsageAccumDiff.md)
  - AllocSetContextCreate
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [MemoryContextMemConsumed](../M/MemoryContextMemConsumed.md)
  - INSTR_TIME_SET_CURRENT/INSTR_TIME_SUBTRACT
- Called from (representative examples):
  - [ExplainOneQuery](../E/ExplainOneQuery.md)

## Notes and Other Information
- Non-static function, can be called directly (bypassing hook mechanism)
- Measures planning-time resources separately from execution-time resources
- Uses AllocSet memory context type for memory measurement (may differ from planner's natural context type)
- Buffer usage measurement tracks planning-phase buffer access patterns
- Timing measurement uses high-resolution instrumentation for accurate planning duration
- Memory context management ensures accurate measurement without affecting normal planner operation

## Simplified Source

```c
void standard_ExplainOneQuery(Query *query, int cursorOptions,
                              IntoClause *into, ExplainState *es,
                              const char *queryString, ParamListInfo params,
                              QueryEnvironment *queryEnv)
{
    PlannedStmt *plan;
    instr_time planstart, planduration;
    BufferUsage bufusage_start, bufusage;
    MemoryContextCounters mem_counters;
    MemoryContext planner_ctx = NULL;
    MemoryContext saved_ctx = NULL;

    // Set up memory tracking if requested
    if (es->memory)
    {
        planner_ctx = AllocSetContextCreate(CurrentMemoryContext,
                                            "explain analyze planner context",
                                            ALLOCSET_DEFAULT_SIZES);
        saved_ctx = MemoryContextSwitchTo(planner_ctx);
    }

    // Record buffer usage before planning
    if (es->buffers)
        bufusage_start = pgBufferUsage;

    // Start timing the planning phase
    INSTR_TIME_SET_CURRENT(planstart);

    // Generate the execution plan
    plan = pg_plan_query(query, queryString, cursorOptions, params);

    // Calculate planning duration
    INSTR_TIME_SET_CURRENT(planduration);
    INSTR_TIME_SUBTRACT(planduration, planstart);

    // Collect memory usage statistics
    if (es->memory)
    {
        MemoryContextSwitchTo(saved_ctx);
        MemoryContextMemConsumed(planner_ctx, &mem_counters);
    }

    // Calculate buffer usage during planning
    if (es->buffers)
    {
        memset(&bufusage, 0, sizeof(BufferUsage));
        BufferUsageAccumDiff(&bufusage, &pgBufferUsage, &bufusage_start);
    }

    // Execute plan and generate output
    ExplainOnePlan(plan, into, es, queryString, params, queryEnv,
                   &planduration,
                   es->buffers ? &bufusage : NULL,
                   es->memory ? &mem_counters : NULL);
}
```