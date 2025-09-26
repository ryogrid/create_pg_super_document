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
- : Query structure to be planned and explained
- : Cursor options flags affecting plan generation (e.g., parallel execution)
- : IntoClause for CREATE TABLE AS statements, NULL for regular queries
- : ExplainState containing EXPLAIN options and output formatting state
- : Original SQL query string for planning context
- : ParamListInfo containing parameter values for parameterized queries  
- : QueryEnvironment providing additional query execution context

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