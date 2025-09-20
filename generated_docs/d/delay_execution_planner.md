# delay_execution_planner

## Location
[src/test/modules/delay_execution/delay_execution.c:42-74](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/delay_execution/delay_execution.c#L42-L74)

## Overview
A planner hook function in PostgreSQL's test module that provides deliberate delays during query planning for testing purposes.

## Definition

```c
static PlannedStmt *
delay_execution_planner(Query *parse, const char *query_string,
						int cursorOptions, ParamListInfo boundParams)
```
## Detailed Description
The  function serves as a planner hook that introduces controlled delays during the query planning phase. This function is part of PostgreSQL's testing infrastructure, specifically designed to test timing-sensitive scenarios and race conditions by creating predictable delays in query execution planning.

The function works by:
1. First invoking either the previous planner hook (if one exists) or the standard PostgreSQL planner
2. If the  is set to a non-zero value, it acquires and immediately releases a PostgreSQL advisory lock
3. The lock acquisition/release cycle creates the desired delay
4. It ensures that any pending cache invalidations are processed after the delay

This mechanism allows tests to control the timing of query planning operations, which is essential for testing concurrent operations, deadlock scenarios, and other timing-dependent behaviors in PostgreSQL.

## Parameters / Member Variables
- : The Query structure representing the parsed SQL statement to be planned
- : The original SQL query string as provided by the client
- : Integer flags controlling cursor behavior and options
- : Parameter list information for parameterized queries (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - prev_planner_hook (function pointer to previous hook)
  - [standard_planner](../s/standard_planner.md) (PostgreSQL's standard query planner)
  - DirectFunctionCall1 (PostgreSQL's direct function call interface)
  - [pg_advisory_lock_int8](../p/pg_advisory_lock_int8.md) (advisory lock acquisition function)
  - [pg_advisory_unlock_int8](../p/pg_advisory_unlock_int8.md) (advisory lock release function)
  - [Int64GetDatum](../I/Int64GetDatum.md) (converts int64 to PostgreSQL Datum type)
  - [AcceptInvalidationMessages](../A/AcceptInvalidationMessages.md) (processes pending cache invalidations)
- Called from:
  - [_PG_init](../P/_PG_init.md) (registered as planner hook during module initialization)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the delay_execution.c file
- The function is part of PostgreSQL's test infrastructure, not production code
- The delay mechanism relies on the  global variable
- Advisory locks are used instead of sleep functions to create delays that are more predictable and testable
- The function properly chains with other planner hooks by checking for 
- Cache invalidation processing ensures the delay doesn't interfere with PostgreSQL's cache consistency mechanisms