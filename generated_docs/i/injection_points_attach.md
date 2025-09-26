# injection_points_attach

## Location
[src/test/modules/injection_points/injection_points.c:268-309](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/injection_points/injection_points.c#L268-L309)

## Overview
A SQL-callable function that creates and registers injection points with specified actions, enabling dynamic testing point insertion during PostgreSQL execution.

## Definition
Datum injection_points_attach(PG_FUNCTION_ARGS)

## Detailed Description
The `injection_points_attach` function provides the primary interface for creating injection points within PostgreSQL. It accepts a name and action type, then registers the injection point with the appropriate callback function. The function supports three action types: "error" (triggers an error), "notice" (logs a notice message), and "wait" (implements blocking synchronization). It can create either global injection points (affecting all processes) or local injection points (affecting only the current process based on the injection_point_local setting). For local injection points, it maintains a list for automatic cleanup when the session ends.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function arguments containing:
  - Argument 0: Text string representing the injection point name
  - Argument 1: Text string representing the action type ("error", "notice", or "wait")

## Dependencies
- Functions called/Symbols referenced:
  - [text_to_cstring](../t/text_to_cstring.md)
  - [InjectionPointAttach](../I/InjectionPointAttach.md)
  - [makeString](../m/makeString.md)
  - [lappend](../l/lappend.md)
  - [pstrdup](../p/pstrdup.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - PG_RETURN_VOID
  - [InjectionPointCondition](../I/InjectionPointCondition.md)
  - INJ_CONDITION_PID
- Called from (representative examples):
  - SQL interface (can be called from SQL queries as a function)

## Notes and Other Information
- Accessible via SQL as a function, typically used in test scripts and procedures
- Action types map to specific callback functions: "error" → injection_error, "notice" → injection_notice, "wait" → injection_wait
- When injection_point_local is enabled, creates process-specific injection points using PID-based conditions
- Local injection points are tracked in inj_list_local for automatic cleanup
- Throws an ERROR for unsupported action types
- Essential component of PostgreSQL's testing infrastructure for creating controlled test scenarios
- Part of the injection_points extension module