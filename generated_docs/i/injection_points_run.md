# injection_points_run

## Location
[src/test/modules/injection_points/injection_points.c:310-323](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/injection_points/injection_points.c#L310-L323)

## Overview
A SQL-callable function that manually triggers execution of a named injection point, allowing tests to programmatically activate injection points on demand.

## Definition
Datum injection_points_run(PG_FUNCTION_ARGS)

## Detailed Description
The `injection_points_run` function provides a direct mechanism to execute injection points by name from SQL. It serves as a bridge between SQL test scripts and the injection point framework, allowing tests to precisely control when specific injection points should fire. The function takes an injection point name as input and immediately invokes the INJECTION_POINT macro with that name, which will execute any callbacks registered for that injection point. This enables deterministic testing scenarios where the exact timing of injection point execution needs to be controlled.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function arguments containing:
  - Argument 0: Text string representing the injection point name to execute

## Dependencies
- Functions called/Symbols referenced:
  - [text_to_cstring](../t/text_to_cstring.md)
  - INJECTION_POINT
  - PG_RETURN_VOID
- Called from (representative examples):
  - SQL interface (can be called from SQL queries as a function)

## Notes and Other Information
- Provides direct SQL access to trigger injection points programmatically
- Extremely useful for testing scenarios where injection points need to be triggered at precise moments
- Works in conjunction with injection_points_attach to first create and then trigger injection points
- The INJECTION_POINT macro handles the actual execution of registered callbacks
- Simple interface that makes injection point testing accessible from standard SQL test scripts
- Part of the injection_points extension module and commonly used in PostgreSQL's regression testing