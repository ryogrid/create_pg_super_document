# pg_dependencies_recv

## Location
[src/backend/statistics/dependencies.c:710-725](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/dependencies.c#L710-L725)

## Overview
This function serves as the binary input routine for the pg_dependencies data type but deliberately prevents binary input by throwing an error, maintaining consistency with the text input restriction.

## Definition

```c
Datum
pg_dependencies_recv(PG_FUNCTION_ARGS)
```
## Detailed Description
The function is the binary input counterpart to pg_dependencies_in, designed to handle binary protocol input for the pg_dependencies type. Like its text input counterpart, it intentionally disallows input by raising an error with ERRCODE_FEATURE_NOT_SUPPORTED.

This restriction ensures that pg_dependencies values can only be created through PostgreSQL's internal statistics collection mechanisms during ANALYZE operations, not through external input via either text or binary protocols. The design prevents users from manually creating or modifying dependency statistics, which could lead to incorrect query optimization decisions.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - PG_RETURN_VOID (PostgreSQL macro for returning void from a function)
  - ereport (error reporting mechanism)
  - [errcode](../e/errcode.md) (error code assignment)
  - [errmsg](../e/errmsg.md) (error message formatting)

- Called from (representative examples):
  - Not directly called (registered as type binary input function in system catalogs)

## Notes and Other Information
- Part of the complete pg_dependencies type definition infrastructure
- Mirrors the behavior of pg_dependencies_in for binary protocol consistency
- Prevents corruption of statistics through external manipulation
- Registered in PostgreSQL's type system but blocks actual usage
- Essential for maintaining data integrity of functional dependency statistics
- The error helps identify incorrect attempts to input dependency data manually

## Simplified Source

```c
Datum pg_dependencies_recv(PG_FUNCTION_ARGS) {
    // Reject binary input for pg_dependencies type
    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("cannot accept a value of type %s", "pg_dependencies")));

    PG_RETURN_VOID();  // Never reached
}
```