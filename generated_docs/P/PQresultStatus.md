# PQresultStatus

## Location
[src/interfaces/libpq/fe-exec.c:3411-3418](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L3411-L3418)

## Overview
PQresultStatus returns the execution status of a PostgreSQL query result, providing information about whether the command completed successfully or encountered an error.

## Definition

```c
ExecStatusType
PQresultStatus(const PGresult *res)
```
## Detailed Description
This function is a fundamental accessor function for PGresult objects that returns the execution status of a completed PostgreSQL command or query. It provides a way for client applications to determine the outcome of their database operations by returning one of the ExecStatusType enumeration values.

The function performs a simple null-check on the result parameter and returns the appropriate status. If the result pointer is NULL (indicating an invalid or uninitialized result), it returns PGRES_FATAL_ERROR as a safety measure. Otherwise, it returns the actual status stored in the result's resultStatus field.

This function is essential for error handling and result processing in libpq-based applications, as it allows programs to branch their logic based on whether operations succeeded, failed, or completed with warnings.

## Parameters / Member Variables
- : Const pointer to a PGresult structure containing the query execution result

## Dependencies
- Functions called/Symbols referenced:
  - PGRES_FATAL_ERROR (returned for NULL result)
- Called from:
  - Widely used throughout PostgreSQL test code and examples
  - Referenced in libpq_pipeline.c, testlo.c, testlo64.c, isolationtester.c
  - Used in fe-auth.c, fe-connect.c, fe-lobj.c within libpq itself
  - Public API function declared in libpq-fe.h

## Notes and Other Information
- This is a public libpq API function available to all client applications
- Returns an ExecStatusType enumeration value indicating command execution status
- Provides null-safety by returning PGRES_FATAL_ERROR for NULL input
- One of the most commonly used accessor functions for PGresult objects
- Essential for proper error handling in PostgreSQL client applications
- The function is thread-safe as it only reads from the result structure
- The function is located at src/interfaces/libpq/fe-exec.c:3411-3418