# PQresultVerboseErrorMessage

## Location
[src/interfaces/libpq/fe-exec.c:3435-3465](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L3435-L3465)

## Overview
PQresultVerboseErrorMessage retrieves a formatted error message from a PGresult with configurable verbosity and context visibility levels.

## Definition

```c
char *
PQresultVerboseErrorMessage(const PGresult *res,
							PGVerbosity verbosity,
							PGContextVisibility show_context)
```
## Detailed Description
This function extracts and formats error information from a PGresult object with customizable levels of detail. It provides more control over error message formatting compared to basic error retrieval functions. The function validates that the result contains an error (either fatal or non-fatal), formats the error message using the specified verbosity and context settings, and returns a dynamically allocated string that the caller must free.

The function handles memory allocation failures gracefully by returning appropriate error messages. It uses PostgreSQL's internal error message building functionality to construct comprehensive error reports.

## Parameters / Member Variables
- `*res`: Pointer to the PGresult containing the error information to format
- `verbosity`: Controls the amount of detail included in the error message (PGVerbosity enum)
- `show_context`: Determines whether to include context information in the error message (PGContextVisibility enum)
## Dependencies
- Functions called/Symbols referenced:
  - strdup
  - [libpq_gettext](../l/libpq_gettext.md)
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - [pqBuildErrorMessage3](../p/pqBuildErrorMessage3.md)
  - PQExpBufferDataBroken
  - [termPQExpBuffer](../t/termPQExpBuffer.md)
- Types referenced:
  - PGVerbosity
  - PGContextVisibility
  - [PQExpBufferData](PQExpBufferData.md)
  - PGRES_FATAL_ERROR
  - PGRES_NONFATAL_ERROR
- Called from (representative examples):
  - [exec_command_errverbose](../e/exec_command_errverbose.md) (psql command processing)

## Notes and Other Information
- Returns a dynamically allocated string that must be freed by the caller
- Returns NULL if memory allocation fails
- Only works with error results (PGRES_FATAL_ERROR or PGRES_NONFATAL_ERROR)
- For non-error results, returns a constant error message
- Uses PostgreSQL's internationalization system (libpq_gettext) for error messages
- Part of the libpq client library interface