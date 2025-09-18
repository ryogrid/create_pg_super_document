# PQresultVerboseErrorMessage

## Location
src/interfaces/libpq/fe-exec.c: 3435 - 3465

## Overview
PQresultVerboseErrorMessage retrieves a formatted error message from a PGresult with configurable verbosity and context visibility levels.

## Definition


## Detailed Description
This function extracts and formats error information from a PGresult object with customizable levels of detail. It provides more control over error message formatting compared to basic error retrieval functions. The function validates that the result contains an error (either fatal or non-fatal), formats the error message using the specified verbosity and context settings, and returns a dynamically allocated string that the caller must free.

The function handles memory allocation failures gracefully by returning appropriate error messages. It uses PostgreSQL's internal error message building functionality to construct comprehensive error reports.

## Parameters / Member Variables
- : Pointer to the PGresult containing the error information to format
- : Controls the amount of detail included in the error message (PGVerbosity enum)
- : Determines whether to include context information in the error message (PGContextVisibility enum)

## Dependencies
- Functions called/Symbols referenced:
  - strdup
  - libpq_gettext
  - initPQExpBuffer
  - pqBuildErrorMessage3
  - PQExpBufferDataBroken
  - termPQExpBuffer
- Types referenced:
  - PGVerbosity
  - PGContextVisibility
  - PQExpBufferData
  - PGRES_FATAL_ERROR
  - PGRES_NONFATAL_ERROR
- Called from (representative examples):
  - exec_command_errverbose (psql command processing)

## Notes and Other Information
- Returns a dynamically allocated string that must be freed by the caller
- Returns NULL if memory allocation fails
- Only works with error results (PGRES_FATAL_ERROR or PGRES_NONFATAL_ERROR)
- For non-error results, returns a constant error message
- Uses PostgreSQL's internationalization system (libpq_gettext) for error messages
- Part of the libpq client library interface