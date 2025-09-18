# exec_command_errverbose

## Location
src/bin/psql/command.c: 1377 - 1406

## Overview
Implements the \errverbose command in psql, which displays detailed error information from the most recent failed database query.

## Definition


## Detailed Description
This function retrieves and displays verbose error information from the last failed query result stored in pset.last_error_result. It uses PQresultVerboseErrorMessage() with verbose settings and context to provide comprehensive error details. If no previous error exists, it informs the user accordingly. The function handles memory management by freeing the error message after display.

## Parameters / Member Variables
- : Scanner state for parsing the command line (unused in this function)
- : Boolean indicating whether this command should be executed (used for conditional processing)

## Dependencies
- Functions called/Symbols referenced:
  - PQresultVerboseErrorMessage
  - PQfreemem
  - pg_log_error
  - puts
  - _ (internationalization macro)
  - PsqlScanState (type)
  - PQERRORS_VERBOSE (constant)
  - PQSHOW_CONTEXT_ALWAYS (constant)
  - PSQL_CMD_SKIP_LINE (return value)
  - backslashResult (return type)
- Called from (representative examples):
  - exec_command
  - EditableObjectType (indirectly through command dispatch)

## Notes and Other Information
- The command takes no arguments and operates on the globally stored last error result
- Uses verbose error reporting flags to provide maximum detail about the error
- Properly handles memory allocation failures when retrieving error messages
- Provides internationalized messages for "out of memory" and "no previous error" cases
- The error message is displayed using pg_log_error() to ensure proper formatting and output routing
- Memory allocated by PQresultVerboseErrorMessage() is correctly freed using PQfreemem()