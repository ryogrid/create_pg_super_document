# pqBuildErrorMessage3

## Location
src/interfaces/libpq/fe-protocol3.c: 1014 - 1184

## Overview
Constructs a formatted error message from the fields in a PGresult structure, with support for different verbosity levels and context visibility options.

## Definition


## Detailed Description
This function builds a comprehensive error message by extracting and formatting various diagnostic fields from a PGresult structure. It supports three verbosity levels: TERSE (minimal), DEFAULT (standard), and VERBOSE (detailed). The function handles different types of error position information (statement position and internal position), formats query text with syntax cursor display when appropriate, and includes additional diagnostic information based on the verbosity setting.

The function processes error fields in a specific order: severity, SQLSTATE (if verbose), primary message, position information, and then additional details like DETAIL, HINT, QUERY, CONTEXT, and schema/table/column names. For VERBOSE mode, it also includes source location information (file, line, function).

## Parameters / Member Variables
- : PQExpBuffer to append the formatted error message to
- : PGresult structure containing the error fields to format
- : Controls the amount of detail in the error message (PQERRORS_TERSE, PQERRORS_DEFAULT, PQERRORS_VERBOSE, PQERRORS_SQLSTATE)
- : Controls when to show context information (PQSHOW_CONTEXT_NEVER, PQSHOW_CONTEXT_ERRORS, PQSHOW_CONTEXT_ALWAYS)

## Dependencies
- Functions called/Symbols referenced:
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [libpq_gettext](../l/libpq_gettext.md)
  - [PQresultErrorField](../P/PQresultErrorField.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - atoi
  - appendPQExpBufferChar
  - reportErrorPosition
- Called from (representative examples):
  - [PQresultVerboseErrorMessage](../P/PQresultVerboseErrorMessage.md)
  - [pqGetErrorNotice3](pqGetErrorNotice3.md)

## Notes and Other Information
- Handles NULL PGresult by returning "out of memory" message
- Falls back to base error message if no broken-down fields are available
- Supports internationalization through libpq_gettext for translatable strings
- Handles both statement position and internal position error reporting
- [Query](../Q/Query.md) text with syntax cursor display is shown only for non-TERSE verbosity levels
- VERBOSE mode includes schema, table, column, datatype, and constraint names when available
- Source location information (file:line, function) is included only in VERBOSE mode
- Context information display is controlled by show_context parameter and result status