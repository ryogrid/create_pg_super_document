# listForeignDataWrappers

## Location
src/bin/psql/describe.c: 5728 - 5798

## Overview
Lists foreign data wrappers in the PostgreSQL database, showing their names, owners, handlers, and validators, with optional verbose information including access privileges and options.

## Definition
bool listForeignDataWrappers(const char *pattern, bool verbose)

## Detailed Description
This function queries the pg_foreign_data_wrapper system catalog to display information about foreign data wrappers (FDWs) in the database. It shows essential FDW properties including the wrapper name, owner, handler function, and validator function. In verbose mode, it additionally displays access control lists (ACLs), FDW-specific options formatted as key-value pairs, and descriptions. The function supports pattern matching to filter results and provides internationalized column headers. This implements the \dew psql command functionality.

## Parameters / Member Variables
- `pattern`: Optional SQL pattern to filter foreign data wrapper names (can be NULL to show all FDWs)
- `verbose`: Boolean flag to control whether to show additional detailed information (ACLs, options, descriptions)

## Dependencies
- Functions called/Symbols referenced:
  - initPQExpBuffer
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [printACLColumn](../p/printACLColumn.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [validateSQLNamePattern](../v/validateSQLNamePattern.md)
  - termPQExpBuffer
  - [PSQLexec](../P/PSQLexec.md)
  - [printQuery](../p/printQuery.md)
  - [PQclear](../P/PQclear.md)
  - gettext_noop
- Called from (representative examples):
  - [exec_command_d](../e/exec_command_d.md) (psql command dispatcher)

## Notes and Other Information
- Returns false if pattern validation fails or query execution fails
- In verbose mode, displays FDW options using pg_options_to_table() for proper formatting
- Includes left join with pg_description for object descriptions in verbose mode
- Orders results alphabetically by FDW name for consistent output
- Uses internationalization support for all column headers and titles
- This function corresponds to the \dew command in psql for listing foreign data wrappers