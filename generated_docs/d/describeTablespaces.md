# describeTablespaces

## Location
src/bin/psql/describe.c: 215 - 287

## Overview
Implements the \db psql command to display a list of tablespaces in the database, showing their names, owners, locations, and optional detailed information.

## Definition
```c
bool describeTablespaces(const char *pattern, bool verbose)
```

## Detailed Description
This function generates and executes a SQL query to list tablespaces from the pg_tablespace system catalog. It constructs a query that displays essential tablespace information including name, owner (resolved via pg_get_userbyid), and physical location (via pg_tablespace_location). In verbose mode, it additionally shows access control lists, tablespace options, size information (formatted with pg_size_pretty), and descriptions. The function supports pattern-based filtering and provides proper internationalization for column headers.

## Parameters / Member Variables
- `pattern`: Optional regular expression pattern to filter tablespaces by name
- `verbose`: Boolean flag to include additional columns (ACL, options, size, description)

## Dependencies
- Functions called/Symbols referenced:
  - initPQExpBuffer
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [printACLColumn](../p/printACLColumn.md)
  - [validateSQLNamePattern](../v/validateSQLNamePattern.md)
  - termPQExpBuffer
  - [PSQLexec](../P/PSQLexec.md)
  - [printQuery](../p/printQuery.md)
  - [PQclear](../P/PQclear.md)
- Called from (representative examples):
  - [exec_command_d](../e/exec_command_d.md) (in command.c:839)

## Notes and Other Information
- Part of psql's describe functionality (\db command)
- Uses PostgreSQL system functions for user resolution (pg_get_userbyid) and location lookup (pg_tablespace_location)
- In verbose mode, displays human-readable size information using pg_size_pretty
- Shows access control information through printACLColumn function
- Retrieves shared object descriptions from the pg_tablespace catalog
- Orders results alphabetically by tablespace name
- Returns boolean indicating success/failure of the operation