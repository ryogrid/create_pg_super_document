# describeAggregates

## Location
[src/bin/psql/describe.c:71-140](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L71-L140)

## Overview
Implements the \da psql command to display a list of aggregate functions in the database, with optional pattern matching and filtering capabilities.

## Definition

```c
bool
describeAggregates(const char *pattern, bool verbose, bool showSystem)
```
## Detailed Description
This function generates and executes a SQL query to list aggregate functions from the PostgreSQL system catalogs. It constructs a SELECT query that retrieves aggregate function information from pg_proc and pg_namespace catalogs, formatting the output as a table showing schema name, function name, return type, and argument types. The function handles version-specific differences in PostgreSQL (using prokind='a' for version 11+ and proisagg for older versions) and supports pattern-based filtering and system object visibility control.

## Parameters / Member Variables
- `*pattern`: Optional regular expression pattern to filter aggregate functions by name or schema
- `verbose`: Flag to enable verbose output (currently not used in implementation)
- `showSystem`: Boolean flag to control whether system schema aggregates (pg_catalog, information_schema) are displayed
## Dependencies
- Functions called/Symbols referenced:
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [validateSQLNamePattern](../v/validateSQLNamePattern.md)
  - [termPQExpBuffer](../t/termPQExpBuffer.md)
  - [PSQLexec](../P/PSQLexec.md)
  - [printQuery](../p/printQuery.md)
  - [PQclear](../P/PQclear.md)
- Called from (representative examples):
  - [exec_command_d](../e/exec_command_d.md) (in command.c:836)

## Notes and Other Information
- Part of psql's describe functionality (\da command)
- Handles PostgreSQL version compatibility (version 11+ vs older versions)
- Uses internationalization through gettext_noop for column headers
- Returns boolean indicating success/failure of the operation
- Excludes system schemas by default unless showSystem is true
- The verbose parameter is accepted but not currently utilized in the implementation