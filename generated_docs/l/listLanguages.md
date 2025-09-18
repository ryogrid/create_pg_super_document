# listLanguages

## Location
[src/bin/psql/describe.c:4307-4382](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L4307-L4382)

## Overview
Lists and displays information about procedural languages available in the PostgreSQL database, corresponding to the psql \dL command.

## Definition
bool listLanguages(const char *pattern, bool verbose, bool showSystem)

## Detailed Description
The listLanguages function generates and executes a SQL query to retrieve information about procedural languages from the pg_catalog.pg_language system catalog. It formats the output as a table showing language details such as name, owner, and trusted status. When verbose mode is enabled, it includes additional details like call handlers, validators, inline handlers, and access control lists (ACLs). The function builds a dynamic SQL query based on the provided parameters and uses PostgreSQL's query execution and formatting infrastructure to display the results.

## Parameters / Member Variables
- : Optional SQL pattern to filter language names (can be NULL for no filtering)
- : Boolean flag to include extended information (handlers, validators, ACLs) in the output
- : Boolean flag to include system/internal languages in the listing

## Dependencies
- Functions called/Symbols referenced:
  - initPQExpBuffer
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md)
  - [printACLColumn](../p/printACLColumn.md)
  - [validateSQLNamePattern](../v/validateSQLNamePattern.md)
  - termPQExpBuffer
  - [PSQLexec](../P/PSQLexec.md)
  - [printQuery](../p/printQuery.md)
- Called from (representative examples):
  - [exec_command_d](../e/exec_command_d.md) (psql command dispatcher)

## Notes and Other Information
- Implements the \dL psql meta-command functionality
- Queries the pg_catalog.pg_language system catalog
- Joins with pg_catalog.pg_description for language descriptions
- Uses gettext_noop for internationalization support
- Returns false on query validation or execution failure, true on success
- When showSystem is false and no pattern is provided, filters out internal languages (lanplcallfoid != 0)
- Output is ordered by language name for consistent presentation