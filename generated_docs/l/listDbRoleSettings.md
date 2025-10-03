# listDbRoleSettings

## Location
[src/bin/psql/describe.c:3761-3829](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L3761-L3829)

## Overview
A psql command function that implements the \\drds (describe role database settings) metacommand to display role-specific database configuration settings.

## Definition

```c
bool
listDbRoleSettings(const char *pattern, const char *pattern2)
```
## Detailed Description
This function provides functionality for the psql \\drds metacommand, which displays configuration settings that are specific to combinations of database roles and databases. It queries the pg_db_role_setting system catalog to retrieve role-specific and database-specific parameter settings. The function supports pattern matching for both role names and database names, allowing users to filter results. It constructs and executes a SQL query that joins pg_db_role_setting with pg_database and pg_roles catalogs to provide human-readable output with role names, database names, and their associated configuration settings.

## Parameters / Member Variables
- `*pattern`: A SQL pattern (with wildcards) to filter by role name, or NULL to match all roles
- `*pattern2`: A SQL pattern (with wildcards) to filter by database name, or NULL to match all databases
## Dependencies
- Functions called/Symbols referenced:
  - [PQExpBufferData](../P/PQExpBufferData.md) (PostgreSQL's expandable string buffer structure)
  - [printQueryOpt](../p/printQueryOpt.md) (print formatting options structure)
  - [initPQExpBuffer](../i/initPQExpBuffer.md) (initialize buffer)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md) (formatted append to buffer)
  - [validateSQLNamePattern](../v/validateSQLNamePattern.md) (validate and append SQL name patterns)
  - [PSQLexec](../P/PSQLexec.md) (execute SQL query)
  - [termPQExpBuffer](../t/termPQExpBuffer.md) (cleanup buffer)
  - [printQuery](../p/printQuery.md) (display query results)
- Called from (representative examples):
  - [exec_command_d](../e/exec_command_d.md) (psql command dispatcher at src/bin/psql/command.c:941)
  - DESCRIBE_H (function declaration in src/bin/psql/describe.h:38)

## Notes and Other Information
- Returns true on success, false on error
- Implements the psql \\drds metacommand functionality
- Provides helpful error messages when no settings are found (only in non-quiet mode)
- Unlike most describe functions, this one explicitly reports when no results are found to help users understand the dual-pattern nature of the command
- The query output includes role name, database name, and settings formatted as newline-separated configuration parameters
- Located in src/bin/psql/describe.c:3761-3829
- Uses LEFT JOINs to handle cases where role or database might be NULL (indicating global settings)