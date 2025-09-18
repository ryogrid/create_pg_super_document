# listForeignTables

## Location
[src/bin/psql/describe.c:5930-6001](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L5930-L6001)

## Overview
Implements the  command in psql to list foreign tables with their associated schemas, servers, and optional FDW options and descriptions.

## Definition


## Detailed Description
This function queries the PostgreSQL system catalogs to retrieve information about foreign tables. It constructs a SQL query that joins multiple system tables (pg_foreign_table, pg_class, pg_namespace, pg_foreign_server, and optionally pg_description) to present a comprehensive view of foreign tables. The function supports pattern matching for selective display and verbose mode for additional details like FDW options and descriptions.

The query retrieves:
- Schema name (namespace)
- Table name
- Server name
- FDW options (in verbose mode)
- Table description (in verbose mode)

## Parameters / Member Variables
- : SQL name pattern for filtering foreign tables (can be NULL for all tables)
- : Boolean flag to include additional information like FDW options and descriptions

## Dependencies
- Functions called/Symbols referenced:
  - [PQExpBufferData](../P/PQExpBufferData.md) (data structure)
  - [printQueryOpt](../p/printQueryOpt.md) (data structure)
  - initPQExpBuffer
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md)
  - [validateSQLNamePattern](../v/validateSQLNamePattern.md)
  - termPQExpBuffer
  - [PSQLexec](../P/PSQLexec.md)
  - [printQuery](../p/printQuery.md)
- Called from (representative examples):
  - [exec_command_d](../e/exec_command_d.md) (in src/bin/psql/command.c:1003)

## Notes and Other Information
- This function is part of psql's describe commands (\d family)
- Uses internationalization with gettext_noop for column headers
- Implements proper error handling by returning false on failures
- The query joins multiple system catalogs to provide comprehensive foreign table information
- Pattern validation is handled by validateSQLNamePattern to ensure SQL injection safety
- Results are formatted and displayed using psql's standard query printing mechanism