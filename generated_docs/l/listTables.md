# listTables

## Location
src/bin/psql/describe.c: 3909 - 4106

## Overview
A comprehensive psql command function that implements multiple table-related metacommands (\\dt, \\di, \\dv, etc.) to display various types of database relations including tables, indexes, views, sequences, and foreign tables.

## Definition


## Detailed Description
This function is the primary handler for multiple psql metacommands that list database relations. It supports listing tables (\\dt), indexes (\\di), views (\\dv), materialized views (\\dm), sequences (\\ds), and foreign tables (\\dE) either individually or in combination. The tabtypes parameter determines which relation types to include using single character codes (t=tables, i=indexes, v=views, m=materialized views, s=sequences, E=foreign tables). The function constructs a complex SQL query that joins pg_class with pg_namespace and optionally with pg_am (access methods) and pg_index depending on the requested information. It provides detailed information including schema, name, type, owner, and optionally persistence, access method, size, and description.

## Parameters / Member Variables
- : A string containing characters specifying which relation types to display ('t'=tables, 'i'=indexes, 'v'=views, 'm'=materialized views, 's'=sequences, 'E'=foreign tables)
- : A SQL pattern (with wildcards) to filter by relation name, or NULL to match all relations
- : Boolean flag to include additional columns like persistence, access method, size, and description
- : Boolean flag indicating whether to include system relations (catalog tables, toast tables, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [PQExpBufferData](../P/PQExpBufferData.md) (PostgreSQL's expandable string buffer structure)
  - [printQueryOpt](../p/printQueryOpt.md) (print formatting options structure)
  - initPQExpBuffer (initialize buffer)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md) (formatted append to buffer)
  - RELKIND_* constants (relation kind constants like RELKIND_RELATION, RELKIND_VIEW, etc.)
  - CppAsString2 (macro to convert constants to strings)
  - [validateSQLNamePattern](../v/validateSQLNamePattern.md) (validate and append SQL name patterns)
  - termPQExpBuffer (cleanup buffer)
  - [PSQLexec](../P/PSQLexec.md) (execute SQL query)
  - lengthof (macro to get array length)
  - [printQuery](../p/printQuery.md) (display query results)
- Called from (representative examples):
  - [exec_command_d](../e/exec_command_d.md) (psql command dispatcher at src/bin/psql/command.c:800, 931)
  - DESCRIBE_H (function declaration in src/bin/psql/describe.h:71)

## Notes and Other Information
- Returns true on success, false on error
- Implements multiple psql metacommands: \\dt, \\di, \\dv, \\dm, \\ds, \\dE
- If tabtypes is empty, defaults to showing tables, views, materialized views, sequences, and foreign tables
- Version-aware: includes access method information for PostgreSQL 12.0+ when not hidden
- In verbose mode, shows additional columns including persistence (permanent/temporary/unlogged), access method (if applicable), size, and description
- By default excludes system schemas (pg_catalog, pg_toast, information_schema) unless showSystem is true or a pattern is specified
- Supports TOAST table visibility when showSystem is true or a pattern is provided
- Provides helpful error messages when no relations are found (only in non-quiet mode)
- Uses column translation for internationalization support
- Results are ordered by schema name and relation name for consistent display
- Located in src/bin/psql/describe.c:3909-4106