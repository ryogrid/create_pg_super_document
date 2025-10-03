# listPartitionedTables

## Location
[src/bin/psql/describe.c:4107-4306](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L4107-L4306)

## Overview
A specialized psql command function that implements the \\dP metacommand to display partitioned tables and indexes with detailed partition hierarchy information and size statistics.

## Definition

```c
bool
listPartitionedTables(const char *reltypes, const char *pattern, bool verbose)
```
## Detailed Description
This function provides functionality for the psql \\dP metacommand, which is specifically designed to list partitioned tables and indexes introduced in PostgreSQL 10.0 with declarative partitioning. The function supports filtering by relation types (tables 't', indexes 'i', and nested partitions 'n') and can display comprehensive information about partition hierarchies. In verbose mode, it shows partition sizes using either recursive queries (pre-12.0) or the pg_partition_tree function (12.0+). The function handles mixed output when both tables and indexes are requested and provides parent-child relationship information when nested partitions or patterns are specified.

## Parameters / Member Variables
- `*reltypes`: A string containing characters specifying which types to display ('t'=tables, 'i'=indexes, 'n'=nested/non-leaf partitioned tables)
- `*pattern`: A SQL pattern (with wildcards) to filter by relation name, or NULL to match all partitioned relations
- `verbose`: Boolean flag to include additional columns like partition sizes and description
## Dependencies
- Functions called/Symbols referenced:
  - [PQExpBufferData](../P/PQExpBufferData.md) (PostgreSQL's expandable string buffer structure)
  - [printQueryOpt](../p/printQueryOpt.md) (print formatting options structure)  
  - [formatPGVersionNumber](../f/formatPGVersionNumber.md) (format PostgreSQL version for display)
  - [initPQExpBuffer](../i/initPQExpBuffer.md) (initialize buffer)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md) (formatted append to buffer)
  - CppAsString2 (macro to convert constants to strings)
  - RELKIND_PARTITIONED_TABLE, RELKIND_PARTITIONED_INDEX (relation kind constants)
  - [validateSQLNamePattern](../v/validateSQLNamePattern.md) (validate and append SQL name patterns)
  - [termPQExpBuffer](../t/termPQExpBuffer.md) (cleanup buffer)
  - [PSQLexec](../P/PSQLexec.md) (execute SQL query)
  - lengthof (macro to get array length)
  - [printQuery](../p/printQuery.md) (display query results)
- Called from (representative examples):
  - [exec_command_d](../e/exec_command_d.md) (psql command dispatcher at src/bin/psql/command.c:914)
  - DESCRIBE_H (function declaration in src/bin/psql/describe.h:74)

## Notes and Other Information
- Returns true on success, false on error
- Implements the psql \\dP metacommand functionality
- Requires PostgreSQL 10.0+ (exits early with error message for older versions)
- If no relation types are specified, defaults to showing both tables and indexes
- Supports multiple output titles: "List of partitioned tables", "List of partitioned indexes", or "List of partitioned relations"
- Version-aware size calculation: uses recursive CTE for pre-12.0, pg_partition_tree function for 12.0+
- In verbose mode with nested partitions, shows both "Leaf partition size" (direct children) and "Total size" (all descendants)
- Shows parent-child relationships when 'n' flag is used or pattern is specified
- By default excludes system schemas unless a pattern is provided
- Automatically filters out leaf partitions unless nested viewing or pattern matching is requested
- Results are ordered by schema, type (if mixed output), parent name, and relation name
- Uses column translation for internationalization support
- Located in src/bin/psql/describe.c:4107-4306