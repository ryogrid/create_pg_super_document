# permissionsList

## Location
[src/bin/psql/describe.c:1011-1174](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L1011-L1174)

## Overview
Implements the \z and \dp psql commands to display access privileges (grants and revokes) for database tables, views, materialized views, sequences, foreign tables, and partitioned tables.

## Definition


## Detailed Description
This function constructs and executes a comprehensive SQL query to retrieve access control information from PostgreSQL system catalogs. It displays detailed privilege information including table-level permissions, column-level privileges, and row-level security policies. The output includes schema name, object name, object type, access privileges, column privileges, and policies (when supported by the server version).

The function adapts its query based on PostgreSQL server version to handle the evolution of row-level security features. For servers version 9.5-9.6, it displays basic policy information. For PostgreSQL 10+, it includes support for RESTRICTIVE policies, showing whether policies are permissive or restrictive in nature.

The query excludes indexes and toast tables as they have no meaningful access rights. It formats complex privilege information in a human-readable way, including policy details with USING and WITH CHECK expressions, and role-based policy assignments.

## Parameters / Member Variables
- : SQL pattern to filter object names (can be NULL to show all objects)
- : If true, includes system schema objects (pg_catalog, information_schema)

## Dependencies
- Functions called/Symbols referenced:
  - initPQExpBuffer (initialize query buffer)
  - [printfPQExpBuffer](printfPQExpBuffer.md) (format base SQL query)
  - [printACLColumn](printACLColumn.md) (format access control list for table-level privileges)
  - [validateSQLNamePattern](../v/validateSQLNamePattern.md) (validate and apply name pattern filtering)
  - [PSQLexec](../P/PSQLexec.md) (execute the constructed SQL query)
  - [printQuery](printQuery.md) (display formatted results with translation support)
  - termPQExpBuffer (cleanup query buffer)
  - RELKIND constants (RELKIND_RELATION, RELKIND_VIEW, RELKIND_MATVIEW, etc.)
- Called from (representative examples):
  - [exec_command_d](../e/exec_command_d.md) (src/bin/psql/command.c:903) - handles \dp command variant
  - [exec_command_z](../e/exec_command_z.md) (src/bin/psql/command.c:3036) - handles \z command
  - Declared in DESCRIBE_H (src/bin/psql/describe.h:44)

## Notes and Other Information
- Excludes indexes and toast tables from results as they have no meaningful access rights
- Supports version-specific features: basic policies (9.5+) and restrictive policies (10+)
- Column privileges are displayed in a nested format showing column name followed by its ACL
- Policy information includes command type, USING expressions (u), WITH CHECK expressions (c), and applicable roles
- For PostgreSQL 10+, distinguishes between PERMISSIVE and RESTRICTIVE policies
- Uses translate_columns array to control which columns should be translated for internationalization
- Results are ordered by schema name and object name for consistent presentation
- Object types are translated for display (table, view, materialized view, sequence, foreign table, partitioned table)
- Returns boolean status indicating success/failure of the operation