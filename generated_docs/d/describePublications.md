# describePublications

## Location
[src/bin/psql/describe.c:6339-6524](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L6339-L6524)

## Overview
Provides detailed descriptions of PostgreSQL logical replication publications, including their properties and associated tables/schemas, implementing the psql \dRp+ meta-command functionality.

## Definition

```c
bool
describePublications(const char *pattern)
```
## Detailed Description
The  function implements the  psql meta-command to display comprehensive information about logical replication publications. Unlike  which shows a simple list, this function provides detailed descriptions for each publication including:

1. Publication properties (owner, replication settings)
2. Individual tables published (with optional column lists and WHERE clauses for PostgreSQL 15+)
3. Schemas published (for PostgreSQL 15+)

The function dynamically adapts its output based on PostgreSQL server version:
- PostgreSQL 10+: Basic publication support
- PostgreSQL 11+: Truncate operations support
- PostgreSQL 13+: Via root partitioning support  
- PostgreSQL 15+: Column lists, WHERE clauses, and schema-level publications

For each publication found, it creates a detailed table showing all properties and then adds footer sections listing associated tables and schemas.

## Parameters / Member Variables
- : Optional regular expression pattern to filter publications by name. If NULL, all publications are described.

## Dependencies
- Functions called/Symbols referenced:
  - [formatPGVersionNumber](../f/formatPGVersionNumber.md)
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md)
  - [validateSQLNamePattern](../v/validateSQLNamePattern.md)
  - [termPQExpBuffer](../t/termPQExpBuffer.md)
  - [PSQLexec](../P/PSQLexec.md)
  - [printTableInit](../p/printTableInit.md)
  - [printTableAddHeader](../p/printTableAddHeader.md)
  - [printTableAddCell](../p/printTableAddCell.md)
  - [addFooterToPublicationDesc](../a/addFooterToPublicationDesc.md)
  - [printTable](../p/printTable.md)
  - [printTableCleanup](../p/printTableCleanup.md)
- Called from (representative examples):
  - [exec_command_d](../e/exec_command_d.md) (in command.c for \dRp+ command processing)

## Notes and Other Information
- Requires PostgreSQL 10.0 or later (publications were introduced in version 10)
- Creates separate detailed descriptions for each publication found
- Uses helper function  to format table and schema lists
- Handles version-specific features gracefully:
  - Column-level publications (PostgreSQL 15+)
  - WHERE clause filtering (PostgreSQL 15+) 
  - Schema-level publications (PostgreSQL 15+)
  - Truncate operation support (PostgreSQL 11+)
  - Via root partitioning (PostgreSQL 13+)
- Returns false and displays error message if no publications are found
- Uses proper error handling with cleanup for partial failures
- Each publication is displayed as a separate table with footer sections for related objects