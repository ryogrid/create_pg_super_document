# dumpTableSchema

## Location
[src/bin/pg_dump/pg_dump.c:15946-16807](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L15946-L16807)

## Overview
Generates the SQL declaration (schema definition) for a user-defined table, view, materialized view, foreign table, or partitioned table, handling all structural elements without data content.

## Definition


## Detailed Description
This is a comprehensive function that constructs CREATE TABLE, CREATE VIEW, CREATE MATERIALIZED VIEW, or CREATE FOREIGN TABLE statements with all associated properties. It handles diverse table types and their specific requirements:

- **Views**: Creates standard or dummy views with column specifications and CHECK OPTION clauses
- **Tables**: Regular, partitioned, foreign, and materialized tables with complete attribute definitions
- **Binary upgrade mode**: Special handling for maintaining exact compatibility during upgrades
- **Inheritance**: Processes parent-child relationships and inherited constraints  
- **Advanced features**: Handles storage parameters, statistics targets, compression, replica identity, row-level security, and tablespaces

The function generates both CREATE and DROP statements, manages column properties (types, defaults, NOT NULL, collation), processes constraints, and handles special cases for dropped columns and binary upgrade scenarios.

## Parameters / Member Variables
- : Archive context containing dump configuration and output handling
- : Complete table metadata including columns, constraints, inheritance, and storage properties

## Dependencies
- Functions called/Symbols referenced:
  - [createDummyViewAsClause](../c/createDummyViewAsClause.md)
  - [createViewAsClause](../c/createViewAsClause.md)
  - [fmtId](../f/fmtId.md)
  - fmtQualifiedDumpable
  - [binary_upgrade_set_type_oids_by_rel](../b/binary_upgrade_set_type_oids_by_rel.md)
  - [binary_upgrade_set_pg_class_oids](../b/binary_upgrade_set_pg_class_oids.md)
  - [ExecuteSqlQueryForSingleRow](../E/ExecuteSqlQueryForSingleRow.md)
  - [getFormattedTypeName](../g/getFormattedTypeName.md)
  - [shouldPrintColumn](../s/shouldPrintColumn.md)
  - [findCollationByOid](../f/findCollationByOid.md)
  - [appendReloptionsArrayAH](../a/appendReloptionsArrayAH.md)
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [dumpTableComment](dumpTableComment.md)
  - [dumpTableSecLabel](dumpTableSecLabel.md)
- Types referenced:
  - [Archive](../A/Archive.md)
  - [TableInfo](../T/TableInfo.md)
  - DumpOptions
  - [CollInfo](../C/CollInfo.md)
  - [ConstraintInfo](../C/ConstraintInfo.md)
  - PQExpBuffer
- Called from:
  - [dumpTable](dumpTable.md)

## Notes and Other Information
- Handles complex inheritance hierarchies and constraint propagation
- Special logic for binary upgrade mode to preserve exact database structure
- Manages tablespace assignments and access method specifications
- Processes replica identity settings for logical replication
- Creates appropriate dependency relationships for proper restore ordering
- Handles both regular and dummy view creation to resolve circular dependencies
- Supports PostgreSQL-specific features like row-level security and generated columns