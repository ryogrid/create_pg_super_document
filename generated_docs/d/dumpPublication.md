# dumpPublication

## Location
[src/bin/pg_dump/pg_dump.c:4339-4434](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L4339-L4434)

## Overview
Generates the SQL statements to recreate a logical replication publication during pg_dump restore operations.

## Definition


## Detailed Description
The `dumpPublication` function creates the SQL DDL statements needed to recreate publications during database restoration. It constructs a comprehensive "CREATE PUBLICATION" statement that includes:

- Publication name
- Table scope ("FOR ALL TABLES" if `puballtables` is true)
- Publishing options specifying which DML operations to replicate (insert, update, delete, truncate)
- Partition root publishing behavior (publish_via_partition_root) for PostgreSQL 13+

The function dynamically builds the publish parameter list based on the boolean flags in the PublicationInfo structure, ensuring only the enabled operations are included in the output. It also generates the corresponding DROP statement for cleanup during restoration.

## Parameters / Member Variables
- `fout`: Archive pointer for output operations and dump options
- `pubinfo`: PublicationInfo structure containing publication details including:
  - `dobj.name`: Publication name
  - `rolname`: Publication owner
  - `puballtables`: Whether publication includes all tables
  - `pubinsert`: Whether to publish INSERT operations
  - `pubupdate`: Whether to publish UPDATE operations
  - `pubdelete`: Whether to publish DELETE operations
  - `pubtruncate`: Whether to publish TRUNCATE operations
  - `pubviaroot`: Whether to publish via partition root

## Dependencies
- Functions called/Symbols referenced:
  - `DumpOptions` (data structure)
  - `createPQExpBuffer`, `appendPQExpBuffer` series (query building)
  - [fmtId](../f/fmtId.md) (identifier formatting)
  - [ArchiveEntry](../A/ArchiveEntry.md) (archive entry creation)
  - [dumpComment](dumpComment.md), `dumpSecLabel` (auxiliary object dumping)
  - `DUMP_COMPONENT_DEFINITION`, `DUMP_COMPONENT_COMMENT`, `DUMP_COMPONENT_SECLABEL` (component flags)
  - `SECTION_POST_DATA` (archive section)
- Called from (representative examples):
  - [dumpDumpableObject](dumpDumpableObject.md) (main dump dispatch function)

## Notes and Other Information
- Skips processing entirely in data-only dump mode
- Builds the publish parameter list dynamically, including only enabled operations
- Creates archive entries in SECTION_POST_DATA section for proper restoration order
- Handles comments and security labels associated with the publication
- Part of PostgreSQL's logical replication infrastructure backup and restore system
- Publications are a PostgreSQL 10+ feature for defining logical replication scope and behavior
- The generated SQL includes proper parameter formatting and escaping for safe restoration