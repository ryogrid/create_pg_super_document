# dumpPublicationNamespace

## Location
[src/bin/pg_dump/pg_dump.c:4654-4696](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L4654-L4696)

## Overview
Generates SQL commands to recreate publication schema mappings by producing ALTER PUBLICATION statements that add tables in schemas to publications.

## Definition

```c
static void
dumpPublicationNamespace(Archive *fout, const PublicationSchemaInfo *pubsinfo)
```
## Detailed Description
This function generates the SQL DDL necessary to recreate a publication's schema membership. It creates an  statement that adds all tables in a specific schema to a publication. The function is part of pg_dump's output generation phase and creates archive entries that will be written to the dump file.

The function skips operation in data-only dumps since schema membership is a structural definition. It creates archive entries in the SECTION_POST_DATA section to ensure proper ordering during restore operations.

## Parameters / Member Variables
- `*fout`: Archive structure for writing dump output
- `*pubsinfo`: Publication schema info containing the relationship details between publication and schema
## Dependencies
- Functions called/Symbols referenced:
  - [psprintf](../p/psprintf.md) - formats strings safely
  - [fmtId](../f/fmtId.md) - formats identifiers safely for SQL output
  - [ArchiveEntry](../A/ArchiveEntry.md) - creates an archive entry for the dump
  - [createPQExpBuffer](../c/createPQExpBuffer.md)/destroyPQExpBuffer - manages query buffers
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md) - builds SQL statements
- Called from (representative examples):
  - [dumpDumpableObject](dumpDumpableObject.md) - [main](../m/main.md) dispatcher for dumping various object types

## Notes and Other Information
- Only operates when DUMP_COMPONENT_DEFINITION flag is set
- Skipped entirely in data-only dumps (--data-only option)
- No drop statement is generated since schema drops handle this automatically
- Creates archive entries in SECTION_POST_DATA for proper restore ordering
- [Publication](../P/Publication.md) schema objects cannot currently have comments or security labels
- Uses the publication owner's role name for the archive entry