# dumpCompositeTypeColComments

## Location
[src/bin/pg_dump/pg_dump.c:11993-12081](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L11993-L12081)

## Overview
The dumpCompositeTypeColComments function generates COMMENT ON COLUMN statements for the attributes of a user-defined composite type.

## Definition

```c
static void
dumpCompositeTypeColComments(Archive *fout, const TypeInfo *tyinfo,
							 PGresult *res)
```
## Detailed Description
This function processes comments associated with the columns of a composite type and generates appropriate COMMENT ON COLUMN statements. It takes advantage of a pre-existing query result containing column information to avoid re-querying the database. The function searches for comments associated with the type's pg_class OID and matches them with column attribute numbers to generate properly formatted comment statements.

The function creates archive entries for each column comment with proper dependencies to ensure they are applied after the type creation. Comments for dropped columns are ignored to maintain consistency with the type structure.

## Parameters / Member Variables
- `*fout`: Archive handle for the dump output stream
- `*tyinfo`: TypeInfo structure containing metadata about the composite type
- `*res`: PGresult containing pre-queried column information (attnum, attname, attisdropped)
## Dependencies
- Functions called/Symbols referenced:
  - [findComments](../f/findComments.md)
  - [fmtId](../f/fmtId.md)
  - fmtQualifiedDumpable
  - appendStringLiteralAH
  - [createDumpId](../c/createDumpId.md)
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [resetPQExpBuffer](../r/resetPQExpBuffer.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
- Called from (representative examples):
  - [dumpCompositeType](dumpCompositeType.md)

## Notes and Other Information
- Returns early if --no-comments option is specified in dump options
- Uses RelationRelationId to search for comments since composite types have associated pg_class entries
- Comments are archived in SECTION_NONE with dependencies on the parent type
- The function reuses query results from the caller to avoid redundant database queries
- Skips comments for dropped columns to maintain dump consistency