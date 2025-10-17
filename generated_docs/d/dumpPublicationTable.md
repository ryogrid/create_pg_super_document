# dumpPublicationTable

## Location
[src/bin/pg_dump/pg_dump.c:4697-4757](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L4697-L4757)

## Overview
Generates SQL commands to recreate publication table mappings by producing ALTER PUBLICATION statements that add specific tables to publications with optional column lists and row filters.

## Definition

```c
static void
dumpPublicationTable(Archive *fout, const PublicationRelInfo *pubrinfo)
```
## Detailed Description
This function generates the SQL DDL necessary to recreate a publication's table membership. It creates an  statement that adds a specific table to a publication, including support for column lists and WHERE clause row filters when present. The function is part of pg_dump's output generation phase and creates archive entries that will be written to the dump file.

The function handles the complete syntax for table publication including optional column specifications and row-level filtering conditions. It ensures proper parentheses around WHERE expressions since pg_get_expr doesn't supply them for simple expressions like "WHERE TRUE".

## Parameters / Member Variables
- `*fout`: Archive structure for writing dump output
- `*pubrinfo`: Publication relation info containing the relationship details between publication and table
## Dependencies
- Functions called/Symbols referenced:
  - [psprintf](../p/psprintf.md) - formats strings safely
  - [fmtId](../f/fmtId.md) - formats identifiers safely for SQL output
  - fmtQualifiedDumpable - formats qualified table names for dump output
  - [ArchiveEntry](../A/ArchiveEntry.md) - creates an archive entry for the dump
  - [createPQExpBuffer](../c/createPQExpBuffer.md)/destroyPQExpBuffer - manages query buffers
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)/appendPQExpBufferStr - builds SQL statements
- Called from (representative examples):
  - [dumpDumpableObject](dumpDumpableObject.md) - [main](../m/main.md) dispatcher for dumping various object types

## Notes and Other Information
- Only operates when DUMP_COMPONENT_DEFINITION flag is set
- Skipped entirely in data-only dumps (--data-only option)
- No drop statement is generated since table drops handle this automatically
- Creates archive entries in SECTION_POST_DATA for proper restore ordering
- Supports column lists (pubrattrs) and row filters (pubrelqual) for PostgreSQL 15+
- [Publication](../P/Publication.md) table objects cannot currently have comments or security labels
- Uses the publication owner's role name for the archive entry to ensure correct restore permissions
- Adds parentheses around WHERE expressions to handle cases like "WHERE TRUE" properly

## Simplified Source

```c
static void dumpPublicationTable(Archive *fout, const PublicationRelInfo *pubrinfo) {
    PublicationInfo *pubinfo = pubrinfo->publication;
    TableInfo *tbinfo = pubrinfo->pubtable;
    PQExpBuffer query;
    char *tag;

    // Skip in data-only dumps
    if (fout->dopt->dataOnly)
        return;

    // Create descriptive tag for the archive entry
    tag = psprintf("%s %s", pubinfo->dobj.name, tbinfo->dobj.name);

    query = createPQExpBuffer();

    // Build ALTER PUBLICATION ADD TABLE statement
    appendPQExpBuffer(query, "ALTER PUBLICATION %s ADD TABLE ONLY",
                      fmtId(pubinfo->dobj.name));
    appendPQExpBuffer(query, " %s", fmtQualifiedDumpable(tbinfo));

    // Add column list if specified
    if (pubrinfo->pubrattrs)
        appendPQExpBuffer(query, " (%s)", pubrinfo->pubrattrs);

    // Add row filter if specified
    if (pubrinfo->pubrelqual) {
        // Add parentheses around expression (pg_get_expr doesn't provide them)
        appendPQExpBuffer(query, " WHERE (%s)", pubrinfo->pubrelqual);
    }
    appendPQExpBufferStr(query, ";\n");

    // Create archive entry if definition should be dumped
    if (pubrinfo->dobj.dump & DUMP_COMPONENT_DEFINITION) {
        ArchiveEntry(fout, pubrinfo->dobj.catId, pubrinfo->dobj.dumpId,
                     ARCHIVE_OPTS(.tag = tag,
                                  .namespace = tbinfo->dobj.namespace->dobj.name,
                                  .owner = pubinfo->rolname,
                                  .description = "PUBLICATION TABLE",
                                  .section = SECTION_POST_DATA,
                                  .createStmt = query->data));
    }

    // Cleanup
    free(tag);
    destroyPQExpBuffer(query);
}
```