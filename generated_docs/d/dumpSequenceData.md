# dumpSequenceData

## Location
[src/bin/pg_dump/pg_dump.c:17843-17892](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L17843-L17892)

## Overview
Writes the current data (value state) of one user-defined sequence using SQL setval() function calls.

## Definition

```c
static void
dumpSequenceData(Archive *fout, const TableDataInfo *tdinfo)
```
## Detailed Description
The  function generates SQL statements to restore the current state of a sequence, specifically its last value and whether it has been called. It queries the sequence to retrieve  and  from the sequence relation, then creates a  call that will restore these values when the dump is loaded. This ensures that sequences maintain their proper state across dump/restore operations, preventing duplicate key violations or other issues that could arise from sequences starting over from their initial values.

## Parameters / Member Variables
- `*fout`: Archive structure containing dump options and output methods
- `*tdinfo`: TableDataInfo structure containing sequence data metadata and reference to the underlying TableInfo
## Dependencies
- Functions called/Symbols referenced:
  - fmtQualifiedDumpable
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - [createPQExpBuffer](../c/createPQExpBuffer.md)/resetPQExpBuffer/destroyPQExpBuffer
  - appendStringLiteralAH
  - [createDumpId](../c/createDumpId.md)
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - ngettext
- Called from (representative examples):
  - [dumpDumpableObject](dumpDumpableObject.md)

## Notes and Other Information
- Generates setval() calls with three parameters: sequence name, last value, and is_called flag
- The is_called flag determines whether the next call to nextval() will increment the sequence or return the current last_value
- Creates a separate archive entry in the SECTION_DATA section, ensuring sequence data is restored after sequence definitions
- Uses proper SQL literal escaping through appendStringLiteralAH for sequence names
- Depends on the sequence definition being restored first (handled via dependency tracking)

## Simplified Source

```c
static void
dumpSequenceData(Archive *fout, const TableDataInfo *tdinfo)
{
    TableInfo *tbinfo = tdinfo->tdtable;
    PGresult *res;
    char *last;
    bool called;
    PQExpBuffer query = createPQExpBuffer();

    // Query sequence current state
    appendPQExpBuffer(query, "SELECT last_value, is_called FROM %s", fmtQualifiedDumpable(tbinfo));

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

    if (PQntuples(res) != 1)
        pg_fatal("query to get data of sequence \"%s\" returned %d rows (expected 1)",
                tbinfo->dobj.name, PQntuples(res));

    // Extract current sequence values
    last = PQgetvalue(res, 0, 0);
    called = (strcmp(PQgetvalue(res, 0, 1), "t") == 0);

    // Generate setval() call to restore sequence state
    resetPQExpBuffer(query);
    appendPQExpBufferStr(query, "SELECT pg_catalog.setval(");
    appendStringLiteralAH(query, fmtQualifiedDumpable(tbinfo), fout);
    appendPQExpBuffer(query, ", %s, %s);\n", last, (called ? "true" : "false"));

    // Create archive entry for sequence data
    if (tdinfo->dobj.dump & DUMP_COMPONENT_DATA)
        ArchiveEntry(fout, nilCatalogId, createDumpId(),
                   ARCHIVE_OPTS(.tag = tbinfo->dobj.name,
                               .namespace = tbinfo->dobj.namespace->dobj.name,
                               .owner = tbinfo->rolname,
                               .description = "SEQUENCE SET",
                               .section = SECTION_DATA,
                               .createStmt = query->data,
                               .deps = &(tbinfo->dobj.dumpId),
                               .nDeps = 1));

    PQclear(res);
    destroyPQExpBuffer(query);
}
```