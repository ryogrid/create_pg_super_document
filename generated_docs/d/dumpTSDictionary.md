# dumpTSDictionary

## Location
[src/bin/pg_dump/pg_dump.c:14651-14730](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L14651-L14730)

## Overview
Writes out a single text search dictionary definition to the PostgreSQL dump output, generating the necessary CREATE TEXT SEARCH DICTIONARY statement with template and initialization options.

## Definition

```c
static void
dumpTSDictionary(Archive *fout, const TSDictInfo *dictinfo)
```
## Detailed Description
The  function is responsible for dumping text search dictionary objects during a pg_dump operation. It generates the CREATE TEXT SEARCH DICTIONARY statement by fetching the dictionary's template information from the database and including any initialization options. The function constructs both creation and drop statements, handles binary upgrade scenarios, and dumps associated comments and ownership information.

The function performs a database query to retrieve the template namespace and name from pg_ts_template and pg_namespace system catalogs to properly reference the dictionary's template in the dump output.

## Parameters / Member Variables
- `*fout`: Archive structure containing dump configuration and output methods
- `*dictinfo`: TSDictInfo structure containing dictionary metadata including template OID, initialization options, and ownership information
## Dependencies
- Functions called/Symbols referenced:
  - [createPQExpBuffer](../c/createPQExpBuffer.md)
  - [destroyPQExpBuffer](destroyPQExpBuffer.md)
  - [pg_strdup](../p/pg_strdup.md)
  - [fmtId](../f/fmtId.md)
  - fmtQualifiedDumpable
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [ExecuteSqlQueryForSingleRow](../E/ExecuteSqlQueryForSingleRow.md)
  - [PQgetvalue](../P/PQgetvalue.md)
  - [PQclear](../P/PQclear.md)
  - [binary_upgrade_extension_member](../b/binary_upgrade_extension_member.md)
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [dumpComment](dumpComment.md)
  - free
- Called from (representative examples):
  - [dumpDumpableObject](dumpDumpableObject.md) (via switch statement for DO_TSDICT objects)

## Notes and Other Information
- Only executes during schema dumps (skipped when dopt->dataOnly is true)
- Queries the database to resolve template references to fully qualified template names
- Handles optional initialization parameters (dictinitoption) when present
- Supports binary upgrade mode with appropriate extension member handling
- Includes owner information in the archive entry for proper ownership restoration
- Generates both CREATE and DROP statements for complete dump/restore capability
- Part of PostgreSQL's text search infrastructure dumping functionality
- Uses qualified names to handle schema-qualified dictionary and template names properly

## Simplified Source

```c
static void
dumpTSDictionary(Archive *fout, const TSDictInfo *dictinfo)
{
    DumpOptions *dopt = fout->dopt;
    PQExpBuffer q;
    PQExpBuffer delq;
    PQExpBuffer query;
    char       *qdictname;
    PGresult   *res;
    char       *nspname;
    char       *tmplname;

    // Skip in data-only dump mode
    if (dopt->dataOnly)
        return;

    q = createPQExpBuffer();
    delq = createPQExpBuffer();
    query = createPQExpBuffer();

    qdictname = pg_strdup(fmtId(dictinfo->dobj.name));

    // Fetch template namespace and name
    appendPQExpBuffer(query, "SELECT nspname, tmplname "
                      "FROM pg_ts_template p, pg_namespace n "
                      "WHERE p.oid = '%u' AND n.oid = tmplnamespace",
                      dictinfo->dicttemplate);
    res = ExecuteSqlQueryForSingleRow(fout, query->data);
    nspname = PQgetvalue(res, 0, 0);
    tmplname = PQgetvalue(res, 0, 1);

    // Build CREATE TEXT SEARCH DICTIONARY statement
    appendPQExpBuffer(q, "CREATE TEXT SEARCH DICTIONARY %s (\n",
                      fmtQualifiedDumpable(dictinfo));

    appendPQExpBufferStr(q, "    TEMPLATE = ");
    appendPQExpBuffer(q, "%s.", fmtId(nspname));
    appendPQExpBufferStr(q, fmtId(tmplname));

    PQclear(res);

    // Add initialization options if present
    if (dictinfo->dictinitoption)
        appendPQExpBuffer(q, ",\n    %s", dictinfo->dictinitoption);

    appendPQExpBufferStr(q, " );\n");

    // Build DROP statement
    appendPQExpBuffer(delq, "DROP TEXT SEARCH DICTIONARY %s;\n",
                      fmtQualifiedDumpable(dictinfo));

    // Handle binary upgrade mode
    if (dopt->binary_upgrade)
        binary_upgrade_extension_member(q, &dictinfo->dobj,
                                        "TEXT SEARCH DICTIONARY", qdictname,
                                        dictinfo->dobj.namespace->dobj.name);

    // Archive the dictionary definition
    if (dictinfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
        ArchiveEntry(fout, dictinfo->dobj.catId, dictinfo->dobj.dumpId,
                     ARCHIVE_OPTS(.tag = dictinfo->dobj.name,
                                  .namespace = dictinfo->dobj.namespace->dobj.name,
                                  .owner = dictinfo->rolname,
                                  .description = "TEXT SEARCH DICTIONARY",
                                  .section = SECTION_PRE_DATA,
                                  .createStmt = q->data,
                                  .dropStmt = delq->data));

    // Dump dictionary comments
    if (dictinfo->dobj.dump & DUMP_COMPONENT_COMMENT)
        dumpComment(fout, "TEXT SEARCH DICTIONARY", qdictname,
                    dictinfo->dobj.namespace->dobj.name, dictinfo->rolname,
                    dictinfo->dobj.catId, 0, dictinfo->dobj.dumpId);

    // Cleanup
    destroyPQExpBuffer(q);
    destroyPQExpBuffer(delq);
    destroyPQExpBuffer(query);
    free(qdictname);
}
```