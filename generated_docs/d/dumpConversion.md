# dumpConversion

## Location
[src/bin/pg_dump/pg_dump.c:14099-14194](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L14099-L14194)

## Overview
Writes out a single conversion definition, generating CREATE CONVERSION SQL statements for character encoding transformations between different encodings.

## Definition

```c
static void
dumpConversion(Archive *fout, const ConvInfo *convinfo)
```
## Detailed Description
The  function generates SQL commands to recreate encoding conversion objects during database dumps. It queries the pg_conversion catalog to retrieve conversion properties including source encoding, target encoding, conversion function, and default status. The function constructs CREATE CONVERSION statements with proper encoding names obtained via  system function.

The function handles both regular and default conversions - default conversions are automatically selected when converting between specific encoding pairs. The conversion function (typically a C function) performs the actual character encoding transformation.

## Parameters / Member Variables
- `*fout`: Archive structure containing dump options and output methods
- `*convinfo`: ConvInfo structure containing conversion metadata including OID, name, namespace, and owner
## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlQueryForSingleRow](../E/ExecuteSqlQueryForSingleRow.md)
  - fmtQualifiedDumpable
  - [fmtId](../f/fmtId.md)
  - appendStringLiteralAH
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [dumpComment](dumpComment.md)
  - [binary_upgrade_extension_member](../b/binary_upgrade_extension_member.md)
- Called from (representative examples):
  - [dumpDumpableObject](dumpDumpableObject.md)

## Notes and Other Information
- Only operates in schema dump mode (skipped when dopt->dataOnly is true)
- Uses PostgreSQL built-in functions  to resolve encoding names
- Supports both regular and DEFAULT conversions (automatic selection for encoding pairs)
- The conversion procedure (conproc) is output as regproc which includes proper quoting
- Generates proper DROP statements for clean restoration
- Includes comment dumping if enabled in dump options
- Supports binary upgrade scenarios

## Simplified Source

```c
static void
dumpConversion(Archive *fout, const ConvInfo *convinfo)
{
    DumpOptions *dopt = fout->dopt;
    PQExpBuffer query, q, delq;
    char *qconvname;
    PGresult *res;
    const char *conforencoding, *contoencoding, *conproc;
    bool condefault;

    // Skip in data-only mode
    if (dopt->dataOnly)
        return;

    // Initialize buffers and format conversion name
    query = createPQExpBuffer();
    q = createPQExpBuffer();
    delq = createPQExpBuffer();
    qconvname = pg_strdup(fmtId(convinfo->dobj.name));

    // Query conversion properties
    appendPQExpBuffer(query, "SELECT "
                      "pg_catalog.pg_encoding_to_char(conforencoding) AS conforencoding, "
                      "pg_catalog.pg_encoding_to_char(contoencoding) AS contoencoding, "
                      "conproc, condefault "
                      "FROM pg_catalog.pg_conversion c "
                      "WHERE c.oid = '%u'::pg_catalog.oid",
                      convinfo->dobj.catId.oid);

    res = ExecuteSqlQueryForSingleRow(fout, query->data);

    // Extract conversion properties
    conforencoding = PQgetvalue(res, 0, PQfnumber(res, "conforencoding"));
    contoencoding = PQgetvalue(res, 0, PQfnumber(res, "contoencoding"));
    conproc = PQgetvalue(res, 0, PQfnumber(res, "conproc"));
    condefault = (PQgetvalue(res, 0, PQfnumber(res, "condefault"))[0] == 't');

    // Build DROP statement
    appendPQExpBuffer(delq, "DROP CONVERSION %s;\n",
                      fmtQualifiedDumpable(convinfo));

    // Build CREATE statement
    appendPQExpBuffer(q, "CREATE %sCONVERSION %s FOR ",
                      condefault ? "DEFAULT " : "",
                      fmtQualifiedDumpable(convinfo));

    // Add source encoding
    appendStringLiteralAH(q, conforencoding, fout);
    appendPQExpBufferStr(q, " TO ");

    // Add target encoding
    appendStringLiteralAH(q, contoencoding, fout);

    // Add conversion function (regproc output is already quoted)
    appendPQExpBuffer(q, " FROM %s;\n", conproc);

    // Handle binary upgrade
    if (dopt->binary_upgrade)
        binary_upgrade_extension_member(q, &convinfo->dobj,
                                       "CONVERSION", qconvname,
                                       convinfo->dobj.namespace->dobj.name);

    // Register with archive
    if (convinfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
        ArchiveEntry(fout, convinfo->dobj.catId, convinfo->dobj.dumpId,
                    ARCHIVE_OPTS(.tag = convinfo->dobj.name,
                                .namespace = convinfo->dobj.namespace->dobj.name,
                                .owner = convinfo->rolname,
                                .description = "CONVERSION",
                                .section = SECTION_PRE_DATA,
                                .createStmt = q->data,
                                .dropStmt = delq->data));

    // Dump comments
    if (convinfo->dobj.dump & DUMP_COMPONENT_COMMENT)
        dumpComment(fout, "CONVERSION", qconvname,
                   convinfo->dobj.namespace->dobj.name, convinfo->rolname,
                   convinfo->dobj.catId, 0, convinfo->dobj.dumpId);

    // Cleanup
    PQclear(res);
    destroyPQExpBuffer(query);
    destroyPQExpBuffer(q);
    destroyPQExpBuffer(delq);
    free(qconvname);
}
```