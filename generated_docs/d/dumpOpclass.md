# dumpOpclass

## Location
[src/bin/pg_dump/pg_dump.c:13342-13622](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L13342-L13622)

## Overview
Writes out a complete operator class definition including its operators and functions, generating CREATE OPERATOR CLASS and DROP OPERATOR CLASS statements for pg_dump output.

## Definition

```c
static void
dumpOpclass(Archive *fout, const OpclassInfo *opcinfo)
```
## Detailed Description
This function generates a comprehensive CREATE OPERATOR CLASS statement by querying the PostgreSQL system catalogs to retrieve all associated operators and functions. It constructs the complete operator class definition including the data type, access method, optional operator family, storage type, operator entries with strategy numbers, and function entries with procedure numbers.

The function handles complex relationships between operator classes and their components by joining multiple system catalog tables (pg_opclass, pg_amop, pg_amproc, pg_opfamily, pg_depend) to ensure only relevant operators and functions tied to the specific operator class are included. It properly formats operator and function references, handles cross-type comparisons, and includes sorting operator families when specified.

## Parameters / Member Variables
- `*fout`: Archive handle containing dump options and database connection
- `*opcinfo`: OpclassInfo structure containing operator class metadata including OID, name, namespace, and role information
## Dependencies
- Functions called/Symbols referenced:
  - [createPQExpBuffer](../c/createPQExpBuffer.md)/destroyPQExpBuffer (for SQL statement building)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)/appendPQExpBufferStr/resetPQExpBuffer (for statement construction)
  - [ExecuteSqlQueryForSingleRow](../E/ExecuteSqlQueryForSingleRow.md)/ExecuteSqlQuery (for catalog queries)
  - [PQfnumber](../P/PQfnumber.md)/PQgetvalue/PQntuples/PQclear (for result processing)
  - [pg_strdup](../p/pg_strdup.md)/free (for memory management)
  - [fmtId](../f/fmtId.md)/fmtQualifiedDumpable (for identifier formatting)
  - [binary_upgrade_extension_member](../b/binary_upgrade_extension_member.md) (for binary upgrade support)
  - [ArchiveEntry](../A/ArchiveEntry.md) (to register dump entry)
  - [dumpComment](dumpComment.md) (to handle operator class comments)
- Called from (representative examples):
  - [dumpDumpableObject](dumpDumpableObject.md) (as part of general object dumping)
  - fmtQualifiedDumpable

## Notes and Other Information
- Skips execution in data-only dump mode
- Handles DEFAULT operator classes with special formatting
- Retrieves and formats STORAGE clause when key type differs from input type
- Processes OPERATOR entries with strategy numbers and optional sort operator families
- Processes FUNCTION entries with procedure numbers and explicit type specifications for cross-type comparisons
- Includes fallback STORAGE clause to avoid generating invalid SQL when no operators or functions exist
- Supports binary upgrade scenarios with proper extension member handling
- Generates both creation and deletion statements for complete dump/restore cycle
- Handles operator class comments as separate dump components
- Uses dependency relationships to ensure only relevant operators and functions are included
- Part of PostgreSQL's pg_dump utility for comprehensive schema export

## Simplified Source

```c
static void
dumpOpclass(Archive *fout, const OpclassInfo *opcinfo)
{
    DumpOptions *dopt = fout->dopt;
    PQExpBuffer query, q, delq, nameusing;
    PGresult *res;
    char *opcintype, *opckeytype, *opcdefault;
    char *opcfamily, *opcfamilyname, *opcfamilynsp, *amname;
    bool needComma = false;
    int ntups, i;

    // Skip in data-only mode
    if (dopt->dataOnly)
        return;

    // Initialize all buffers
    query = createPQExpBuffer();
    q = createPQExpBuffer();
    delq = createPQExpBuffer();
    nameusing = createPQExpBuffer();

    // Query operator class basic information
    appendPQExpBuffer(query, "SELECT opcintype::pg_catalog.regtype, "
                      "opckeytype::pg_catalog.regtype, "
                      "opcdefault, opcfamily, "
                      "opfname AS opcfamilyname, "
                      "nspname AS opcfamilynsp, "
                      "(SELECT amname FROM pg_catalog.pg_am WHERE oid = opcmethod) AS amname "
                      "FROM pg_catalog.pg_opclass c "
                      "LEFT JOIN pg_catalog.pg_opfamily f ON f.oid = opcfamily "
                      "LEFT JOIN pg_catalog.pg_namespace n ON n.oid = opfnamespace "
                      "WHERE c.oid = '%u'::pg_catalog.oid",
                      opcinfo->dobj.catId.oid);

    res = ExecuteSqlQueryForSingleRow(fout, query->data);

    // Extract basic properties
    opcintype = pg_strdup(PQgetvalue(res, 0, PQfnumber(res, "opcintype")));
    opckeytype = PQgetvalue(res, 0, PQfnumber(res, "opckeytype"));
    opcdefault = PQgetvalue(res, 0, PQfnumber(res, "opcdefault"));
    opcfamily = pg_strdup(PQgetvalue(res, 0, PQfnumber(res, "opcfamily")));
    opcfamilyname = PQgetvalue(res, 0, PQfnumber(res, "opcfamilyname"));
    opcfamilynsp = PQgetvalue(res, 0, PQfnumber(res, "opcfamilynsp"));
    amname = pg_strdup(PQgetvalue(res, 0, PQfnumber(res, "amname")));

    // Build DROP statement
    appendPQExpBuffer(delq, "DROP OPERATOR CLASS %s USING %s;\n",
                      fmtQualifiedDumpable(opcinfo), fmtId(amname));

    // Build CREATE statement header
    appendPQExpBuffer(q, "CREATE OPERATOR CLASS %s\n    ",
                      fmtQualifiedDumpable(opcinfo));
    if (strcmp(opcdefault, "t") == 0)
        appendPQExpBufferStr(q, "DEFAULT ");
    appendPQExpBuffer(q, "FOR TYPE %s USING %s", opcintype, fmtId(amname));

    // Add family if specified
    if (strlen(opcfamilyname) > 0) {
        appendPQExpBuffer(q, " FAMILY %s.%s",
                         fmtId(opcfamilynsp), fmtId(opcfamilyname));
    }
    appendPQExpBufferStr(q, " AS\n    ");

    // Add STORAGE clause if key type differs
    if (strcmp(opckeytype, "-") != 0) {
        appendPQExpBuffer(q, "STORAGE %s", opckeytype);
        needComma = true;
    }
    PQclear(res);

    // Query and add OPERATOR entries
    resetPQExpBuffer(query);
    appendPQExpBuffer(query, "SELECT amopstrategy, "
                      "amopopr::pg_catalog.regoperator, "
                      "opfname AS sortfamily, nspname AS sortfamilynsp "
                      "FROM pg_catalog.pg_amop ao JOIN pg_catalog.pg_depend ON "
                      "(classid = 'pg_catalog.pg_amop'::pg_catalog.regclass AND objid = ao.oid) "
                      "LEFT JOIN pg_catalog.pg_opfamily f ON f.oid = amopsortfamily "
                      "LEFT JOIN pg_catalog.pg_namespace n ON n.oid = opfnamespace "
                      "WHERE refclassid = 'pg_catalog.pg_opclass'::pg_catalog.regclass "
                      "AND refobjid = '%u'::pg_catalog.oid "
                      "AND amopfamily = '%s'::pg_catalog.oid "
                      "ORDER BY amopstrategy",
                      opcinfo->dobj.catId.oid, opcfamily);

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    ntups = PQntuples(res);

    // Add each operator
    for (i = 0; i < ntups; i++) {
        char *amopstrategy = PQgetvalue(res, i, PQfnumber(res, "amopstrategy"));
        char *amopopr = PQgetvalue(res, i, PQfnumber(res, "amopopr"));
        char *sortfamily = PQgetvalue(res, i, PQfnumber(res, "sortfamily"));
        char *sortfamilynsp = PQgetvalue(res, i, PQfnumber(res, "sortfamilynsp"));

        if (needComma)
            appendPQExpBufferStr(q, " ,\n    ");

        appendPQExpBuffer(q, "OPERATOR %s %s", amopstrategy, amopopr);

        // Add sort family if specified
        if (strlen(sortfamily) > 0) {
            appendPQExpBuffer(q, " FOR ORDER BY %s.%s",
                             fmtId(sortfamilynsp), fmtId(sortfamily));
        }
        needComma = true;
    }
    PQclear(res);

    // Query and add FUNCTION entries
    resetPQExpBuffer(query);
    appendPQExpBuffer(query, "SELECT amprocnum, "
                      "amproc::pg_catalog.regprocedure, "
                      "amproclefttype::pg_catalog.regtype, "
                      "amprocrighttype::pg_catalog.regtype "
                      "FROM pg_catalog.pg_amproc ap, pg_catalog.pg_depend "
                      "WHERE refclassid = 'pg_catalog.pg_opclass'::pg_catalog.regclass "
                      "AND refobjid = '%u'::pg_catalog.oid "
                      "AND classid = 'pg_catalog.pg_amproc'::pg_catalog.regclass "
                      "AND objid = ap.oid "
                      "ORDER BY amprocnum",
                      opcinfo->dobj.catId.oid);

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    ntups = PQntuples(res);

    // Add each function
    for (i = 0; i < ntups; i++) {
        char *amprocnum = PQgetvalue(res, i, PQfnumber(res, "amprocnum"));
        char *amproc = PQgetvalue(res, i, PQfnumber(res, "amproc"));
        char *amproclefttype = PQgetvalue(res, i, PQfnumber(res, "amproclefttype"));
        char *amprocrighttype = PQgetvalue(res, i, PQfnumber(res, "amprocrighttype"));

        if (needComma)
            appendPQExpBufferStr(q, " ,\n    ");

        appendPQExpBuffer(q, "FUNCTION %s", amprocnum);

        // Add type specification for cross-type comparisons
        if (*amproclefttype && *amprocrighttype)
            appendPQExpBuffer(q, " (%s, %s)", amproclefttype, amprocrighttype);

        appendPQExpBuffer(q, " %s", amproc);
        needComma = true;
    }
    PQclear(res);

    // Add fallback STORAGE clause if nothing was added
    if (!needComma)
        appendPQExpBuffer(q, "STORAGE %s", opcintype);

    appendPQExpBufferStr(q, ";\n");

    // Build nameusing for comments and binary upgrade
    appendPQExpBuffer(nameusing, "%s USING %s",
                      fmtId(opcinfo->dobj.name), fmtId(amname));

    // Handle binary upgrade
    if (dopt->binary_upgrade)
        binary_upgrade_extension_member(q, &opcinfo->dobj,
                                       "OPERATOR CLASS", nameusing->data,
                                       opcinfo->dobj.namespace->dobj.name);

    // Register with archive
    if (opcinfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
        ArchiveEntry(fout, opcinfo->dobj.catId, opcinfo->dobj.dumpId,
                    ARCHIVE_OPTS(.tag = opcinfo->dobj.name,
                                .namespace = opcinfo->dobj.namespace->dobj.name,
                                .owner = opcinfo->rolname,
                                .description = "OPERATOR CLASS",
                                .section = SECTION_PRE_DATA,
                                .createStmt = q->data,
                                .dropStmt = delq->data));

    // Dump comments
    if (opcinfo->dobj.dump & DUMP_COMPONENT_COMMENT)
        dumpComment(fout, "OPERATOR CLASS", nameusing->data,
                   opcinfo->dobj.namespace->dobj.name, opcinfo->rolname,
                   opcinfo->dobj.catId, 0, opcinfo->dobj.dumpId);

    // Cleanup
    free(opcintype);
    free(opcfamily);
    free(amname);
    destroyPQExpBuffer(query);
    destroyPQExpBuffer(q);
    destroyPQExpBuffer(delq);
    destroyPQExpBuffer(nameusing);
}
```