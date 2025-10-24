# dumpOpfamily

## Location
[src/bin/pg_dump/pg_dump.c:13623-13841](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L13623-L13841)

## Overview
Writes out a single operator family definition along with any loose operator members that aren't bound to a specific opclass within the opfamily.

## Definition

```c
static void
dumpOpfamily(Archive *fout, const OpfamilyInfo *opfinfo)
```
## Detailed Description
The  function is responsible for generating SQL commands to recreate an operator family during database dumps. It constructs CREATE OPERATOR FAMILY and ALTER OPERATOR FAMILY statements to properly restore the operator family and its associated operators and support functions. The function queries the PostgreSQL catalog to fetch:

1. Operator members (pg_amop) tied directly to the opfamily
2. Support function members (pg_amproc) tied directly to the opfamily  
3. Access method information from pg_opfamily

The function generates both the CREATE command for the basic operator family definition and an ALTER command to add any loose operators and functions that are directly dependent on the family but not bound to specific operator classes.

## Parameters / Member Variables
- `*fout`: Archive structure containing dump options and output methods
- `*opfinfo`: OpfamilyInfo structure containing operator family metadata including OID, name, namespace, and owner
## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - [ExecuteSqlQueryForSingleRow](../E/ExecuteSqlQueryForSingleRow.md)
  - fmtQualifiedDumpable
  - [fmtId](../f/fmtId.md)
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [dumpComment](dumpComment.md)
  - [binary_upgrade_extension_member](../b/binary_upgrade_extension_member.md)
- Called from (representative examples):
  - [dumpDumpableObject](dumpDumpableObject.md)

## Notes and Other Information
- Only operates in schema dump mode (skipped when dopt->dataOnly is true)
- Handles both operators with optional ORDER BY clauses for sort families
- Generates proper DROP statements for clean restoration
- Supports binary upgrade scenarios
- Includes comment dumping if enabled in dump options
- Uses PQExpBuffer for efficient SQL string construction

## Simplified Source

```c
static void
dumpOpfamily(Archive *fout, const OpfamilyInfo *opfinfo)
{
    DumpOptions *dopt = fout->dopt;
    PQExpBuffer query, q, delq, nameusing;
    PGresult *res, *res_ops, *res_procs;
    char *amname;
    bool needComma = false;
    int ntups, i;

    // Skip in data-only mode
    if (dopt->dataOnly)
        return;

    // Initialize buffers
    query = createPQExpBuffer();
    q = createPQExpBuffer();
    delq = createPQExpBuffer();
    nameusing = createPQExpBuffer();

    // Query loose operator members tied directly to opfamily
    appendPQExpBuffer(query, "SELECT amopstrategy, "
                      "amopopr::pg_catalog.regoperator, "
                      "opfname AS sortfamily, nspname AS sortfamilynsp "
                      "FROM pg_catalog.pg_amop ao JOIN pg_catalog.pg_depend ON "
                      "(classid = 'pg_catalog.pg_amop'::pg_catalog.regclass AND objid = ao.oid) "
                      "LEFT JOIN pg_catalog.pg_opfamily f ON f.oid = amopsortfamily "
                      "LEFT JOIN pg_catalog.pg_namespace n ON n.oid = opfnamespace "
                      "WHERE refclassid = 'pg_catalog.pg_opfamily'::pg_catalog.regclass "
                      "AND refobjid = '%u'::pg_catalog.oid "
                      "AND amopfamily = '%u'::pg_catalog.oid "
                      "ORDER BY amopstrategy",
                      opfinfo->dobj.catId.oid, opfinfo->dobj.catId.oid);

    res_ops = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

    // Query loose function members tied directly to opfamily
    resetPQExpBuffer(query);
    appendPQExpBuffer(query, "SELECT amprocnum, "
                      "amproc::pg_catalog.regprocedure, "
                      "amproclefttype::pg_catalog.regtype, "
                      "amprocrighttype::pg_catalog.regtype "
                      "FROM pg_catalog.pg_amproc ap, pg_catalog.pg_depend "
                      "WHERE refclassid = 'pg_catalog.pg_opfamily'::pg_catalog.regclass "
                      "AND refobjid = '%u'::pg_catalog.oid "
                      "AND classid = 'pg_catalog.pg_amproc'::pg_catalog.regclass "
                      "AND objid = ap.oid "
                      "ORDER BY amprocnum",
                      opfinfo->dobj.catId.oid);

    res_procs = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

    // Get access method name
    resetPQExpBuffer(query);
    appendPQExpBuffer(query, "SELECT "
                      "(SELECT amname FROM pg_catalog.pg_am WHERE oid = opfmethod) AS amname "
                      "FROM pg_catalog.pg_opfamily "
                      "WHERE oid = '%u'::pg_catalog.oid",
                      opfinfo->dobj.catId.oid);

    res = ExecuteSqlQueryForSingleRow(fout, query->data);
    amname = pg_strdup(PQgetvalue(res, 0, PQfnumber(res, "amname")));

    // Build DROP statement
    appendPQExpBuffer(delq, "DROP OPERATOR FAMILY %s USING %s;\n",
                      fmtQualifiedDumpable(opfinfo), fmtId(amname));

    // Build CREATE statement
    appendPQExpBuffer(q, "CREATE OPERATOR FAMILY %s USING %s;\n",
                      fmtQualifiedDumpable(opfinfo), fmtId(amname));

    PQclear(res);

    // Add ALTER statement for loose members if any exist
    if (PQntuples(res_ops) > 0 || PQntuples(res_procs) > 0) {
        appendPQExpBuffer(q, "ALTER OPERATOR FAMILY %s USING %s ADD\n    ",
                         fmtQualifiedDumpable(opfinfo), fmtId(amname));

        // Add operator entries
        ntups = PQntuples(res_ops);
        for (i = 0; i < ntups; i++) {
            char *amopstrategy = PQgetvalue(res_ops, i, PQfnumber(res_ops, "amopstrategy"));
            char *amopopr = PQgetvalue(res_ops, i, PQfnumber(res_ops, "amopopr"));
            char *sortfamily = PQgetvalue(res_ops, i, PQfnumber(res_ops, "sortfamily"));
            char *sortfamilynsp = PQgetvalue(res_ops, i, PQfnumber(res_ops, "sortfamilynsp"));

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

        // Add function entries
        ntups = PQntuples(res_procs);
        for (i = 0; i < ntups; i++) {
            char *amprocnum = PQgetvalue(res_procs, i, PQfnumber(res_procs, "amprocnum"));
            char *amproc = PQgetvalue(res_procs, i, PQfnumber(res_procs, "amproc"));
            char *amproclefttype = PQgetvalue(res_procs, i, PQfnumber(res_procs, "amproclefttype"));
            char *amprocrighttype = PQgetvalue(res_procs, i, PQfnumber(res_procs, "amprocrighttype"));

            if (needComma)
                appendPQExpBufferStr(q, " ,\n    ");

            appendPQExpBuffer(q, "FUNCTION %s (%s, %s) %s",
                             amprocnum, amproclefttype, amprocrighttype, amproc);

            needComma = true;
        }

        appendPQExpBufferStr(q, ";\n");
    }

    // Build nameusing for comments and binary upgrade
    appendPQExpBuffer(nameusing, "%s USING %s",
                      fmtId(opfinfo->dobj.name), fmtId(amname));

    // Handle binary upgrade
    if (dopt->binary_upgrade)
        binary_upgrade_extension_member(q, &opfinfo->dobj,
                                       "OPERATOR FAMILY", nameusing->data,
                                       opfinfo->dobj.namespace->dobj.name);

    // Register with archive
    if (opfinfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
        ArchiveEntry(fout, opfinfo->dobj.catId, opfinfo->dobj.dumpId,
                    ARCHIVE_OPTS(.tag = opfinfo->dobj.name,
                                .namespace = opfinfo->dobj.namespace->dobj.name,
                                .owner = opfinfo->rolname,
                                .description = "OPERATOR FAMILY",
                                .section = SECTION_PRE_DATA,
                                .createStmt = q->data,
                                .dropStmt = delq->data));

    // Dump comments
    if (opfinfo->dobj.dump & DUMP_COMPONENT_COMMENT)
        dumpComment(fout, "OPERATOR FAMILY", nameusing->data,
                   opfinfo->dobj.namespace->dobj.name, opfinfo->rolname,
                   opfinfo->dobj.catId, 0, opfinfo->dobj.dumpId);

    // Cleanup
    free(amname);
    PQclear(res_ops);
    PQclear(res_procs);
    destroyPQExpBuffer(query);
    destroyPQExpBuffer(q);
    destroyPQExpBuffer(delq);
    destroyPQExpBuffer(nameusing);
}
```