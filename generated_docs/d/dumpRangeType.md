# dumpRangeType

## Location
[src/bin/pg_dump/pg_dump.c:11091-11248](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L11091-L11248)

## Overview
Generates SQL commands to recreate a user-defined range type during PostgreSQL database dump operations.

## Definition

```c
static void
dumpRangeType(Archive *fout, const TypeInfo *tyinfo)
```
## Detailed Description
The  function creates SQL statements to recreate user-defined range types in PostgreSQL dumps. Range types are composite types that represent a range of values of some element type (the subtype). The function handles complex range type properties including subtype, operator class, collation, canonical function, and subtype difference function.

The function performs the following operations:
1. Prepares and executes a query to retrieve range type metadata from , , and  system catalogs
2. Constructs a  statement with appropriate parameters
3. Handles version-specific features like multirange types (PostgreSQL 14+)
4. Manages optional parameters like custom operator classes, collations, canonical functions, and subtype difference functions
5. Supports binary upgrade mode with OID preservation

## Parameters / Member Variables
- `*fout`: Archive object containing dump configuration and state information
- `*tyinfo`: TypeInfo structure containing metadata about the range type to be dumped
## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlStatement](../E/ExecuteSqlStatement.md)
  - [ExecuteSqlQueryForSingleRow](../E/ExecuteSqlQueryForSingleRow.md)
  - [fmtId](../f/fmtId.md)
  - fmtQualifiedDumpable
  - [binary_upgrade_set_type_oids_by_type_oid](../b/binary_upgrade_set_type_oids_by_type_oid.md)
  - [findCollationByOid](../f/findCollationByOid.md)
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [dumpComment](dumpComment.md)
  - [dumpSecLabel](dumpSecLabel.md)
  - [dumpACL](dumpACL.md)
- Called from (representative examples):
  - [dumpType](dumpType.md)

## Notes and Other Information
- Uses prepared statements for efficiency when dumping multiple range types
- Supports PostgreSQL 14+ multirange types with version-specific conditional logic
- Only includes non-default operator classes in the CREATE TYPE statement for brevity
- Handles collation specifications only when different from the subtype's default collation
- Optional canonical and subtype_diff functions are included only when explicitly defined
- Binary upgrade mode preserves original type OIDs for consistent restoration
- Comprehensive dump component handling for definition, comments, security labels, and ACLs

## Simplified Source

```c
static void
dumpRangeType(Archive *fout, const TypeInfo *tyinfo)
{
    DumpOptions *dopt = fout->dopt;
    PQExpBuffer q = createPQExpBuffer();
    PQExpBuffer delq = createPQExpBuffer();
    PQExpBuffer query = createPQExpBuffer();
    PGresult *res;
    char *qtypname, *qualtypname;

    // Prepare query for range type details if not already done
    if (!fout->is_prepared[PREPQUERY_DUMPRANGETYPE])
    {
        appendPQExpBufferStr(query, "PREPARE dumpRangeType(pg_catalog.oid) AS\n");

        // Build query with version-specific multirange support
        if (fout->remoteVersion >= 140000)
            appendPQExpBufferStr(query, "pg_catalog.format_type(rngmultitypid, NULL) AS rngmultitype, ");
        else
            appendPQExpBufferStr(query, "NULL AS rngmultitype, ");

        appendPQExpBufferStr(query,
                           "pg_catalog.format_type(rngsubtype, NULL) AS rngsubtype, "
                           "opc.opcname AS opcname, "
                           "(SELECT nspname FROM pg_catalog.pg_namespace nsp "
                           "  WHERE nsp.oid = opc.opcnamespace) AS opcnsp, "
                           "opc.opcdefault, "
                           "CASE WHEN rngcollation = st.typcollation THEN 0 "
                           "     ELSE rngcollation END AS collation, "
                           "rngcanonical, rngsubdiff "
                           "FROM pg_catalog.pg_range r, pg_catalog.pg_type st, "
                           "     pg_catalog.pg_opclass opc "
                           "WHERE st.oid = rngsubtype AND opc.oid = rngsubopc AND "
                           "rngtypid = $1");

        ExecuteSqlStatement(fout, query->data);
        fout->is_prepared[PREPQUERY_DUMPRANGETYPE] = true;
    }

    // Execute query to get range type details
    printfPQExpBuffer(query, "EXECUTE dumpRangeType('%u')", tyinfo->dobj.catId.oid);
    res = ExecuteSqlQueryForSingleRow(fout, query->data);

    qtypname = pg_strdup(fmtId(tyinfo->dobj.name));
    qualtypname = pg_strdup(fmtQualifiedDumpable(tyinfo));

    // Create DROP statement
    appendPQExpBuffer(delq, "DROP TYPE %s;\n", qualtypname);

    // Handle binary upgrade OID preservation
    if (dopt->binary_upgrade)
        binary_upgrade_set_type_oids_by_type_oid(fout, q, tyinfo->dobj.catId.oid,
                                                false, true);

    // Start CREATE TYPE AS RANGE statement
    appendPQExpBuffer(q, "CREATE TYPE %s AS RANGE (", qualtypname);
    appendPQExpBuffer(q, "\n    subtype = %s", PQgetvalue(res, 0, PQfnumber(res, "rngsubtype")));

    // Add multirange type name if supported and available
    if (!PQgetisnull(res, 0, PQfnumber(res, "rngmultitype")))
        appendPQExpBuffer(q, ",\n    multirange_type_name = %s",
                         PQgetvalue(res, 0, PQfnumber(res, "rngmultitype")));

    // Add subtype_opclass only if not default
    if (PQgetvalue(res, 0, PQfnumber(res, "opcdefault"))[0] != 't')
    {
        char *opcname = PQgetvalue(res, 0, PQfnumber(res, "opcname"));
        char *nspname = PQgetvalue(res, 0, PQfnumber(res, "opcnsp"));
        appendPQExpBuffer(q, ",\n    subtype_opclass = %s.%s",
                         fmtId(nspname), fmtId(opcname));
    }

    // Add collation if different from subtype default
    Oid collationOid = atooid(PQgetvalue(res, 0, PQfnumber(res, "collation")));
    if (OidIsValid(collationOid))
    {
        CollInfo *coll = findCollationByOid(collationOid);
        if (coll)
            appendPQExpBuffer(q, ",\n    collation = %s", fmtQualifiedDumpable(coll));
    }

    // Add canonical function if specified
    char *procname = PQgetvalue(res, 0, PQfnumber(res, "rngcanonical"));
    if (strcmp(procname, "-") != 0)
        appendPQExpBuffer(q, ",\n    canonical = %s", procname);

    // Add subtype_diff function if specified
    procname = PQgetvalue(res, 0, PQfnumber(res, "rngsubdiff"));
    if (strcmp(procname, "-") != 0)
        appendPQExpBuffer(q, ",\n    subtype_diff = %s", procname);

    appendPQExpBufferStr(q, "\n);\n");

    // Handle extension membership for binary upgrade
    if (dopt->binary_upgrade)
        binary_upgrade_extension_member(q, &tyinfo->dobj, "TYPE", qtypname,
                                      tyinfo->dobj.namespace->dobj.name);

    // Archive the type definition
    if (tyinfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
        ArchiveEntry(fout, tyinfo->dobj.catId, tyinfo->dobj.dumpId,
                    ARCHIVE_OPTS(.tag = tyinfo->dobj.name,
                                .namespace = tyinfo->dobj.namespace->dobj.name,
                                .owner = tyinfo->rolname,
                                .description = "TYPE",
                                .section = SECTION_PRE_DATA,
                                .createStmt = q->data,
                                .dropStmt = delq->data));

    // Dump additional metadata
    if (tyinfo->dobj.dump & DUMP_COMPONENT_COMMENT)
        dumpComment(fout, "TYPE", qtypname, tyinfo->dobj.namespace->dobj.name,
                   tyinfo->rolname, tyinfo->dobj.catId, 0, tyinfo->dobj.dumpId);

    if (tyinfo->dobj.dump & DUMP_COMPONENT_SECLABEL)
        dumpSecLabel(fout, "TYPE", qtypname, tyinfo->dobj.namespace->dobj.name,
                    tyinfo->rolname, tyinfo->dobj.catId, 0, tyinfo->dobj.dumpId);

    if (tyinfo->dobj.dump & DUMP_COMPONENT_ACL)
        dumpACL(fout, tyinfo->dobj.dumpId, InvalidDumpId, "TYPE", qtypname, NULL,
               tyinfo->dobj.namespace->dobj.name, NULL, tyinfo->rolname, &tyinfo->dacl);

    // Cleanup
    PQclear(res);
    destroyPQExpBuffer(q);
    destroyPQExpBuffer(delq);
    destroyPQExpBuffer(query);
    free(qtypname);
    free(qualtypname);
}
```