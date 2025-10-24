# dumpCompositeType

## Location
[src/bin/pg_dump/pg_dump.c:11787-11992](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L11787-L11992)

## Overview
The dumpCompositeType function generates SQL statements to recreate a user-defined composite type during PostgreSQL database dumps.

## Definition

```c
static void
dumpCompositeType(Archive *fout, const TypeInfo *tyinfo)
```
## Detailed Description
This function processes a composite type (user-defined type with multiple attributes) and generates the appropriate CREATE TYPE statement along with any necessary metadata. It handles both regular dumps and binary upgrades, with special consideration for dropped columns in binary upgrade mode.

The function performs a query to retrieve all attributes of the composite type, including their names, types, alignment, length, and collation information. It constructs a CREATE TYPE statement with all non-dropped attributes, and for binary upgrades, it includes special handling for dropped columns by creating placeholders and generating subsequent ALTER statements.

The function also handles dumping of associated comments, security labels, and access control lists for the type and its columns.

## Parameters / Member Variables
- `*fout`: Archive handle for the dump output stream
- `*tyinfo`: TypeInfo structure containing metadata about the composite type to dump
## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlStatement](../E/ExecuteSqlStatement.md)
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - [binary_upgrade_set_type_oids_by_type_oid](../b/binary_upgrade_set_type_oids_by_type_oid.md)
  - [binary_upgrade_set_pg_class_oids](../b/binary_upgrade_set_pg_class_oids.md)
  - [fmtId](../f/fmtId.md)
  - fmtQualifiedDumpable
  - [findCollationByOid](../f/findCollationByOid.md)
  - appendStringLiteralAH
  - [binary_upgrade_extension_member](../b/binary_upgrade_extension_member.md)
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [dumpComment](dumpComment.md)
  - [dumpSecLabel](dumpSecLabel.md)
  - [dumpACL](dumpACL.md)
  - [dumpCompositeTypeColComments](dumpCompositeTypeColComments.md)
- Called from (representative examples):
  - [dumpType](dumpType.md)

## Notes and Other Information
- Uses prepared statements for efficiency when dumping multiple composite types
- Special handling for binary upgrade mode includes preserving dropped columns as placeholders
- Collation clauses are only included when they differ from the type's default collation
- The function dumps the type definition in the PRE_DATA section to ensure proper dependency ordering
- Column comments are handled separately via dumpCompositeTypeColComments

## Simplified Source

```c
static void
dumpCompositeType(Archive *fout, const TypeInfo *tyinfo)
{
    DumpOptions *dopt = fout->dopt;
    PQExpBuffer q = createPQExpBuffer();
    PQExpBuffer dropped = createPQExpBuffer();
    PQExpBuffer delq = createPQExpBuffer();
    PQExpBuffer query = createPQExpBuffer();
    PGresult   *res;
    char       *qtypname;
    char       *qualtypname;
    int         ntups;
    int         actual_atts;

    // Prepare and execute query for composite type attributes
    if (!fout->is_prepared[PREPQUERY_DUMPCOMPOSITETYPE]) {
        // Set up prepared statement for type-specific details
        appendPQExpBufferStr(query,
                             "PREPARE dumpCompositeType(pg_catalog.oid) AS\n"
                             "SELECT a.attname, a.attnum, "
                             "pg_catalog.format_type(a.atttypid, a.atttypmod) AS atttypdefn, "
                             "a.attlen, a.attalign, a.attisdropped, "
                             "CASE WHEN a.attcollation <> at.typcollation "
                             "THEN a.attcollation ELSE 0 END AS attcollation "
                             "FROM pg_catalog.pg_type ct "
                             "JOIN pg_catalog.pg_attribute a ON a.attrelid = ct.typrelid "
                             "LEFT JOIN pg_catalog.pg_type at ON at.oid = a.atttypid "
                             "WHERE ct.oid = $1 "
                             "ORDER BY a.attnum");
        ExecuteSqlStatement(fout, query->data);
        fout->is_prepared[PREPQUERY_DUMPCOMPOSITETYPE] = true;
    }

    printfPQExpBuffer(query, "EXECUTE dumpCompositeType('%u')", tyinfo->dobj.catId.oid);
    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    ntups = PQntuples(res);

    // Get column indices for result processing
    int i_attname = PQfnumber(res, "attname");
    int i_atttypdefn = PQfnumber(res, "atttypdefn");
    int i_attlen = PQfnumber(res, "attlen");
    int i_attalign = PQfnumber(res, "attalign");
    int i_attisdropped = PQfnumber(res, "attisdropped");
    int i_attcollation = PQfnumber(res, "attcollation");

    // Handle binary upgrade mode
    if (dopt->binary_upgrade) {
        binary_upgrade_set_type_oids_by_type_oid(fout, q, tyinfo->dobj.catId.oid, false, false);
        binary_upgrade_set_pg_class_oids(fout, q, tyinfo->typrelid, false);
    }

    qtypname = pg_strdup(fmtId(tyinfo->dobj.name));
    qualtypname = pg_strdup(fmtQualifiedDumpable(tyinfo));

    // Build CREATE TYPE statement
    appendPQExpBuffer(q, "CREATE TYPE %s AS (", qualtypname);

    actual_atts = 0;
    for (int i = 0; i < ntups; i++) {
        char       *attname = PQgetvalue(res, i, i_attname);
        char       *atttypdefn = PQgetvalue(res, i, i_atttypdefn);
        char       *attlen = PQgetvalue(res, i, i_attlen);
        char       *attalign = PQgetvalue(res, i, i_attalign);
        bool        attisdropped = (PQgetvalue(res, i, i_attisdropped)[0] == 't');
        Oid         attcollation = atooid(PQgetvalue(res, i, i_attcollation));

        if (attisdropped && !dopt->binary_upgrade)
            continue;

        // Format attribute definition
        if (actual_atts++ > 0)
            appendPQExpBufferChar(q, ',');
        appendPQExpBufferStr(q, "\n\t");

        if (!attisdropped) {
            appendPQExpBuffer(q, "%s %s", fmtId(attname), atttypdefn);

            // Add collation if different from type's default
            if (OidIsValid(attcollation)) {
                CollInfo   *coll = findCollationByOid(attcollation);
                if (coll)
                    appendPQExpBuffer(q, " COLLATE %s", fmtQualifiedDumpable(coll));
            }
        } else {
            // Handle dropped attributes for binary upgrade
            appendPQExpBuffer(q, "%s INTEGER /* dummy */", fmtId(attname));
            // ... generate UPDATE and ALTER statements for dropped columns
        }
    }
    appendPQExpBufferStr(q, "\n);\n");
    appendPQExpBufferStr(q, dropped->data);

    appendPQExpBuffer(delq, "DROP TYPE %s;\n", qualtypname);

    // Handle binary upgrade extension member
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

    // Dump associated metadata
    if (tyinfo->dobj.dump & DUMP_COMPONENT_COMMENT)
        dumpComment(fout, "TYPE", qtypname, tyinfo->dobj.namespace->dobj.name,
                    tyinfo->rolname, tyinfo->dobj.catId, 0, tyinfo->dobj.dumpId);

    if (tyinfo->dobj.dump & DUMP_COMPONENT_SECLABEL)
        dumpSecLabel(fout, "TYPE", qtypname, tyinfo->dobj.namespace->dobj.name,
                     tyinfo->rolname, tyinfo->dobj.catId, 0, tyinfo->dobj.dumpId);

    if (tyinfo->dobj.dump & DUMP_COMPONENT_ACL)
        dumpACL(fout, tyinfo->dobj.dumpId, InvalidDumpId, "TYPE",
                qtypname, NULL, tyinfo->dobj.namespace->dobj.name,
                NULL, tyinfo->rolname, &tyinfo->dacl);

    // Dump column comments
    if (tyinfo->dobj.dump & DUMP_COMPONENT_COMMENT)
        dumpCompositeTypeColComments(fout, tyinfo, res);

    // Cleanup
    PQclear(res);
    destroyPQExpBuffer(q);
    destroyPQExpBuffer(dropped);
    destroyPQExpBuffer(delq);
    destroyPQExpBuffer(query);
    free(qtypname);
    free(qualtypname);
}
```