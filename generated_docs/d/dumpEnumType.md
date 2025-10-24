# dumpEnumType

## Location
[src/bin/pg_dump/pg_dump.c:10951-11090](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L10951-L11090)

## Overview
Generates SQL commands to recreate a user-defined enum type during PostgreSQL database dump operations.

## Definition

```c
enum_oid;
```
## Detailed Description
The  function is responsible for creating SQL statements that recreate user-defined enumerated types in PostgreSQL dumps. It handles both regular dumps and binary upgrade scenarios, ensuring that enum values are recreated with the correct order and, in binary upgrade mode, with preserved OIDs.

The function performs the following key operations:
1. Prepares and executes a query to retrieve enum labels from  ordered by 
2. Constructs a  statement with the enum values
3. For binary upgrades, preserves original OIDs using 
4. Handles associated metadata including comments, security labels, and ACLs

## Parameters / Member Variables
- : Archive object containing dump configuration and state information
- : TypeInfo structure containing metadata about the enum type to be dumped

## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlStatement](../E/ExecuteSqlStatement.md)
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - [fmtId](../f/fmtId.md)
  - fmtQualifiedDumpable
  - [binary_upgrade_set_type_oids_by_type_oid](../b/binary_upgrade_set_type_oids_by_type_oid.md)
  - appendStringLiteralAH
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [dumpComment](dumpComment.md)
  - [dumpSecLabel](dumpSecLabel.md)
  - [dumpACL](dumpACL.md)
- Called from (representative examples):
  - [dumpType](dumpType.md)

## Notes and Other Information
- Uses prepared statements for efficiency when dumping multiple enum types
- Binary upgrade mode requires special handling to preserve enum value OIDs
- The function ensures proper SQL escaping of enum labels using 
- Includes comprehensive dump component handling (definition, comments, security labels, ACLs)
- Enum values are retrieved in  to maintain proper ordering in the recreated type

## Simplified Source

```c
static void
dumpEnumType(Archive *fout, const TypeInfo *tyinfo)
{
    DumpOptions *dopt = fout->dopt;
    PQExpBuffer q = createPQExpBuffer();
    PQExpBuffer delq = createPQExpBuffer();
    PQExpBuffer query = createPQExpBuffer();
    PGresult *res;
    char *qtypname, *qualtypname;

    // Prepare query for enum values if not already done
    if (!fout->is_prepared[PREPQUERY_DUMPENUMTYPE])
    {
        appendPQExpBufferStr(query,
                           "PREPARE dumpEnumType(pg_catalog.oid) AS\n"
                           "SELECT oid, enumlabel "
                           "FROM pg_catalog.pg_enum "
                           "WHERE enumtypid = $1 "
                           "ORDER BY enumsortorder");
        ExecuteSqlStatement(fout, query->data);
        fout->is_prepared[PREPQUERY_DUMPENUMTYPE] = true;
    }

    // Execute query to get enum values
    printfPQExpBuffer(query, "EXECUTE dumpEnumType('%u')", tyinfo->dobj.catId.oid);
    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

    int num = PQntuples(res);
    qtypname = pg_strdup(fmtId(tyinfo->dobj.name));
    qualtypname = pg_strdup(fmtQualifiedDumpable(tyinfo));

    // Create DROP statement
    appendPQExpBuffer(delq, "DROP TYPE %s;\n", qualtypname);

    // Handle binary upgrade OID preservation
    if (dopt->binary_upgrade)
        binary_upgrade_set_type_oids_by_type_oid(fout, q, tyinfo->dobj.catId.oid,
                                                false, false);

    // Start CREATE TYPE statement
    appendPQExpBuffer(q, "CREATE TYPE %s AS ENUM (", qualtypname);

    if (!dopt->binary_upgrade)
    {
        // Regular dump: Add all enum values at once
        int i_enumlabel = PQfnumber(res, "enumlabel");
        for (int i = 0; i < num; i++)
        {
            char *label = PQgetvalue(res, i, i_enumlabel);
            if (i > 0)
                appendPQExpBufferChar(q, ',');
            appendPQExpBufferStr(q, "\n    ");
            appendStringLiteralAH(q, label, fout);
        }
    }

    appendPQExpBufferStr(q, "\n);\n");

    if (dopt->binary_upgrade)
    {
        // Binary upgrade: Add values one by one with preserved OIDs
        int i_oid = PQfnumber(res, "oid");
        int i_enumlabel = PQfnumber(res, "enumlabel");

        for (int i = 0; i < num; i++)
        {
            Oid enum_oid = atooid(PQgetvalue(res, i, i_oid));
            char *label = PQgetvalue(res, i, i_enumlabel);

            if (i == 0)
                appendPQExpBufferStr(q, "\n-- For binary upgrade, must preserve pg_enum oids\n");

            appendPQExpBuffer(q, "SELECT pg_catalog.binary_upgrade_set_next_pg_enum_oid('%u'::pg_catalog.oid);\n", enum_oid);
            appendPQExpBuffer(q, "ALTER TYPE %s ADD VALUE ", qualtypname);
            appendStringLiteralAH(q, label, fout);
            appendPQExpBufferStr(q, ";\n\n");
        }
    }

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