# dumpDomain

## Location
[src/bin/pg_dump/pg_dump.c:11562-11786](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L11562-L11786)

## Overview
Generates SQL commands to recreate a user-defined domain type with constraints, defaults, and collations during PostgreSQL database dump operations.

## Definition

```c
static void
dumpDomain(Archive *fout, const TypeInfo *tyinfo)
```
## Detailed Description
The  function creates SQL statements to recreate domain types in PostgreSQL dumps. Domains are essentially constrained versions of existing data types, allowing users to define reusable type definitions with specific constraints, default values, and collations. The function handles the complete domain specification including base type, collation (when different from base type), NOT NULL constraints (with version-specific naming), default values, and CHECK constraints.

The function performs the following operations:
1. Queries  system catalog for domain metadata including base type, constraints, defaults, and collation information
2. Constructs a  statement with all applicable modifiers
3. Handles version-specific features like named NOT NULL constraints (PostgreSQL 17+)
4. Includes custom collations only when they differ from the base type's collation
5. Processes inline CHECK constraints and NOT NULL constraints
6. Manages both literal and expression-based default values
7. Dumps comments for individual constraints

## Parameters / Member Variables
- `*fout`: Archive object containing dump configuration and state information
- `*tyinfo`: TypeInfo structure containing metadata about the domain type to be dumped, including constraint information
## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlStatement](../E/ExecuteSqlStatement.md)
  - [ExecuteSqlQueryForSingleRow](../E/ExecuteSqlQueryForSingleRow.md)
  - [fmtId](../f/fmtId.md)
  - fmtQualifiedDumpable
  - [binary_upgrade_set_type_oids_by_type_oid](../b/binary_upgrade_set_type_oids_by_type_oid.md)
  - [findCollationByOid](../f/findCollationByOid.md)
  - appendStringLiteralAH
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [dumpComment](dumpComment.md)
  - [dumpSecLabel](dumpSecLabel.md)
  - [dumpACL](dumpACL.md)
- Called from (representative examples):
  - [dumpType](dumpType.md)

## Notes and Other Information
- Supports sophisticated constraint handling including inline CHECK constraints and NOT NULL constraints
- Version-aware NOT NULL constraint naming (PostgreSQL 17+ supports named NOT NULL constraints)
- Only includes collation specification when it differs from the base type to avoid redundancy
- Handles both compiled expressions (typdefaultbin) and literal defaults (typdefault) appropriately
- Binary upgrade mode forces array type creation for domains
- Comprehensive constraint comment handling for both CHECK and NOT NULL constraints
- Uses 'DOMAIN' description in archive entries to distinguish from other type categories
- The function demonstrates PostgreSQL's evolution with conditional logic for newer constraint features

## Simplified Source

```c
static void
dumpDomain(Archive *fout, const TypeInfo *tyinfo)
{
    DumpOptions *dopt = fout->dopt;
    PQExpBuffer q = createPQExpBuffer();
    PQExpBuffer delq = createPQExpBuffer();
    PQExpBuffer query = createPQExpBuffer();
    PGresult *res;
    char *qtypname, *qualtypname;

    // Prepare query for domain details if not already done
    if (!fout->is_prepared[PREPQUERY_DUMPDOMAIN])
    {
        appendPQExpBufferStr(query, "PREPARE dumpDomain(pg_catalog.oid) AS\n");
        appendPQExpBufferStr(query, "SELECT t.typnotnull, "
                           "pg_catalog.format_type(t.typbasetype, t.typtypmod) AS typdefn, "
                           "pg_catalog.pg_get_expr(t.typdefaultbin, 'pg_catalog.pg_type'::pg_catalog.regclass) AS typdefaultbin, "
                           "t.typdefault, "
                           "CASE WHEN t.typcollation <> u.typcollation "
                           "THEN t.typcollation ELSE 0 END AS typcollation "
                           "FROM pg_catalog.pg_type t "
                           "LEFT JOIN pg_catalog.pg_type u ON (t.typbasetype = u.oid) "
                           "WHERE t.oid = $1");
        ExecuteSqlStatement(fout, query->data);
        fout->is_prepared[PREPQUERY_DUMPDOMAIN] = true;
    }

    // Execute query to get domain details
    printfPQExpBuffer(query, "EXECUTE dumpDomain('%u')", tyinfo->dobj.catId.oid);
    res = ExecuteSqlQueryForSingleRow(fout, query->data);

    // Extract domain properties
    char *typnotnull = PQgetvalue(res, 0, PQfnumber(res, "typnotnull"));
    char *typdefn = PQgetvalue(res, 0, PQfnumber(res, "typdefn"));

    // Handle default value (binary expression or literal)
    char *typdefault = NULL;
    bool typdefault_is_literal = false;
    if (!PQgetisnull(res, 0, PQfnumber(res, "typdefaultbin")))
        typdefault = PQgetvalue(res, 0, PQfnumber(res, "typdefaultbin"));
    else if (!PQgetisnull(res, 0, PQfnumber(res, "typdefault")))
    {
        typdefault = PQgetvalue(res, 0, PQfnumber(res, "typdefault"));
        typdefault_is_literal = true;
    }

    Oid typcollation = atooid(PQgetvalue(res, 0, PQfnumber(res, "typcollation")));

    // Handle binary upgrade OID preservation
    if (dopt->binary_upgrade)
        binary_upgrade_set_type_oids_by_type_oid(fout, q, tyinfo->dobj.catId.oid,
                                                true, false);

    qtypname = pg_strdup(fmtId(tyinfo->dobj.name));
    qualtypname = pg_strdup(fmtQualifiedDumpable(tyinfo));

    // Build CREATE DOMAIN statement
    appendPQExpBuffer(q, "CREATE DOMAIN %s AS %s", qualtypname, typdefn);

    // Add collation if different from base type
    if (OidIsValid(typcollation))
    {
        CollInfo *coll = findCollationByOid(typcollation);
        if (coll)
            appendPQExpBuffer(q, " COLLATE %s", fmtQualifiedDumpable(coll));
    }

    // Add NOT NULL constraint (version-specific handling)
    if (typnotnull[0] == 't')
    {
        if (fout->remoteVersion < 170000 || tyinfo->notnull == NULL)
            appendPQExpBufferStr(q, " NOT NULL");
        else
        {
            ConstraintInfo *notnull = tyinfo->notnull;
            if (!notnull->separate)
            {
                // Check if using default constraint name
                char *default_name = psprintf("%s_not_null", tyinfo->dobj.name);
                if (strcmp(default_name, notnull->dobj.name) == 0)
                    appendPQExpBufferStr(q, " NOT NULL");
                else
                    appendPQExpBuffer(q, " CONSTRAINT %s %s",
                                     fmtId(notnull->dobj.name), notnull->condef);
                free(default_name);
            }
        }
    }

    // Add default value
    if (typdefault != NULL)
    {
        appendPQExpBufferStr(q, " DEFAULT ");
        if (typdefault_is_literal)
            appendStringLiteralAH(q, typdefault, fout);
        else
            appendPQExpBufferStr(q, typdefault);
    }

    PQclear(res);

    // Add CHECK constraints
    for (int i = 0; i < tyinfo->nDomChecks; i++)
    {
        ConstraintInfo *domcheck = &(tyinfo->domChecks[i]);
        if (!domcheck->separate && domcheck->contype == 'c')
            appendPQExpBuffer(q, "\n\tCONSTRAINT %s %s",
                             fmtId(domcheck->dobj.name), domcheck->condef);
    }

    appendPQExpBufferStr(q, ";\n");

    // Create DROP statement
    appendPQExpBuffer(delq, "DROP DOMAIN %s;\n", qualtypname);

    // Handle extension membership for binary upgrade
    if (dopt->binary_upgrade)
        binary_upgrade_extension_member(q, &tyinfo->dobj, "DOMAIN", qtypname,
                                      tyinfo->dobj.namespace->dobj.name);

    // Archive the domain definition
    if (tyinfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
        ArchiveEntry(fout, tyinfo->dobj.catId, tyinfo->dobj.dumpId,
                    ARCHIVE_OPTS(.tag = tyinfo->dobj.name,
                                .namespace = tyinfo->dobj.namespace->dobj.name,
                                .owner = tyinfo->rolname,
                                .description = "DOMAIN",
                                .section = SECTION_PRE_DATA,
                                .createStmt = q->data,
                                .dropStmt = delq->data));

    // Dump additional metadata
    if (tyinfo->dobj.dump & DUMP_COMPONENT_COMMENT)
        dumpComment(fout, "DOMAIN", qtypname, tyinfo->dobj.namespace->dobj.name,
                   tyinfo->rolname, tyinfo->dobj.catId, 0, tyinfo->dobj.dumpId);

    if (tyinfo->dobj.dump & DUMP_COMPONENT_SECLABEL)
        dumpSecLabel(fout, "DOMAIN", qtypname, tyinfo->dobj.namespace->dobj.name,
                    tyinfo->rolname, tyinfo->dobj.catId, 0, tyinfo->dobj.dumpId);

    if (tyinfo->dobj.dump & DUMP_COMPONENT_ACL)
        dumpACL(fout, tyinfo->dobj.dumpId, InvalidDumpId, "TYPE", qtypname, NULL,
               tyinfo->dobj.namespace->dobj.name, NULL, tyinfo->rolname, &tyinfo->dacl);

    // Dump constraint comments
    for (int i = 0; i < tyinfo->nDomChecks; i++)
    {
        ConstraintInfo *domcheck = &(tyinfo->domChecks[i]);
        if (!domcheck->separate && domcheck->dobj.dump & DUMP_COMPONENT_COMMENT)
        {
            PQExpBuffer conprefix = createPQExpBuffer();
            appendPQExpBuffer(conprefix, "CONSTRAINT %s ON DOMAIN", fmtId(domcheck->dobj.name));
            dumpComment(fout, conprefix->data, qtypname, tyinfo->dobj.namespace->dobj.name,
                       tyinfo->rolname, domcheck->dobj.catId, 0, tyinfo->dobj.dumpId);
            destroyPQExpBuffer(conprefix);
        }
    }

    // Cleanup
    destroyPQExpBuffer(q);
    destroyPQExpBuffer(delq);
    destroyPQExpBuffer(query);
    free(qtypname);
    free(qualtypname);
}
```