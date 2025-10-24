# dumpBaseType

## Location
[src/bin/pg_dump/pg_dump.c:11313-11561](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L11313-L11561)

## Overview
Generates SQL commands to recreate a user-defined base type with all its implementation details during PostgreSQL database dump operations.

## Definition

```c
static void
dumpBaseType(Archive *fout, const TypeInfo *tyinfo)
```
## Detailed Description
The  function creates comprehensive SQL statements to recreate user-defined base types in PostgreSQL dumps. Base types are the most complex type category, requiring complete specification of input/output functions, internal representation, storage characteristics, and operational behaviors. The function handles all aspects of base type definition including I/O functions, optional functions (receive/send, typmod, analyze, subscript), storage parameters, alignment, and behavioral attributes.

The function performs the following operations:
1. Queries  system catalog for comprehensive type metadata including all functions, storage parameters, and attributes
2. Constructs a detailed  statement with all required and optional parameters
3. Handles version-specific features like subscript functions (PostgreSQL 14+)
4. Manages type defaults, both literal and expression-based
5. Includes storage optimization parameters (alignment, storage mode, pass-by-value)
6. Supports element types for array base types
7. Handles type categories and preferences for operator resolution

## Parameters / Member Variables
- `*fout`: Archive object containing dump configuration and state information
- `*tyinfo`: TypeInfo structure containing metadata about the base type to be dumped
## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlStatement](../E/ExecuteSqlStatement.md)
  - [ExecuteSqlQueryForSingleRow](../E/ExecuteSqlQueryForSingleRow.md)
  - [fmtId](../f/fmtId.md)
  - fmtQualifiedDumpable
  - [binary_upgrade_set_type_oids_by_type_oid](../b/binary_upgrade_set_type_oids_by_type_oid.md)
  - appendStringLiteralAH
  - [getFormattedTypeName](../g/getFormattedTypeName.md)
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [dumpComment](dumpComment.md)
  - [dumpSecLabel](dumpSecLabel.md)
  - [dumpACL](dumpACL.md)
- Called from (representative examples):
  - [dumpType](dumpType.md)

## Notes and Other Information
- Most complex type dump function due to the comprehensive nature of base type definitions
- Uses  because of circular dependencies between types and their I/O functions
- Handles variable-length types by converting typlen=-1 to 'variable'
- Only includes optional functions (receive/send, typmod, analyze, subscript) when they have valid OIDs
- Supports sophisticated default value handling with both literal strings and parsed expressions
- Includes comprehensive storage parameter specification (alignment, storage mode, pass-by-value)
- Type category 'U' (user-defined) is default and omitted for brevity
- Version-aware handling for newer features like subscript functions
- Full binary upgrade support with OID preservation for consistent restoration

## Simplified Source

```c
static void
dumpBaseType(Archive *fout, const TypeInfo *tyinfo)
{
    DumpOptions *dopt = fout->dopt;
    PQExpBuffer q = createPQExpBuffer();
    PQExpBuffer delq = createPQExpBuffer();
    PQExpBuffer query = createPQExpBuffer();
    PGresult *res;
    char *qtypname, *qualtypname;

    // Prepare query for base type details if not already done
    if (!fout->is_prepared[PREPQUERY_DUMPBASETYPE])
    {
        appendPQExpBufferStr(query,
                           "PREPARE dumpBaseType(pg_catalog.oid) AS\n"
                           "SELECT typlen, typinput, typoutput, typreceive, typsend, "
                           "typreceive::pg_catalog.oid AS typreceiveoid, "
                           "typsend::pg_catalog.oid AS typsendoid, "
                           "typanalyze, typanalyze::pg_catalog.oid AS typanalyzeoid, "
                           "typdelim, typbyval, typalign, typstorage, "
                           "typmodin, typmodout, "
                           "typmodin::pg_catalog.oid AS typmodinoid, "
                           "typmodout::pg_catalog.oid AS typmodoutoid, "
                           "typcategory, typispreferred, "
                           "(typcollation <> 0) AS typcollatable, "
                           "pg_catalog.pg_get_expr(typdefaultbin, 0) AS typdefaultbin, typdefault, ");

        // Version-specific subscript function support
        if (fout->remoteVersion >= 140000)
            appendPQExpBufferStr(query, "typsubscript, typsubscript::pg_catalog.oid AS typsubscriptoid ");
        else
            appendPQExpBufferStr(query, "'-' AS typsubscript, 0 AS typsubscriptoid ");

        appendPQExpBufferStr(query, "FROM pg_catalog.pg_type WHERE oid = $1");
        ExecuteSqlStatement(fout, query->data);
        fout->is_prepared[PREPQUERY_DUMPBASETYPE] = true;
    }

    // Execute query and extract values
    printfPQExpBuffer(query, "EXECUTE dumpBaseType('%u')", tyinfo->dobj.catId.oid);
    res = ExecuteSqlQueryForSingleRow(fout, query->data);

    // Extract key type attributes
    char *typlen = PQgetvalue(res, 0, PQfnumber(res, "typlen"));
    char *typinput = PQgetvalue(res, 0, PQfnumber(res, "typinput"));
    char *typoutput = PQgetvalue(res, 0, PQfnumber(res, "typoutput"));

    // Extract optional function OIDs
    Oid typreceiveoid = atooid(PQgetvalue(res, 0, PQfnumber(res, "typreceiveoid")));
    Oid typsendoid = atooid(PQgetvalue(res, 0, PQfnumber(res, "typsendoid")));
    Oid typmodinoid = atooid(PQgetvalue(res, 0, PQfnumber(res, "typmodinoid")));
    Oid typmodoutoid = atooid(PQgetvalue(res, 0, PQfnumber(res, "typmodoutoid")));
    Oid typanalyzeoid = atooid(PQgetvalue(res, 0, PQfnumber(res, "typanalyzeoid")));
    Oid typsubscriptoid = atooid(PQgetvalue(res, 0, PQfnumber(res, "typsubscriptoid")));

    // Format type names
    qtypname = pg_strdup(fmtId(tyinfo->dobj.name));
    qualtypname = pg_strdup(fmtQualifiedDumpable(tyinfo));

    // Create DROP statement with CASCADE (due to I/O function dependencies)
    appendPQExpBuffer(delq, "DROP TYPE %s CASCADE;\n", qualtypname);

    // Handle binary upgrade OID preservation
    if (dopt->binary_upgrade)
        binary_upgrade_set_type_oids_by_type_oid(fout, q, tyinfo->dobj.catId.oid,
                                                false, false);

    // Build CREATE TYPE statement
    appendPQExpBuffer(q, "CREATE TYPE %s (\n    INTERNALLENGTH = %s",
                     qualtypname, (strcmp(typlen, "-1") == 0) ? "variable" : typlen);

    // Required I/O functions
    appendPQExpBuffer(q, ",\n    INPUT = %s", typinput);
    appendPQExpBuffer(q, ",\n    OUTPUT = %s", typoutput);

    // Optional functions (only include if defined)
    if (OidIsValid(typreceiveoid))
        appendPQExpBuffer(q, ",\n    RECEIVE = %s", PQgetvalue(res, 0, PQfnumber(res, "typreceive")));
    if (OidIsValid(typsendoid))
        appendPQExpBuffer(q, ",\n    SEND = %s", PQgetvalue(res, 0, PQfnumber(res, "typsend")));
    if (OidIsValid(typmodinoid))
        appendPQExpBuffer(q, ",\n    TYPMOD_IN = %s", PQgetvalue(res, 0, PQfnumber(res, "typmodin")));
    if (OidIsValid(typmodoutoid))
        appendPQExpBuffer(q, ",\n    TYPMOD_OUT = %s", PQgetvalue(res, 0, PQfnumber(res, "typmodout")));
    if (OidIsValid(typanalyzeoid))
        appendPQExpBuffer(q, ",\n    ANALYZE = %s", PQgetvalue(res, 0, PQfnumber(res, "typanalyze")));

    // Collatable flag
    if (strcmp(PQgetvalue(res, 0, PQfnumber(res, "typcollatable")), "t") == 0)
        appendPQExpBufferStr(q, ",\n    COLLATABLE = true");

    // Default value handling
    if (!PQgetisnull(res, 0, PQfnumber(res, "typdefaultbin")))
        appendPQExpBuffer(q, ",\n    DEFAULT = %s", PQgetvalue(res, 0, PQfnumber(res, "typdefaultbin")));
    else if (!PQgetisnull(res, 0, PQfnumber(res, "typdefault")))
    {
        appendPQExpBufferStr(q, ",\n    DEFAULT = ");
        appendStringLiteralAH(q, PQgetvalue(res, 0, PQfnumber(res, "typdefault")), fout);
    }

    // Subscript function (PostgreSQL 14+)
    if (OidIsValid(typsubscriptoid))
        appendPQExpBuffer(q, ",\n    SUBSCRIPT = %s", PQgetvalue(res, 0, PQfnumber(res, "typsubscript")));

    // Element type for arrays
    if (OidIsValid(tyinfo->typelem))
        appendPQExpBuffer(q, ",\n    ELEMENT = %s",
                         getFormattedTypeName(fout, tyinfo->typelem, zeroIsError));

    // Additional type parameters (category, preferred, delimiter, alignment, storage, passedbyvalue)
    char *typcategory = PQgetvalue(res, 0, PQfnumber(res, "typcategory"));
    if (strcmp(typcategory, "U") != 0)
    {
        appendPQExpBufferStr(q, ",\n    CATEGORY = ");
        appendStringLiteralAH(q, typcategory, fout);
    }

    if (strcmp(PQgetvalue(res, 0, PQfnumber(res, "typispreferred")), "t") == 0)
        appendPQExpBufferStr(q, ",\n    PREFERRED = true");

    // Storage and alignment parameters
    char *typalign = PQgetvalue(res, 0, PQfnumber(res, "typalign"));
    char *typstorage = PQgetvalue(res, 0, PQfnumber(res, "typstorage"));

    // Add alignment specification
    if (*typalign == TYPALIGN_CHAR)
        appendPQExpBufferStr(q, ",\n    ALIGNMENT = char");
    else if (*typalign == TYPALIGN_SHORT)
        appendPQExpBufferStr(q, ",\n    ALIGNMENT = int2");
    else if (*typalign == TYPALIGN_INT)
        appendPQExpBufferStr(q, ",\n    ALIGNMENT = int4");
    else if (*typalign == TYPALIGN_DOUBLE)
        appendPQExpBufferStr(q, ",\n    ALIGNMENT = double");

    // Add storage specification
    if (*typstorage == TYPSTORAGE_PLAIN)
        appendPQExpBufferStr(q, ",\n    STORAGE = plain");
    else if (*typstorage == TYPSTORAGE_EXTERNAL)
        appendPQExpBufferStr(q, ",\n    STORAGE = external");
    else if (*typstorage == TYPSTORAGE_EXTENDED)
        appendPQExpBufferStr(q, ",\n    STORAGE = extended");
    else if (*typstorage == TYPSTORAGE_MAIN)
        appendPQExpBufferStr(q, ",\n    STORAGE = main");

    if (strcmp(PQgetvalue(res, 0, PQfnumber(res, "typbyval")), "t") == 0)
        appendPQExpBufferStr(q, ",\n    PASSEDBYVALUE");

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