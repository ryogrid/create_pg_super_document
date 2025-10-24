# dumpCollation

## Location
[src/bin/pg_dump/pg_dump.c:13842-14098](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L13842-L14098)

## Overview
Writes out a single collation definition, generating CREATE COLLATION SQL statements with proper provider-specific locale and rules configuration.

## Definition

```c
static void
dumpCollation(Archive *fout, const CollInfo *collinfo)
```
## Detailed Description
The  function generates SQL commands to recreate a collation during database dumps. It handles multiple collation providers (libc, ICU, builtin, default) and adapts the output based on PostgreSQL version differences. The function queries the pg_collation catalog to retrieve collation properties including provider, determinism, locale settings, and ICU rules. It constructs CREATE COLLATION statements with appropriate provider-specific parameters:

- **libc provider**: Uses  and  or unified LANG=C.UTF-8
LANGUAGE=
LC_CTYPE="C.UTF-8"
LC_NUMERIC="C.UTF-8"
LC_TIME="C.UTF-8"
LC_COLLATE="C.UTF-8"
LC_MONETARY="C.UTF-8"
LC_MESSAGES="C.UTF-8"
LC_PAPER="C.UTF-8"
LC_NAME="C.UTF-8"
LC_ADDRESS="C.UTF-8"
LC_TELEPHONE="C.UTF-8"
LC_MEASUREMENT="C.UTF-8"
LC_IDENTIFICATION="C.UTF-8"
LC_ALL=
- **ICU provider**: Uses LANG=C.UTF-8
LANGUAGE=
LC_CTYPE="C.UTF-8"
LC_NUMERIC="C.UTF-8"
LC_TIME="C.UTF-8"
LC_COLLATE="C.UTF-8"
LC_MONETARY="C.UTF-8"
LC_MESSAGES="C.UTF-8"
LC_PAPER="C.UTF-8"
LC_NAME="C.UTF-8"
LC_ADDRESS="C.UTF-8"
LC_TELEPHONE="C.UTF-8"
LC_MEASUREMENT="C.UTF-8"
LC_IDENTIFICATION="C.UTF-8"
LC_ALL= and optional  for customization  
- **builtin provider**: Uses unified LANG=C.UTF-8
LANGUAGE=
LC_CTYPE="C.UTF-8"
LC_NUMERIC="C.UTF-8"
LC_TIME="C.UTF-8"
LC_COLLATE="C.UTF-8"
LC_MONETARY="C.UTF-8"
LC_MESSAGES="C.UTF-8"
LC_PAPER="C.UTF-8"
LC_NAME="C.UTF-8"
LC_ADDRESS="C.UTF-8"
LC_TELEPHONE="C.UTF-8"
LC_MEASUREMENT="C.UTF-8"
LC_IDENTIFICATION="C.UTF-8"
LC_ALL= parameter
- **default provider**: Special case for pg_catalog collations

The function includes version compatibility handling for PostgreSQL 10.0+, 12.0+, 15.0+, 16.0+, and 17.0+ to manage evolving collation catalog schema changes.

## Parameters / Member Variables
- `*fout`: Archive structure containing dump options and output methods
- `*collinfo`: CollInfo structure containing collation metadata including OID, name, namespace, and owner
## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlQueryForSingleRow](../E/ExecuteSqlQueryForSingleRow.md)
  - fmtQualifiedDumpable
  - [fmtId](../f/fmtId.md)
  - appendStringLiteralAH
  - [ArchiveEntry](../A/ArchiveEntry.md)
  - [dumpComment](dumpComment.md)
  - [binary_upgrade_extension_member](../b/binary_upgrade_extension_member.md)
  - pg_log_warning
- Called from (representative examples):
  - [dumpDumpableObject](dumpDumpableObject.md)

## Notes and Other Information
- Only operates in schema dump mode (skipped when dopt->dataOnly is true)
- Includes extensive version compatibility logic for different PostgreSQL releases
- Validates collation properties and warns about invalid configurations
- Handles binary upgrade scenarios with collation version preservation
- Supports deterministic/non-deterministic collation settings (PostgreSQL 12+)
- Manages ICU collation rules for advanced locale customization (PostgreSQL 16+)
- Generates proper DROP statements for clean restoration

## Simplified Source

```c
static void
dumpCollation(Archive *fout, const CollInfo *collinfo)
{
    DumpOptions *dopt = fout->dopt;
    PQExpBuffer query, q, delq;
    char *qcollname;
    PGresult *res;
    const char *collprovider, *collcollate, *collctype;
    const char *colllocale, *collicurules;

    // Skip in data-only mode
    if (dopt->dataOnly)
        return;

    // Initialize buffers and format collation name
    query = createPQExpBuffer();
    q = createPQExpBuffer();
    delq = createPQExpBuffer();
    qcollname = pg_strdup(fmtId(collinfo->dobj.name));

    // Build version-aware query for collation properties
    appendPQExpBufferStr(query, "SELECT ");

    // Add provider and version fields based on PostgreSQL version
    if (fout->remoteVersion >= 100000)
        appendPQExpBufferStr(query, "collprovider, collversion, ");
    else
        appendPQExpBufferStr(query, "'c' AS collprovider, NULL AS collversion, ");

    // Add deterministic field (PostgreSQL 12+)
    if (fout->remoteVersion >= 120000)
        appendPQExpBufferStr(query, "collisdeterministic, ");
    else
        appendPQExpBufferStr(query, "true AS collisdeterministic, ");

    // Add locale field with version compatibility
    if (fout->remoteVersion >= 170000)
        appendPQExpBufferStr(query, "colllocale, ");
    else if (fout->remoteVersion >= 150000)
        appendPQExpBufferStr(query, "colliculocale AS colllocale, ");
    else
        appendPQExpBufferStr(query, "NULL AS colllocale, ");

    // Add ICU rules field (PostgreSQL 16+)
    if (fout->remoteVersion >= 160000)
        appendPQExpBufferStr(query, "collicurules, ");
    else
        appendPQExpBufferStr(query, "NULL AS collicurules, ");

    // Complete query with standard fields
    appendPQExpBuffer(query, "collcollate, collctype "
                      "FROM pg_catalog.pg_collation c "
                      "WHERE c.oid = '%u'::pg_catalog.oid",
                      collinfo->dobj.catId.oid);

    res = ExecuteSqlQueryForSingleRow(fout, query->data);

    // Extract collation properties
    collprovider = PQgetvalue(res, 0, PQfnumber(res, "collprovider"));

    // Handle nullable collate/ctype fields
    collcollate = PQgetisnull(res, 0, PQfnumber(res, "collcollate")) ?
                  NULL : PQgetvalue(res, 0, PQfnumber(res, "collcollate"));
    collctype = PQgetisnull(res, 0, PQfnumber(res, "collctype")) ?
                NULL : PQgetvalue(res, 0, PQfnumber(res, "collctype"));

    // Handle empty strings as NULL for older versions
    if (fout->remoteVersion < 150000) {
        if (collcollate && collcollate[0] == '\0') collcollate = NULL;
        if (collctype && collctype[0] == '\0') collctype = NULL;
    }

    colllocale = PQgetisnull(res, 0, PQfnumber(res, "colllocale")) ?
                 NULL : PQgetvalue(res, 0, PQfnumber(res, "colllocale"));
    collicurules = PQgetisnull(res, 0, PQfnumber(res, "collicurules")) ?
                   NULL : PQgetvalue(res, 0, PQfnumber(res, "collicurules"));

    // Build DROP statement
    appendPQExpBuffer(delq, "DROP COLLATION %s;\n",
                      fmtQualifiedDumpable(collinfo));

    // Build CREATE statement with provider
    appendPQExpBuffer(q, "CREATE COLLATION %s (provider = ",
                      fmtQualifiedDumpable(collinfo));

    // Add provider type
    switch (collprovider[0]) {
        case 'b': appendPQExpBufferStr(q, "builtin"); break;
        case 'c': appendPQExpBufferStr(q, "libc"); break;
        case 'i': appendPQExpBufferStr(q, "icu"); break;
        case 'd': appendPQExpBufferStr(q, "default"); break;
        default: pg_fatal("unrecognized collation provider: %s", collprovider);
    }

    // Add deterministic setting if false
    if (strcmp(PQgetvalue(res, 0, PQfnumber(res, "collisdeterministic")), "f") == 0)
        appendPQExpBufferStr(q, ", deterministic = false");

    // Add locale parameters based on provider
    if (collprovider[0] == 'b' && colllocale) {
        // Builtin provider uses locale parameter
        appendPQExpBufferStr(q, ", locale = ");
        appendStringLiteralAH(q, colllocale, fout);
    } else if (collprovider[0] == 'i') {
        // ICU provider
        if (fout->remoteVersion >= 150000 && colllocale) {
            appendPQExpBufferStr(q, ", locale = ");
            appendStringLiteralAH(q, colllocale, fout);
        } else if (collcollate) {
            appendPQExpBufferStr(q, ", locale = ");
            appendStringLiteralAH(q, collcollate, fout);
        }

        // Add ICU rules if present
        if (collicurules) {
            appendPQExpBufferStr(q, ", rules = ");
            appendStringLiteralAH(q, collicurules, fout);
        }
    } else if (collprovider[0] == 'c' && collcollate && collctype) {
        // libc provider
        if (strcmp(collcollate, collctype) == 0) {
            appendPQExpBufferStr(q, ", locale = ");
            appendStringLiteralAH(q, collcollate, fout);
        } else {
            appendPQExpBufferStr(q, ", lc_collate = ");
            appendStringLiteralAH(q, collcollate, fout);
            appendPQExpBufferStr(q, ", lc_ctype = ");
            appendStringLiteralAH(q, collctype, fout);
        }
    }

    // Add version for binary upgrade
    if (dopt->binary_upgrade) {
        int i_collversion = PQfnumber(res, "collversion");
        if (!PQgetisnull(res, 0, i_collversion)) {
            appendPQExpBufferStr(q, ", version = ");
            appendStringLiteralAH(q, PQgetvalue(res, 0, i_collversion), fout);
        }
    }

    appendPQExpBufferStr(q, ");\n");

    // Handle binary upgrade
    if (dopt->binary_upgrade)
        binary_upgrade_extension_member(q, &collinfo->dobj,
                                       "COLLATION", qcollname,
                                       collinfo->dobj.namespace->dobj.name);

    // Register with archive
    if (collinfo->dobj.dump & DUMP_COMPONENT_DEFINITION)
        ArchiveEntry(fout, collinfo->dobj.catId, collinfo->dobj.dumpId,
                    ARCHIVE_OPTS(.tag = collinfo->dobj.name,
                                .namespace = collinfo->dobj.namespace->dobj.name,
                                .owner = collinfo->rolname,
                                .description = "COLLATION",
                                .section = SECTION_PRE_DATA,
                                .createStmt = q->data,
                                .dropStmt = delq->data));

    // Dump comments
    if (collinfo->dobj.dump & DUMP_COMPONENT_COMMENT)
        dumpComment(fout, "COLLATION", qcollname,
                   collinfo->dobj.namespace->dobj.name, collinfo->rolname,
                   collinfo->dobj.catId, 0, collinfo->dobj.dumpId);

    // Cleanup
    PQclear(res);
    destroyPQExpBuffer(query);
    destroyPQExpBuffer(q);
    destroyPQExpBuffer(delq);
    free(qcollname);
}
```