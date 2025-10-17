# dumpSearchPath

## Location
[src/bin/pg_dump/pg_dump.c:3614-3675](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L3614-L3675)

## Overview
The  function records the active search_path from the source database to ensure schemas are resolved correctly during restoration.

## Definition

```c
static void
dumpSearchPath(Archive *AH)
```
## Detailed Description
The  function captures the current database's search path and stores it as a restoration command in the dump archive. Rather than using the search_path GUC directly, it queries current_schemas(false) to get the actual resolved schema names, avoiding wildcards like '' that might not be valid during restoration. The function constructs a set_config() call instead of a simple SET command for better backwards compatibility, especially when dealing with empty search paths. The resolved search path is also stored in the Archive structure for use in plain text dumps.

## Parameters / Member Variables
- `*AH`: Pointer to the Archive structure where the search path command will be stored and archived
## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlQueryForSingleRow](../E/ExecuteSqlQueryForSingleRow.md) (executes current_schemas() query)
  - [parsePGArray](../p/parsePGArray.md) (parses the PostgreSQL array result into C array)
  - [createPQExpBuffer](../c/createPQExpBuffer.md)/appendPQExpBufferStr/destroyPQExpBuffer (string buffer management)
  - [fmtId](../f/fmtId.md) (properly quotes schema names as SQL identifiers)
  - appendStringLiteralAH (safely quotes the search path as SQL literal)
  - pg_log_info (logs the search path being saved)
  - [createDumpId](../c/createDumpId.md) (generates unique dump ID)
  - [ArchiveEntry](../A/ArchiveEntry.md) (creates archive entry with SQL command)
  - ARCHIVE_OPTS/SECTION_PRE_DATA (archive configuration macros)
  - [pg_strdup](../p/pg_strdup.md) (duplicates string for storage in AH->searchpath)
- Called from (representative examples):
  - [main](../m/main.md) (pg_dump main function)
  - fmtQualifiedDumpable

## Notes and Other Information
- Uses current_schemas(false) instead of search_path GUC to avoid wildcard expansion issues during restoration
- Employs set_config() rather than SET command for better handling of edge cases like empty search paths
- The resolved search path is stored in both the archive entry and AH->searchpath for different dump formats
- Critical for ensuring schema-qualified object references resolve correctly during restoration
- Placed in SECTION_PRE_DATA to ensure search path is set before other database objects are processed

## Simplified Source

```c
static void
dumpSearchPath(Archive *AH)
{
    PQExpBuffer qry = createPQExpBuffer();
    PQExpBuffer path = createPQExpBuffer();
    char **schemanames = NULL;
    int nschemanames = 0;

    // Get current resolved schema names (not raw search_path to avoid wildcards)
    PGresult *res = ExecuteSqlQueryForSingleRow(AH,
                       "SELECT pg_catalog.current_schemas(false)");

    // Parse the array result into schema names
    if (!parsePGArray(PQgetvalue(res, 0, 0), &schemanames, &nschemanames))
        pg_fatal("could not parse result of current_schemas()");

    // Build comma-separated list of properly quoted schema names
    for (int i = 0; i < nschemanames; i++) {
        if (i > 0)
            appendPQExpBufferStr(path, ", ");
        appendPQExpBufferStr(path, fmtId(schemanames[i]));
    }

    // Create set_config() command (more robust than SET search_path)
    appendPQExpBufferStr(qry, "SELECT pg_catalog.set_config('search_path', ");
    appendStringLiteralAH(qry, path->data, AH);
    appendPQExpBufferStr(qry, ", false);\n");

    // Log and archive the command
    pg_log_info("saving \"search_path = %s\"", path->data);
    ArchiveEntry(AH, nilCatalogId, createDumpId(),
                 ARCHIVE_OPTS(.tag = "SEARCHPATH",
                             .description = "SEARCHPATH",
                             .section = SECTION_PRE_DATA,
                             .createStmt = qry->data));

    // Store for plain text dumps
    AH->searchpath = pg_strdup(qry->data);

    // Cleanup
    free(schemanames);
    PQclear(res);
    destroyPQExpBuffer(qry);
    destroyPQExpBuffer(path);
}
```