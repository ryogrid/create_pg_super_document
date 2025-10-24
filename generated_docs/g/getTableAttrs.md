# getTableAttrs

## Location
[src/bin/pg_dump/pg_dump.c:8805-9361](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L8805-L9361)

## Overview
Retrieves detailed information about table attributes (columns) including names, types, defaults, constraints, and metadata for all interesting tables in a pg_dump operation.

## Definition

```c
void
getTableAttrs(Archive *fout, TableInfo *tblinfo, int numTables)
```
## Detailed Description
The  function performs comprehensive attribute collection for tables that are marked as interesting for dumping. It executes multiple carefully constructed SQL queries against the system catalogs to gather column metadata, default expressions, and CHECK constraints. The function implements version-specific logic to handle features introduced in different PostgreSQL versions (compression in 14.0+, identity columns in 10.0+, missing values in 11.0+, generated columns in 12.0+).

The function operates in three main phases: 1) Collect basic column information from pg_attribute, 2) Retrieve column default expressions from pg_attrdef, and 3) Gather CHECK constraint definitions from pg_constraint. It uses array-based queries with unnest() to efficiently batch operations while maintaining proper locking constraints.

## Parameters / Member Variables
- `*fout`: Archive pointer for the pg_dump operation, containing version info and dump options
- `*tblinfo`: Array of TableInfo structures to populate with attribute information
- `numTables`: Number of tables in the tblinfo array
## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - [appendPQExpBufferChar](../a/appendPQExpBufferChar.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - atooid
  - [pg_malloc](../p/pg_malloc.md)
  - [pg_strdup](../p/pg_strdup.md)
  - [PQgetisnull](../P/PQgetisnull.md)
  - [AssignDumpId](../A/AssignDumpId.md)
  - [shouldPrintColumn](../s/shouldPrintColumn.md)
  - [addObjectDependency](../a/addObjectDependency.md)
  - pg_log_info
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md)
  - ngettext
  - pg_log_error_hint
  - [exit_nicely](../e/exit_nicely.md)
- Called from (representative examples):
  - [getSchemaData](getSchemaData.md)

## Notes and Other Information
- Skips sequences and tables marked as uninteresting to optimize performance
- Implements version-dependent SQL queries for features like compression, identity, and generated columns
- Uses efficient batch queries with unnest() arrays to reduce round-trips to the database
- Handles both inline and separate default/constraint dumping based on various conditions
- Properly manages memory allocation for all attribute arrays within each TableInfo structure
- Validates column numbering and constraint counts for data integrity
- Supports inheritance-aware attribute handling through inhNotNull tracking
- Only processes defaults and constraints when not doing a data-only dump

## Simplified Source

```c
void
getTableAttrs(Archive *fout, TableInfo *tblinfo, int numTables)
{
    PQExpBuffer q = createPQExpBuffer();
    PQExpBuffer tbloids = createPQExpBuffer();
    PGresult *res;
    int ntups, curtblindx;

    // Build array of table OIDs for interesting tables only
    appendPQExpBufferChar(tbloids, '{');
    for (int i = 0; i < numTables; i++) {
        TableInfo *tbinfo = &tblinfo[i];

        // Skip sequences and uninteresting tables
        if (tbinfo->relkind == RELKIND_SEQUENCE || !tbinfo->interesting)
            continue;

        if (tbloids->len > 1)
            appendPQExpBufferChar(tbloids, ',');
        appendPQExpBuffer(tbloids, "%u", tbinfo->dobj.catId.oid);
    }
    appendPQExpBufferChar(tbloids, '}');

    // Query pg_attribute for all column information
    appendPQExpBufferStr(q,
        "SELECT a.attrelid, a.attnum, a.attname, a.attstattarget, "
        "a.attstorage, t.typstorage, a.attnotnull, a.atthasdef, "
        "a.attisdropped, a.attlen, a.attalign, a.attislocal, "
        "pg_catalog.format_type(t.oid, a.atttypmod) AS atttypname, "
        "array_to_string(a.attoptions, ', ') AS attoptions, "
        "CASE WHEN a.attcollation <> t.typcollation "
        "THEN a.attcollation ELSE 0 END AS attcollation ");

    // Add version-specific fields
    if (fout->remoteVersion >= 140000)
        appendPQExpBufferStr(q, "a.attcompression AS attcompression, ");
    if (fout->remoteVersion >= 100000)
        appendPQExpBufferStr(q, "a.attidentity, ");
    if (fout->remoteVersion >= 110000)
        appendPQExpBufferStr(q, "CASE WHEN a.atthasmissing AND NOT a.attisdropped "
                                "THEN a.attmissingval ELSE null END AS attmissingval, ");
    if (fout->remoteVersion >= 120000)
        appendPQExpBufferStr(q, "a.attgenerated ");

    // Complete the query with joins and ordering
    appendPQExpBuffer(q,
        "FROM unnest('%s'::pg_catalog.oid[]) AS src(tbloid) "
        "JOIN pg_catalog.pg_attribute a ON (src.tbloid = a.attrelid) "
        "LEFT JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid) "
        "WHERE a.attnum > 0::pg_catalog.int2 "
        "ORDER BY a.attrelid, a.attnum",
        tbloids->data);

    res = ExecuteSqlQuery(fout, q->data, PGRES_TUPLES_OK);
    ntups = PQntuples(res);

    // Get column indices for result fields
    int i_attrelid = PQfnumber(res, "attrelid");
    int i_attname = PQfnumber(res, "attname");
    int i_atttypname = PQfnumber(res, "atttypname");
    // ... other column indices

    // Process results grouped by table
    curtblindx = -1;
    for (int r = 0; r < ntups;) {
        Oid attrelid = atooid(PQgetvalue(res, r, i_attrelid));
        TableInfo *tbinfo = NULL;
        int numatts;

        // Count attributes for this table
        for (numatts = 1; numatts < ntups - r; numatts++)
            if (atooid(PQgetvalue(res, r + numatts, i_attrelid)) != attrelid)
                break;

        // Find corresponding TableInfo
        while (++curtblindx < numTables) {
            tbinfo = &tblinfo[curtblindx];
            if (tbinfo->dobj.catId.oid == attrelid)
                break;
        }

        // Allocate attribute arrays
        tbinfo->numatts = numatts;
        tbinfo->attnames = (char **) pg_malloc(numatts * sizeof(char *));
        tbinfo->atttypnames = (char **) pg_malloc(numatts * sizeof(char *));
        tbinfo->notnull = (bool *) pg_malloc(numatts * sizeof(bool));
        // ... allocate other attribute arrays

        // Fill in attribute data
        for (int j = 0; j < numatts; j++, r++) {
            tbinfo->attnames[j] = pg_strdup(PQgetvalue(res, r, i_attname));
            tbinfo->atttypnames[j] = pg_strdup(PQgetvalue(res, r, i_atttypname));
            tbinfo->notnull[j] = (PQgetvalue(res, r, i_attnotnull)[0] == 't');
            // ... fill other attribute fields
        }
    }

    PQclear(res);

    // Get column defaults (if not data-only dump)
    if (!fout->dopt->dataOnly) {
        // Query pg_attrdef for default expressions
        // Process default expressions and link to attributes
    }

    // Get CHECK constraints (if not data-only dump)
    if (!fout->dopt->dataOnly) {
        // Query pg_constraint for CHECK constraints
        // Process constraint definitions and link to tables
    }

    // Cleanup
    destroyPQExpBuffer(q);
    destroyPQExpBuffer(tbloids);
}
```