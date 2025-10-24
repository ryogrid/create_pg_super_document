# getIndexes

## Location
[src/bin/pg_dump/pg_dump.c:7433-7742](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L7433-L7742)

## Overview
Retrieves comprehensive information about all indexes on dumpable tables and creates corresponding DumpableObject entries for use during pg_dump operations.

## Definition

```c
void
getIndexes(Archive *fout, TableInfo tblinfo[], int numTables)
```
## Detailed Description
The getIndexes function performs a sophisticated query against PostgreSQL system catalogs to gather complete index information for all tables marked for dumping. It constructs a single optimized SQL query that retrieves index definitions, statistics, constraint relationships, and metadata from multiple system tables including pg_index, pg_class, pg_constraint, and pg_inherits. The function handles version-specific features like replica identity indexes (9.4+), partitioned indexes (11.0+), and NULLS NOT DISTINCT support (15.0+). For each index found, it creates IndxInfo structures and populates them with detailed metadata. Additionally, when indexes are associated with constraints (primary key, unique, or exclusion), it creates corresponding ConstraintInfo entries, establishing proper dependency relationships for correct dump ordering.

## Parameters / Member Variables
- `*fout`: Archive pointer containing dump configuration and database connection information
- `tblinfo[]`: Array of TableInfo structures representing tables to be dumped
- `numTables`: Number of entries in the tblinfo array
## Dependencies
- Functions called/Symbols referenced:
  - [TableInfo](../T/TableInfo.md) (structure type)
  - [IndxInfo](../I/IndxInfo.md) (structure type)
  - [appendPQExpBufferChar](../a/appendPQExpBufferChar.md)
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - PGRES_TUPLES_OK (constant)
  - [pg_malloc](../p/pg_malloc.md)
  - atooid
  - DO_INDEX (enum value)
  - [AssignDumpId](../A/AssignDumpId.md)
  - [parseOidArray](../p/parseOidArray.md)
  - [SimplePtrList](../S/SimplePtrList.md) (structure type)
  - [ConstraintInfo](../C/ConstraintInfo.md) (structure type)
  - DO_CONSTRAINT (enum value)
- Called from (representative examples):
  - [getSchemaData](getSchemaData.md)
  - [SubRelInfo](../S/SubRelInfo.md) (referenced in header)

## Notes and Other Information
- Only processes tables that have the hasindex flag set and are marked as interesting
- Builds an OID array of target tables to optimize the single SQL query approach
- Handles PostgreSQL version differences with conditional SQL generation
- Creates constraint entries for primary key, unique, and exclusion constraint indexes
- Supports partitioned index inheritance relationships (PostgreSQL 11+)
- Retrieves index statistics columns and values for performance analysis
- The function assumes tblinfo array is sorted by OID for efficient table lookup
- Index data is stored in TableInfo structures rather than returned directly
- Memory management includes proper allocation for IndxInfo arrays and string fields

## Simplified Source

```c
void getIndexes(Archive *fout, TableInfo tblinfo[], int numTables)
{
    PQExpBuffer query = createPQExpBuffer();
    PQExpBuffer tbloids = createPQExpBuffer();
    PGresult   *res;
    int         ntups;
    IndxInfo   *indxinfo;

    // Build array of table OIDs that have indexes and are interesting
    appendPQExpBufferChar(tbloids, '{');
    for (int i = 0; i < numTables; i++)
    {
        TableInfo *tbinfo = &tblinfo[i];

        if (!tbinfo->hasindex || !tbinfo->interesting)
            continue;

        if (tbloids->len > 1)
            appendPQExpBufferChar(tbloids, ',');
        appendPQExpBuffer(tbloids, "%u", tbinfo->dobj.catId.oid);
    }
    appendPQExpBufferChar(tbloids, '}');

    // Build comprehensive query for index information
    appendPQExpBufferStr(query,
        "SELECT t.tableoid, t.oid, i.indrelid, "
        "t.relname AS indexname, "
        "pg_catalog.pg_get_indexdef(i.indexrelid) AS indexdef, "
        "i.indkey, i.indisclustered, "
        "c.contype, c.conname, c.condeferrable, c.condeferred, "
        "c.tableoid AS contableoid, c.oid AS conoid, "
        "pg_catalog.pg_get_constraintdef(c.oid, false) AS condef, "
        "t.reltablespace, t.reloptions AS indreloptions, ");

    // Add version-specific fields
    if (fout->remoteVersion >= 90400)
        appendPQExpBufferStr(query, "i.indisreplident, ");
    else
        appendPQExpBufferStr(query, "false AS indisreplident, ");

    // Add partitioning and statistics fields for v11+
    if (fout->remoteVersion >= 110000)
        appendPQExpBufferStr(query,
            "inh.inhparent AS parentidx, "
            "i.indnkeyatts, i.indnatts, "
            "index_statistics_columns, index_statistics_values, ");
    else
        appendPQExpBufferStr(query,
            "0 AS parentidx, i.indnatts AS indnkeyatts, "
            "i.indnatts, '' AS indstatcols, '' AS indstatvals, ");

    // Add NULLS NOT DISTINCT support for v15+
    if (fout->remoteVersion >= 150000)
        appendPQExpBufferStr(query, "i.indnullsnotdistinct ");
    else
        appendPQExpBufferStr(query, "false AS indnullsnotdistinct ");

    // Join with pg_index, pg_class, pg_constraint tables
    appendPQExpBuffer(query,
        "FROM unnest('%s'::pg_catalog.oid[]) AS src(tbloid) "
        "JOIN pg_catalog.pg_index i ON (src.tbloid = i.indrelid) "
        "JOIN pg_catalog.pg_class t ON (t.oid = i.indexrelid) "
        "LEFT JOIN pg_catalog.pg_constraint c ON (...) "
        "WHERE i.indisvalid AND i.indisready "
        "ORDER BY i.indrelid, indexname",
        tbloids->data);

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    ntups = PQntuples(res);

    // Extract column indices from result
    int i_tableoid = PQfnumber(res, "tableoid");
    int i_oid = PQfnumber(res, "oid");
    int i_indexname = PQfnumber(res, "indexname");
    // ... more column indices

    indxinfo = (IndxInfo *) pg_malloc(ntups * sizeof(IndxInfo));

    // Process results grouped by table
    int curtblindx = -1;
    for (int j = 0; j < ntups;)
    {
        Oid indrelid = atooid(PQgetvalue(res, j, i_indrelid));
        TableInfo *tbinfo = NULL;
        int numinds;

        // Count indexes for this table
        for (numinds = 1; numinds < ntups - j; numinds++)
            if (atooid(PQgetvalue(res, j + numinds, i_indrelid)) != indrelid)
                break;

        // Find corresponding TableInfo
        while (++curtblindx < numTables)
        {
            tbinfo = &tblinfo[curtblindx];
            if (tbinfo->dobj.catId.oid == indrelid)
                break;
        }

        // Store index array reference in table
        tbinfo->indexes = indxinfo + j;
        tbinfo->numIndexes = numinds;

        // Process each index for this table
        for (int c = 0; c < numinds; c++, j++)
        {
            // Initialize index object
            indxinfo[j].dobj.objType = DO_INDEX;
            indxinfo[j].dobj.catId.oid = atooid(PQgetvalue(res, j, i_oid));
            AssignDumpId(&indxinfo[j].dobj);
            indxinfo[j].dobj.name = pg_strdup(PQgetvalue(res, j, i_indexname));
            indxinfo[j].indextable = tbinfo;
            indxinfo[j].indexdef = pg_strdup(PQgetvalue(res, j, i_indexdef));

            // Parse index keys and attributes
            parseOidArray(PQgetvalue(res, j, i_indkey),
                         indxinfo[j].indkeys, indxinfo[j].indnattrs);

            char contype = *(PQgetvalue(res, j, i_contype));

            // Create constraint entry for PK/unique/exclusion indexes
            if (contype == 'p' || contype == 'u' || contype == 'x')
            {
                ConstraintInfo *constrinfo = (ConstraintInfo *) pg_malloc(sizeof(ConstraintInfo));
                constrinfo->dobj.objType = DO_CONSTRAINT;
                constrinfo->dobj.catId.oid = atooid(PQgetvalue(res, j, i_conoid));
                AssignDumpId(&constrinfo->dobj);
                constrinfo->dobj.name = pg_strdup(PQgetvalue(res, j, i_conname));
                constrinfo->contable = tbinfo;
                constrinfo->contype = contype;
                constrinfo->conindex = indxinfo[j].dobj.dumpId;

                indxinfo[j].indexconstraint = constrinfo->dobj.dumpId;
            }
            else
            {
                indxinfo[j].indexconstraint = 0;
            }
        }
    }

    PQclear(res);
    destroyPQExpBuffer(query);
    destroyPQExpBuffer(tbloids);
}
```