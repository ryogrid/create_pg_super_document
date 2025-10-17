# buildMatViewRefreshDependencies

## Location
[src/bin/pg_dump/pg_dump.c:2899-3013](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L2899-L3013)

## Overview
Establishes dependency relationships between materialized view refresh operations to ensure proper dump ordering when materialized views depend on other materialized views.

## Definition

```c
static void
buildMatViewRefreshDependencies(Archive *fout)
```
## Detailed Description
This function builds dependency relationships for materialized view refresh operations by querying the database to find all materialized views that depend on other materialized views through rewrite rules. It uses a recursive CTE to traverse the dependency chain and ensures that when dumping, a materialized view's refresh operation depends on the refresh operations of all materialized views it references. The function also propagates the 'populated' status, marking dependent materialized views as unpopulated if any of their dependencies are unpopulated.

## Parameters / Member Variables
- `*fout`: Pointer to Archive structure representing the dump output context
## Dependencies
- Functions called/Symbols referenced:
  - [createPQExpBuffer](../c/createPQExpBuffer.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - [PQntuples](../P/PQntuples.md)
  - [PQfnumber](../P/PQfnumber.md)
  - [PQgetvalue](../P/PQgetvalue.md)
  - atooid
  - [findObjectByCatalogId](../f/findObjectByCatalogId.md)
  - [addObjectDependency](../a/addObjectDependency.md)
  - [PQclear](../P/PQclear.md)
  - [destroyPQExpBuffer](../d/destroyPQExpBuffer.md)
- Types referenced:
  - [Archive](../A/Archive.md)
  - PQExpBuffer
  - [PGresult](../P/PGresult.md)
  - [CatalogId](../C/CatalogId.md)
  - DumpableObject
  - [TableInfo](../T/TableInfo.md)
  - RELKIND_MATVIEW
  - RELKIND_VIEW
  - DO_TABLE
  - DO_REFRESH_MATVIEW
  - PGRES_TUPLES_OK
- Called from:
  - [main](../m/main.md)

## Notes and Other Information
- Only processes databases with PostgreSQL version 9.3 or later (when materialized views were introduced)
- Uses a complex recursive SQL query to find transitive dependencies between materialized views
- Handles dependency chains where materialized view A depends on B, and B depends on C
- Properly handles mixed dependencies involving both materialized views and regular views
- Ensures that unpopulated status propagates through the dependency chain
- Must be called after all objects are created but before they are sorted for dumping
- Critical for maintaining data consistency when restoring materialized views that reference other materialized views

## Simplified Source

```c
static void buildMatViewRefreshDependencies(Archive *fout) {
    PQExpBuffer query;
    PGresult *res;
    int ntups, i;

    // Skip for pre-9.3 versions (no materialized views)
    if (fout->remoteVersion < 90300)
        return;

    // Query to find materialized view dependencies using recursive CTE
    query = createPQExpBuffer();
    appendPQExpBufferStr(query,
        "WITH RECURSIVE w AS ("
        "  SELECT d1.objid, d2.refobjid, c2.relkind AS refrelkind "
        "  FROM pg_depend d1 "
        "  JOIN pg_class c1 ON c1.oid = d1.objid AND c1.relkind = 'm' "
        "  JOIN pg_rewrite r1 ON r1.ev_class = d1.objid "
        "  JOIN pg_depend d2 ON d2.classid = 'pg_rewrite'::regclass AND d2.objid = r1.oid "
        "  JOIN pg_class c2 ON c2.oid = d2.refobjid AND c2.relkind IN ('m','v') "
        "  WHERE d1.classid = 'pg_class'::regclass "
        "  UNION "
        "  SELECT w.objid, d3.refobjid, c3.relkind "
        "  FROM w JOIN pg_rewrite r3 ON r3.ev_class = w.refobjid "
        "  JOIN pg_depend d3 ON d3.classid = 'pg_rewrite'::regclass AND d3.objid = r3.oid "
        "  JOIN pg_class c3 ON c3.oid = d3.refobjid AND c3.relkind IN ('m','v') "
        ") "
        "SELECT 'pg_class'::regclass::oid AS classid, objid, refobjid "
        "FROM w WHERE refrelkind = 'm'");

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    ntups = PQntuples(res);

    // Process each dependency relationship
    for (i = 0; i < ntups; i++) {
        CatalogId objId, refobjId;
        DumpableObject *dobj, *refdobj;
        TableInfo *tbinfo, *reftbinfo;

        // Parse catalog IDs from query result
        objId.tableoid = atooid(PQgetvalue(res, i, 0));
        objId.oid = atooid(PQgetvalue(res, i, 1));
        refobjId.tableoid = objId.tableoid;
        refobjId.oid = atooid(PQgetvalue(res, i, 2));

        // Find objects and establish dependency
        dobj = findObjectByCatalogId(objId);
        refdobj = findObjectByCatalogId(refobjId);

        if (dobj && refdobj) {
            tbinfo = (TableInfo *) dobj;
            reftbinfo = (TableInfo *) refdobj;

            // Add dependency between refresh operations
            addObjectDependency(tbinfo->dataObj, reftbinfo->dataObj->dumpId);

            // Propagate unpopulated status
            if (!reftbinfo->relispopulated)
                tbinfo->relispopulated = false;
        }
    }

    PQclear(res);
    destroyPQExpBuffer(query);
}
```