# getDependencies

## Location
[src/bin/pg_dump/pg_dump.c:18545-18697](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L18545-L18697)

## Overview
Obtains dependency data from PostgreSQL catalogs to establish ordering relationships between database objects during dump and restore operations.

## Definition
```c
static void getDependencies(Archive *fout)
```

## Detailed Description
This function queries the pg_depend system catalog to collect dependency information between database objects. It processes various types of dependencies except PIN ('p') and EXTENSION ('e') dependencies, which are handled elsewhere. The function handles special cases for pg_amop and pg_amproc entries by translating their dependencies to their parent opfamily objects.

Key behaviors:
- Ignores sub-object columns to treat column dependencies as table dependencies
- Handles special dependency translation for pg_amop and pg_amproc entries to opfamilies
- Excludes internal dependencies and self-dependencies for opfamily entries
- Reverses dependency direction for composite type-table relationships to ensure proper ordering
- Marks objects with extension dependencies ('x' type) for special handling
- Uses ordered results to optimize object lookups when processing multiple dependencies

## Parameters / Member Variables
- `fout`: Archive context for the dump operation

## Dependencies
- Functions called/Symbols referenced:
  - pg_log_info
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
  - pg_log_warning (in debug builds)
- Called from:
  - [main](../m/main.md) (in src/bin/pg_dump/pg_dump.c:983)

## Notes and Other Information
- Static function, only accessible within pg_dump.c
- Failure to find objects mentioned in pg_depend is expected and handled gracefully (e.g., TOAST tables)
- Special handling for composite type dependencies ensures proper DROP ordering matches dump restoration needs
- Uses complex UNION query to handle opfamily dependency translation
- Results are ordered by classid and objid for optimization
- Extension dependencies ('x' type) are marked but still processed for ordering purposes
- Debug warnings for missing objects are conditionally compiled (NOT_USED)

## Simplified Source

```c
static void
getDependencies(Archive *fout)
{
    PQExpBuffer query;
    PGresult *res;
    int ntups, i;
    int i_classid, i_objid, i_refclassid, i_refobjid, i_deptype;
    DumpableObject *dobj, *refdobj;

    pg_log_info("reading dependency data");

    query = createPQExpBuffer();

    // Main dependency query - excludes PIN and EXTENSION dependencies
    appendPQExpBufferStr(query, "SELECT "
                               "classid, objid, refclassid, refobjid, deptype "
                               "FROM pg_depend "
                               "WHERE deptype != 'p' AND deptype != 'e'\n");

    // Special handling for pg_amop entries - translate to opfamily dependencies
    appendPQExpBufferStr(query, "UNION ALL\n"
                               "SELECT 'pg_opfamily'::regclass AS classid, amopfamily AS objid, refclassid, refobjid, deptype "
                               "FROM pg_depend d, pg_amop o "
                               "WHERE deptype NOT IN ('p', 'e', 'i') AND "
                               "classid = 'pg_amop'::regclass AND objid = o.oid "
                               "AND NOT (refclassid = 'pg_opfamily'::regclass AND amopfamily = refobjid)\n");

    // Special handling for pg_amproc entries - translate to opfamily dependencies
    appendPQExpBufferStr(query, "UNION ALL\n"
                               "SELECT 'pg_opfamily'::regclass AS classid, amprocfamily AS objid, refclassid, refobjid, deptype "
                               "FROM pg_depend d, pg_amproc p "
                               "WHERE deptype NOT IN ('p', 'e', 'i') AND "
                               "classid = 'pg_amproc'::regclass AND objid = p.oid "
                               "AND NOT (refclassid = 'pg_opfamily'::regclass AND amprocfamily = refobjid)\n");

    // Order for efficiency
    appendPQExpBufferStr(query, "ORDER BY 1,2");

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    ntups = PQntuples(res);

    i_classid = PQfnumber(res, "classid");
    i_objid = PQfnumber(res, "objid");
    i_refclassid = PQfnumber(res, "refclassid");
    i_refobjid = PQfnumber(res, "refobjid");
    i_deptype = PQfnumber(res, "deptype");

    // Process dependencies
    dobj = NULL;

    for (i = 0; i < ntups; i++) {
        CatalogId objId, refobjId;
        char deptype;

        objId.tableoid = atooid(PQgetvalue(res, i, i_classid));
        objId.oid = atooid(PQgetvalue(res, i, i_objid));
        refobjId.tableoid = atooid(PQgetvalue(res, i, i_refclassid));
        refobjId.oid = atooid(PQgetvalue(res, i, i_refobjid));
        deptype = *(PQgetvalue(res, i, i_deptype));

        // Find referring object (cache for efficiency)
        if (dobj == NULL ||
            dobj->catId.tableoid != objId.tableoid ||
            dobj->catId.oid != objId.oid)
            dobj = findObjectByCatalogId(objId);

        // Objects may not be found (e.g., TOAST tables are not collected)
        if (dobj == NULL)
            continue;

        // Find referenced object
        refdobj = findObjectByCatalogId(refobjId);
        if (refdobj == NULL)
            continue;

        // Mark extension dependencies for special handling
        if (deptype == 'x')
            dobj->depends_on_ext = true;

        // Special case: reverse dependency direction for composite type-table relationships
        if (deptype == 'i' &&
            dobj->objType == DO_TABLE &&
            refdobj->objType == DO_TYPE)
            addObjectDependency(refdobj, dobj->dumpId);
        else
            // Normal case: add dependency
            addObjectDependency(dobj, refdobj->dumpId);
    }

    PQclear(res);
    destroyPQExpBuffer(query);
}
```