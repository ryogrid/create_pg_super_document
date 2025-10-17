# getCollations

## Location
[src/bin/pg_dump/pg_dump.c:6100-6171](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L6100-L6171)

## Overview
Reads all collations from the PostgreSQL system catalogs and returns them in a CollInfo structure array for pg_dump processing.

## Definition
```c
CollInfo *getCollations(Archive *fout, int *numCollations)
```

## Detailed Description
The getCollations function is part of pg_dump's catalog scanning infrastructure that retrieves all collation objects defined in the database. It queries the pg_collation system catalog to collect collation metadata including names, namespaces, owners, and encoding information. The function allocates an array of CollInfo structures to store the collation information and uses selectDumpableObject to determine which collations should be included in the dump based on the current dump configuration. This function operates during the schema discovery phase of pg_dump and is essential for preserving locale-specific sorting and character classification behavior.

## Parameters / Member Variables
- `fout`: Archive structure containing connection and dump configuration information
- `numCollations`: Output parameter that receives the total number of collations found

## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - [findNamespace](../f/findNamespace.md)
  - [getRoleName](getRoleName.md)
  - [AssignDumpId](../A/AssignDumpId.md)
  - [selectDumpableObject](../s/selectDumpableObject.md)
  - [pg_malloc](../p/pg_malloc.md)
  - atooid
- Called from (representative examples):
  - [getSchemaData](getSchemaData.md)

## Notes and Other Information
- Retrieves all collations including builtin collations; system-defined collations are filtered at dump-out time rather than during collection
- Each collation is assigned a dump ID for dependency tracking
- The collencoding field stores the encoding associated with the collation
- Collations are critical for proper text sorting and comparison behavior in restored databases
- Memory allocation is done upfront for the entire collation array based on query results

## Simplified Source

```c
CollInfo *getCollations(Archive *fout, int *numCollations)
{
    PGresult *res;
    int ntups, i;
    PQExpBuffer query;
    CollInfo *collinfo;
    int i_tableoid, i_oid, i_collname, i_collnamespace, i_collowner, i_collencoding;

    query = createPQExpBuffer();

    // Query all collations including builtin ones
    appendPQExpBufferStr(query,
                         "SELECT tableoid, oid, collname, collnamespace, "
                         "collowner, collencoding "
                         "FROM pg_collation");

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    ntups = PQntuples(res);
    *numCollations = ntups;

    // Allocate array for collation info
    collinfo = (CollInfo *) pg_malloc(ntups * sizeof(CollInfo));

    // Get column indices
    i_tableoid = PQfnumber(res, "tableoid");
    i_oid = PQfnumber(res, "oid");
    i_collname = PQfnumber(res, "collname");
    i_collnamespace = PQfnumber(res, "collnamespace");
    i_collowner = PQfnumber(res, "collowner");
    i_collencoding = PQfnumber(res, "collencoding");

    // Process each collation
    for (i = 0; i < ntups; i++) {
        // Initialize dump object
        collinfo[i].dobj.objType = DO_COLLATION;
        collinfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
        collinfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
        AssignDumpId(&collinfo[i].dobj);
        collinfo[i].dobj.name = pg_strdup(PQgetvalue(res, i, i_collname));
        collinfo[i].dobj.namespace = findNamespace(atooid(PQgetvalue(res, i, i_collnamespace)));

        // Set collation properties
        collinfo[i].rolname = getRoleName(PQgetvalue(res, i, i_collowner));
        collinfo[i].collencoding = atoi(PQgetvalue(res, i, i_collencoding));

        // Determine if this collation should be dumped
        selectDumpableObject(&(collinfo[i].dobj), fout);
    }

    PQclear(res);
    destroyPQExpBuffer(query);

    return collinfo;
}
```