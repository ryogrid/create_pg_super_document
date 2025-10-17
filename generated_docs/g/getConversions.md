# getConversions

## Location
[src/bin/pg_dump/pg_dump.c:6172-6239](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L6172-L6239)

## Overview
Reads all conversions from the PostgreSQL system catalogs and returns them in a ConvInfo structure array for pg_dump processing.

## Definition
```c
ConvInfo *getConversions(Archive *fout, int *numConversions)
```

## Detailed Description
The getConversions function is part of pg_dump's catalog scanning infrastructure that retrieves all conversion objects defined in the database. It queries the pg_conversion system catalog to collect conversion metadata including names, namespaces, and owners. Conversions in PostgreSQL define how to transform text from one character encoding to another. The function allocates an array of ConvInfo structures to store the conversion information and uses selectDumpableObject to determine which conversions should be included in the dump based on the current dump configuration. This function operates during the schema discovery phase of pg_dump.

## Parameters / Member Variables
- `fout`: Archive structure containing connection and dump configuration information
- `numConversions`: Output parameter that receives the total number of conversions found

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
- Retrieves all conversions including builtin conversions; system-defined conversions are filtered at dump-out time rather than during collection
- Each conversion is assigned a dump ID for dependency tracking
- Conversions are essential for multi-encoding database environments and proper character set handling
- The function stores basic metadata needed to recreate conversion objects during database restore
- Memory allocation is done upfront for the entire conversion array based on query results

## Simplified Source

```c
ConvInfo *getConversions(Archive *fout, int *numConversions)
{
    PGresult *res;
    int ntups, i;
    PQExpBuffer query;
    ConvInfo *convinfo;
    int i_tableoid, i_oid, i_conname, i_connamespace, i_conowner;

    query = createPQExpBuffer();

    // Query all conversions including builtin ones
    appendPQExpBufferStr(query,
                         "SELECT tableoid, oid, conname, connamespace, conowner "
                         "FROM pg_conversion");

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    ntups = PQntuples(res);
    *numConversions = ntups;

    // Allocate array for conversion info
    convinfo = (ConvInfo *) pg_malloc(ntups * sizeof(ConvInfo));

    // Get column indices
    i_tableoid = PQfnumber(res, "tableoid");
    i_oid = PQfnumber(res, "oid");
    i_conname = PQfnumber(res, "conname");
    i_connamespace = PQfnumber(res, "connamespace");
    i_conowner = PQfnumber(res, "conowner");

    // Process each conversion
    for (i = 0; i < ntups; i++) {
        // Initialize dump object
        convinfo[i].dobj.objType = DO_CONVERSION;
        convinfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
        convinfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
        AssignDumpId(&convinfo[i].dobj);
        convinfo[i].dobj.name = pg_strdup(PQgetvalue(res, i, i_conname));
        convinfo[i].dobj.namespace = findNamespace(atooid(PQgetvalue(res, i, i_connamespace)));

        // Set conversion properties
        convinfo[i].rolname = getRoleName(PQgetvalue(res, i, i_conowner));

        // Determine if this conversion should be dumped
        selectDumpableObject(&(convinfo[i].dobj), fout);
    }

    PQclear(res);
    destroyPQExpBuffer(query);

    return convinfo;
}
```