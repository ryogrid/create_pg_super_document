# getCasts

## Location
[src/bin/pg_dump/pg_dump.c:8598-8690](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L8598-L8690)

## Overview
Retrieves basic information about most type casts in the PostgreSQL system for use by pg_dump, excluding certain automatically-created casts like range-to-multirange conversions.

## Definition

```c
CastInfo *
getCasts(Archive *fout, int *numCasts)
```
## Detailed Description
The  function queries the  system catalog to retrieve information about type cast definitions in the database. It implements version-specific logic to handle different PostgreSQL versions, with special filtering for PostgreSQL 14.0+ to exclude automatically-created casts from ranges to their corresponding multirange types.

The function constructs different SQL queries based on the server version: for PostgreSQL 14.0 and later, it includes a subquery to filter out range-to-multirange casts that are automatically created by the system. For each cast found, it creates a  structure and attempts to construct a descriptive name by concatenating the source and target type names.

## Parameters / Member Variables
- `*fout`: Archive pointer for the pg_dump operation, used for executing SQL queries and version checking
- `*numCasts`: Output parameter that receives the number of casts found
## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - [CastInfo](../C/CastInfo.md)
  - [pg_malloc](../p/pg_malloc.md)
  - atooid
  - [AssignDumpId](../A/AssignDumpId.md)
  - [findTypeByOid](../f/findTypeByOid.md)
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - [selectDumpableCast](../s/selectDumpableCast.md)
- Called from (representative examples):
  - [getSchemaData](getSchemaData.md)

## Notes and Other Information
- Version-dependent behavior: PostgreSQL 14.0+ excludes range-to-multirange casts
- Cast names are constructed by concatenating source and target type names for sorting purposes
- Results are ordered by source type OID, then target type OID (ORDER BY 3,4)
- If type information cannot be found, the cast name remains empty
- Each cast's dumpability is determined by selectDumpableCast()
- Handles all cast contexts (implicit, assignment, explicit) and methods (function, inout, binary)

## Simplified Source

```c
CastInfo *
getCasts(Archive *fout, int *numCasts)
{
    PGresult   *res;
    int         ntups;
    PQExpBuffer query = createPQExpBuffer();
    CastInfo   *castinfo;

    // Build version-specific query to retrieve cast information
    if (fout->remoteVersion >= 140000) {
        // PostgreSQL 14+: exclude range-to-multirange casts (auto-created)
        appendPQExpBufferStr(query, "SELECT tableoid, oid, "
                             "castsource, casttarget, castfunc, castcontext, "
                             "castmethod "
                             "FROM pg_cast c "
                             "WHERE NOT EXISTS ( "
                             "SELECT 1 FROM pg_range r "
                             "WHERE c.castsource = r.rngtypid "
                             "AND c.casttarget = r.rngmultitypid "
                             ") "
                             "ORDER BY 3,4");
    } else {
        // Pre-14: get all casts
        appendPQExpBufferStr(query, "SELECT tableoid, oid, "
                             "castsource, casttarget, castfunc, castcontext, "
                             "castmethod "
                             "FROM pg_cast ORDER BY 3,4");
    }

    // Execute query and get results
    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    ntups = PQntuples(res);
    *numCasts = ntups;

    // Allocate memory for cast info array
    castinfo = (CastInfo *) pg_malloc(ntups * sizeof(CastInfo));

    // Get column indices for result processing
    int i_tableoid = PQfnumber(res, "tableoid");
    int i_oid = PQfnumber(res, "oid");
    int i_castsource = PQfnumber(res, "castsource");
    int i_casttarget = PQfnumber(res, "casttarget");
    int i_castfunc = PQfnumber(res, "castfunc");
    int i_castcontext = PQfnumber(res, "castcontext");
    int i_castmethod = PQfnumber(res, "castmethod");

    // Process each cast result
    for (int i = 0; i < ntups; i++) {
        PQExpBufferData namebuf;
        TypeInfo   *sTypeInfo;
        TypeInfo   *tTypeInfo;

        // Initialize dump object metadata
        castinfo[i].dobj.objType = DO_CAST;
        castinfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
        castinfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
        AssignDumpId(&castinfo[i].dobj);

        // Copy cast properties
        castinfo[i].castsource = atooid(PQgetvalue(res, i, i_castsource));
        castinfo[i].casttarget = atooid(PQgetvalue(res, i, i_casttarget));
        castinfo[i].castfunc = atooid(PQgetvalue(res, i, i_castfunc));
        castinfo[i].castcontext = *(PQgetvalue(res, i, i_castcontext));
        castinfo[i].castmethod = *(PQgetvalue(res, i, i_castmethod));

        // Generate cast name from source and target type names for sorting
        initPQExpBuffer(&namebuf);
        sTypeInfo = findTypeByOid(castinfo[i].castsource);
        tTypeInfo = findTypeByOid(castinfo[i].casttarget);
        if (sTypeInfo && tTypeInfo)
            appendPQExpBuffer(&namebuf, "%s %s",
                              sTypeInfo->dobj.name, tTypeInfo->dobj.name);
        castinfo[i].dobj.name = namebuf.data;

        // Determine if this cast should be dumped
        selectDumpableCast(&(castinfo[i]), fout);
    }

    // Cleanup and return results
    PQclear(res);
    destroyPQExpBuffer(query);
    return castinfo;
}
```