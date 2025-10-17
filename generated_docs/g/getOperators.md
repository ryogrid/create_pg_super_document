# getOperators

## Location
[src/bin/pg_dump/pg_dump.c:6018-6099](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L6018-L6099)

## Overview
Reads all operators from the PostgreSQL system catalogs and returns them in an OprInfo structure array for pg_dump processing.

## Definition

```c
OprInfo *
getOperators(Archive *fout, int *numOprs)
```
## Detailed Description
The getOperators function is part of pg_dump's catalog scanning infrastructure that retrieves all operators defined in the database. It queries the pg_operator system catalog to collect operator metadata including names, namespaces, owners, operand types, and implementation functions. The function allocates an array of OprInfo structures to store the operator information and uses selectDumpableObject to determine which operators should be included in the dump based on the current dump configuration. This function operates during the schema discovery phase of pg_dump.

## Parameters / Member Variables
- `*fout`: Archive structure containing connection and dump configuration information
- `*numOprs`: Output parameter that receives the total number of operators found
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
- Retrieves all operators including builtin operators; system-defined operators are filtered at dump-out time rather than during collection
- Each operator is assigned a dump ID for dependency tracking
- The function populates OprInfo structures with catalog metadata needed for proper operator recreation during restore
- Memory allocation is done upfront for the entire operator array based on query results

## Simplified Source

```c
OprInfo *getOperators(Archive *fout, int *numOprs)
{
    PGresult *res;
    int ntups, i;
    PQExpBuffer query = createPQExpBuffer();
    OprInfo *oprinfo;
    int i_tableoid, i_oid, i_oprname, i_oprnamespace, i_oprowner,
        i_oprkind, i_oprleft, i_oprright, i_oprcode;

    // Query all operators including builtin ones
    appendPQExpBufferStr(query,
                         "SELECT tableoid, oid, oprname, oprnamespace, "
                         "oprowner, oprkind, oprleft, oprright, "
                         "oprcode::oid AS oprcode "
                         "FROM pg_operator");

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    ntups = PQntuples(res);
    *numOprs = ntups;

    // Allocate array for operator info
    oprinfo = (OprInfo *) pg_malloc(ntups * sizeof(OprInfo));

    // Get column indices
    i_tableoid = PQfnumber(res, "tableoid");
    i_oid = PQfnumber(res, "oid");
    i_oprname = PQfnumber(res, "oprname");
    i_oprnamespace = PQfnumber(res, "oprnamespace");
    i_oprowner = PQfnumber(res, "oprowner");
    i_oprkind = PQfnumber(res, "oprkind");
    i_oprleft = PQfnumber(res, "oprleft");
    i_oprright = PQfnumber(res, "oprright");
    i_oprcode = PQfnumber(res, "oprcode");

    // Process each operator
    for (i = 0; i < ntups; i++) {
        // Initialize dump object
        oprinfo[i].dobj.objType = DO_OPERATOR;
        oprinfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
        oprinfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
        AssignDumpId(&oprinfo[i].dobj);
        oprinfo[i].dobj.name = pg_strdup(PQgetvalue(res, i, i_oprname));
        oprinfo[i].dobj.namespace = findNamespace(atooid(PQgetvalue(res, i, i_oprnamespace)));

        // Set operator properties
        oprinfo[i].rolname = getRoleName(PQgetvalue(res, i, i_oprowner));
        oprinfo[i].oprkind = (PQgetvalue(res, i, i_oprkind))[0];
        oprinfo[i].oprleft = atooid(PQgetvalue(res, i, i_oprleft));
        oprinfo[i].oprright = atooid(PQgetvalue(res, i, i_oprright));
        oprinfo[i].oprcode = atooid(PQgetvalue(res, i, i_oprcode));

        // Determine if this operator should be dumped
        selectDumpableObject(&(oprinfo[i].dobj), fout);
    }

    PQclear(res);
    destroyPQExpBuffer(query);

    return oprinfo;
}
```