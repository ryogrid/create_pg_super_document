# getOpclasses

## Location
[src/bin/pg_dump/pg_dump.c:6320-6388](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L6320-L6388)

## Overview
Reads all operator classes from the PostgreSQL system catalogs and returns them in an OpclassInfo structure array for pg_dump processing.

## Definition
```c
OpclassInfo *getOpclasses(Archive *fout, int *numOpclasses)
```

## Detailed Description
The getOpclasses function is part of pg_dump's catalog scanning infrastructure that retrieves all operator class objects defined in the database. It queries the pg_opclass system catalog to collect operator class metadata including names, namespaces, owners, and associated access methods. Operator classes define the operators and support functions that an access method can use to work with specific data types (e.g., integer B-tree operators, text comparison operators). The function allocates an array of OpclassInfo structures to store the operator class information and uses selectDumpableObject to determine which operator classes should be included in the dump based on the current dump configuration.

## Parameters / Member Variables
- `fout`: Archive structure containing connection and dump configuration information
- `numOpclasses`: Output parameter that receives the total number of operator classes found

## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - [findNamespace](../f/findNamespace.md)
  - [getRoleName](getRoleName.md)
  - [AssignDumpId](../A/AssignDumpId.md)
  - [selectDumpableObject](../s/selectDumpableObject.md)
  - [pg_malloc](../p/pg_malloc.md)
  - atooid
  - [pg_strdup](../p/pg_strdup.md)
- Called from (representative examples):
  - [getSchemaData](getSchemaData.md)

## Notes and Other Information
- Retrieves all operator classes including builtin operator classes; system-defined operator classes are filtered at dump-out time rather than during collection
- Each operator class is assigned a dump ID for dependency tracking
- The opcmethod field links the operator class to its associated access method (B-tree, Hash, etc.)
- Operator classes are essential for index creation and query optimization as they define how data types can be indexed and compared
- Memory allocation is done upfront for the entire operator class array based on query results
- Operator classes work closely with operator families to provide a complete set of operators for indexing and sorting

## Simplified Source

```c
OpclassInfo *
getOpclasses(Archive *fout, int *numOpclasses)
{
    PGresult   *res;
    int         ntups;
    int         i;
    PQExpBuffer query = createPQExpBuffer();
    OpclassInfo *opcinfo;

    // Query all operator classes from system catalog
    appendPQExpBufferStr(query, "SELECT tableoid, oid, opcmethod, opcname, "
                                "opcnamespace, opcowner "
                                "FROM pg_opclass");

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    ntups = PQntuples(res);
    *numOpclasses = ntups;

    // Allocate array for operator class info
    opcinfo = (OpclassInfo *) pg_malloc(ntups * sizeof(OpclassInfo));

    // Get column indices for result fields
    int i_tableoid = PQfnumber(res, "tableoid");
    int i_oid = PQfnumber(res, "oid");
    int i_opcmethod = PQfnumber(res, "opcmethod");
    int i_opcname = PQfnumber(res, "opcname");
    int i_opcnamespace = PQfnumber(res, "opcnamespace");
    int i_opcowner = PQfnumber(res, "opcowner");

    // Process each operator class result
    for (i = 0; i < ntups; i++)
    {
        // Set object type and catalog info
        opcinfo[i].dobj.objType = DO_OPCLASS;
        opcinfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
        opcinfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));

        // Assign dump ID and basic properties
        AssignDumpId(&opcinfo[i].dobj);
        opcinfo[i].dobj.name = pg_strdup(PQgetvalue(res, i, i_opcname));
        opcinfo[i].dobj.namespace = findNamespace(atooid(PQgetvalue(res, i, i_opcnamespace)));
        opcinfo[i].opcmethod = atooid(PQgetvalue(res, i, i_opcmethod));
        opcinfo[i].rolname = getRoleName(PQgetvalue(res, i, i_opcowner));

        // Determine if this operator class should be dumped
        selectDumpableObject(&(opcinfo[i].dobj), fout);
    }

    PQclear(res);
    destroyPQExpBuffer(query);

    return opcinfo;
}
```