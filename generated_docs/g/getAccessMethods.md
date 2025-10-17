# getAccessMethods

## Location
[src/bin/pg_dump/pg_dump.c:6240-6319](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L6240-L6319)

## Overview
Reads all user-defined access methods from the PostgreSQL system catalogs and returns them in an AccessMethodInfo structure array for pg_dump processing.

## Definition
```c
AccessMethodInfo *getAccessMethods(Archive *fout, int *numAccessMethods)
```

## Detailed Description
The getAccessMethods function is part of pg_dump's catalog scanning infrastructure that retrieves all access method objects defined in the database. It queries the pg_am system catalog to collect access method metadata including names, handlers, and types. Access methods define how PostgreSQL stores and retrieves data (e.g., B-tree, Hash, GIN, GiST). The function handles version differences between PostgreSQL versions, with special handling for pre-9.6 systems that had a different access method API. For modern versions (9.6+), it retrieves the amhandler and amtype; for older versions, it provides default values. The function uses selectDumpableAccessMethod to determine which access methods should be included in the dump.

## Parameters / Member Variables
- `fout`: Archive structure containing connection and dump configuration information
- `numAccessMethods`: Output parameter that receives the total number of access methods found

## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - [AssignDumpId](../A/AssignDumpId.md)
  - [selectDumpableAccessMethod](../s/selectDumpableAccessMethod.md)
  - [pg_malloc](../p/pg_malloc.md)
  - atooid
  - [pg_strdup](../p/pg_strdup.md)
- Called from (representative examples):
  - [getSchemaData](getSchemaData.md)

## Notes and Other Information
- Handles version compatibility between PostgreSQL 9.6+ (which introduced CREATE ACCESS METHOD) and earlier versions
- Access methods do not have namespaces, so the namespace field is set to NULL
- For pre-9.6 versions, the function facilitates OID-to-name mapping through findAccessMethodByOid
- The amhandler field contains the function that implements the access method's operations
- Each access method is assigned a dump ID for dependency tracking
- Memory allocation is done upfront for the entire access method array based on query results

## Simplified Source

```c
AccessMethodInfo *getAccessMethods(Archive *fout, int *numAccessMethods)
{
    PGresult *res;
    int ntups, i;
    PQExpBuffer query;
    AccessMethodInfo *aminfo;
    int i_tableoid, i_oid, i_amname, i_amhandler, i_amtype;

    query = createPQExpBuffer();

    // Build query with version-specific fields
    appendPQExpBufferStr(query, "SELECT tableoid, oid, amname, ");
    if (fout->remoteVersion >= 90600) {
        // PostgreSQL 9.6+ has amtype and amhandler
        appendPQExpBufferStr(query,
                             "amtype, "
                             "amhandler::pg_catalog.regproc AS amhandler ");
    } else {
        // Pre-9.6 versions use default values
        appendPQExpBufferStr(query,
                             "'i'::pg_catalog.\"char\" AS amtype, "
                             "'-'::pg_catalog.regproc AS amhandler ");
    }
    appendPQExpBufferStr(query, "FROM pg_am");

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    ntups = PQntuples(res);
    *numAccessMethods = ntups;

    // Allocate array for access method info
    aminfo = (AccessMethodInfo *) pg_malloc(ntups * sizeof(AccessMethodInfo));

    // Get column indices
    i_tableoid = PQfnumber(res, "tableoid");
    i_oid = PQfnumber(res, "oid");
    i_amname = PQfnumber(res, "amname");
    i_amhandler = PQfnumber(res, "amhandler");
    i_amtype = PQfnumber(res, "amtype");

    // Process each access method
    for (i = 0; i < ntups; i++) {
        // Initialize dump object
        aminfo[i].dobj.objType = DO_ACCESS_METHOD;
        aminfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
        aminfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
        AssignDumpId(&aminfo[i].dobj);
        aminfo[i].dobj.name = pg_strdup(PQgetvalue(res, i, i_amname));
        aminfo[i].dobj.namespace = NULL;  // Access methods have no namespace

        // Set access method properties
        aminfo[i].amhandler = pg_strdup(PQgetvalue(res, i, i_amhandler));
        aminfo[i].amtype = *(PQgetvalue(res, i, i_amtype));

        // Determine if this access method should be dumped
        selectDumpableAccessMethod(&(aminfo[i]), fout);
    }

    PQclear(res);
    destroyPQExpBuffer(query);

    return aminfo;
}
```