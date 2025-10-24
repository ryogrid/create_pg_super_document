# getForeignServers

## Location
[src/bin/pg_dump/pg_dump.c:9752-9845](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L9752-L9845)

## Overview
Reads all foreign servers from the system catalogs and returns them in a structured format for pg_dump processing.

## Definition
```c
ForeignServerInfo *getForeignServers(Archive *fout, int *numForeignServers)
```

## Detailed Description
This function is part of the pg_dump utility and extracts complete information about foreign servers from the PostgreSQL system catalog `pg_foreign_server`. It constructs a comprehensive SQL query to retrieve all relevant server metadata including owner, associated foreign data wrapper, server type, version, access control lists, and connection options. The function processes the query results and creates an array of `ForeignServerInfo` structures, each containing all necessary information for dumping and restoring foreign servers. Each server is assigned a dump ID and evaluated for dumpability. Foreign servers automatically include user mapping components since they can have associated user mappings.

## Parameters / Member Variables
- `fout`: Archive handle for the pg_dump operation, used for executing SQL queries
- `numForeignServers`: Output parameter that receives the count of foreign servers found

## Dependencies
- Functions called/Symbols referenced:
  - [createPQExpBuffer](../c/createPQExpBuffer.md)
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - [PQntuples](../P/PQntuples.md)/PQfnumber/PQgetvalue/PQgetisnull
  - [pg_malloc](../p/pg_malloc.md)
  - atooid
  - [AssignDumpId](../A/AssignDumpId.md)
  - [getRoleName](getRoleName.md)
  - [selectDumpableObject](../s/selectDumpableObject.md)
  - [pg_strdup](../p/pg_strdup.md)
  - [PQclear](../P/PQclear.md)
  - [destroyPQExpBuffer](../d/destroyPQExpBuffer.md)
- Called from (representative examples):
  - [getSchemaData](getSchemaData.md)

## Notes and Other Information
- The function queries the `pg_foreign_server` catalog table with formatted options using `pg_options_to_table`
- Access control information uses `acldefault('S', srvowner)` to get default server privileges
- Each foreign server automatically gets `DUMP_COMPONENT_USERMAP` since servers can have user mappings
- ACL components are conditionally added based on whether ACL information exists
- The `srvfdw` field links the server to its associated foreign data wrapper via OID
- Server type and version are optional fields that may be null
- The returned `ForeignServerInfo` array must be freed by the caller

## Simplified Source

```c
ForeignServerInfo *
getForeignServers(Archive *fout, int *numForeignServers)
{
    PGresult *res;
    int ntups, i;
    PQExpBuffer query;
    ForeignServerInfo *srvinfo;

    query = createPQExpBuffer();

    // Query all foreign servers from system catalog
    appendPQExpBufferStr(query, "SELECT tableoid, oid, srvname, "
                                "srvowner, "
                                "srvfdw, srvtype, srvversion, srvacl, "
                                "acldefault('S', srvowner) AS acldefault, "
                                "array_to_string(ARRAY("
                                "SELECT quote_ident(option_name) || ' ' || "
                                "quote_literal(option_value) "
                                "FROM pg_options_to_table(srvoptions) "
                                "ORDER BY option_name"
                                "), E',\\n    ') AS srvoptions "
                                "FROM pg_foreign_server");

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    ntups = PQntuples(res);
    *numForeignServers = ntups;

    // Allocate array for server info
    srvinfo = (ForeignServerInfo *) pg_malloc(ntups * sizeof(ForeignServerInfo));

    // Get column indices
    int i_tableoid = PQfnumber(res, "tableoid");
    int i_oid = PQfnumber(res, "oid");
    int i_srvname = PQfnumber(res, "srvname");
    int i_srvowner = PQfnumber(res, "srvowner");
    int i_srvfdw = PQfnumber(res, "srvfdw");
    int i_srvtype = PQfnumber(res, "srvtype");
    int i_srvversion = PQfnumber(res, "srvversion");
    int i_srvacl = PQfnumber(res, "srvacl");
    int i_acldefault = PQfnumber(res, "acldefault");
    int i_srvoptions = PQfnumber(res, "srvoptions");

    // Process each foreign server
    for (i = 0; i < ntups; i++) {
        // Set object type and catalog info
        srvinfo[i].dobj.objType = DO_FOREIGN_SERVER;
        srvinfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
        srvinfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
        AssignDumpId(&srvinfo[i].dobj);

        // Set server name and namespace (always NULL for servers)
        srvinfo[i].dobj.name = pg_strdup(PQgetvalue(res, i, i_srvname));
        srvinfo[i].dobj.namespace = NULL;

        // Set ACL information
        srvinfo[i].dacl.acl = pg_strdup(PQgetvalue(res, i, i_srvacl));
        srvinfo[i].dacl.acldefault = pg_strdup(PQgetvalue(res, i, i_acldefault));
        srvinfo[i].dacl.privtype = 0;
        srvinfo[i].dacl.initprivs = NULL;

        // Set server properties
        srvinfo[i].rolname = getRoleName(PQgetvalue(res, i, i_srvowner));
        srvinfo[i].srvfdw = atooid(PQgetvalue(res, i, i_srvfdw));
        srvinfo[i].srvtype = pg_strdup(PQgetvalue(res, i, i_srvtype));
        srvinfo[i].srvversion = pg_strdup(PQgetvalue(res, i, i_srvversion));
        srvinfo[i].srvoptions = pg_strdup(PQgetvalue(res, i, i_srvoptions));

        // Determine if server should be dumped
        selectDumpableObject(&(srvinfo[i].dobj), fout);

        // Servers have user mappings by default
        srvinfo[i].dobj.components |= DUMP_COMPONENT_USERMAP;

        // Mark whether server has an ACL
        if (!PQgetisnull(res, i, i_srvacl))
            srvinfo[i].dobj.components |= DUMP_COMPONENT_ACL;
    }

    // Cleanup and return
    PQclear(res);
    destroyPQExpBuffer(query);
    return srvinfo;
}
```