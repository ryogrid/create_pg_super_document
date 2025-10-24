# getTSConfigurations

## Location
[src/bin/pg_dump/pg_dump.c:9597-9661](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L9597-L9661)

## Overview
This function reads all text search configurations from the PostgreSQL system catalogs and returns them in a TSConfigInfo structure array for use by pg_dump.

## Definition
TSConfigInfo *getTSConfigurations(Archive *fout, int *numTSConfigs)

## Detailed Description
The getTSConfigurations function is part of the pg_dump utility that extracts metadata about text search configurations from the pg_ts_config system catalog. Text search configurations are the top-level objects that define how text search operates by associating parsers with dictionaries for different token types.

The function constructs a SQL query to select all relevant fields from pg_ts_config, executes the query, and processes each result row to populate a TSConfigInfo structure. Each configuration object contains information about its name, namespace, owner, and the parser it uses. Configurations coordinate the text search process by defining which parser to use and how different token types should be processed by various dictionaries.

## Parameters / Member Variables
- : Pointer to Archive structure representing the output destination for the dump
- : Pointer to integer that will be set to the total number of configurations retrieved

## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md): Executes the SQL query against the database
  - [pg_malloc](../p/pg_malloc.md): Allocates memory for the TSConfigInfo array
  - atooid: Converts string OID values to Oid type
  - [AssignDumpId](../A/AssignDumpId.md): Assigns unique dump ID to each configuration object
  - [findNamespace](../f/findNamespace.md): Looks up namespace information for the configuration
  - [getRoleName](getRoleName.md): Retrieves role name for the configuration owner
  - [selectDumpableObject](../s/selectDumpableObject.md): Determines if the configuration should be included in dump
- Called from (representative examples):
  - [getSchemaData](getSchemaData.md): Main schema data collection function

## Notes and Other Information
- The function queries pg_ts_config system catalog to retrieve configuration metadata including name, namespace, owner, and parser reference
- Configurations are the highest level objects in the text search hierarchy, coordinating parsers and dictionaries
- Each configuration references a specific parser (cfgparser) that defines how text is tokenized
- The actual mapping of token types to dictionaries is stored separately in pg_ts_config_map
- Memory is allocated for the entire array of configurations at once using pg_malloc
- The TSConfigInfo structure contains both dump object metadata and configuration-specific parser reference

## Simplified Source

```c
TSConfigInfo *
getTSConfigurations(Archive *fout, int *numTSConfigs)
{
    PGresult *res;
    int ntups, i;
    PQExpBuffer query;
    TSConfigInfo *cfginfo;

    query = createPQExpBuffer();

    // Query all text search configurations from system catalog
    appendPQExpBufferStr(query, "SELECT tableoid, oid, cfgname, "
                                "cfgnamespace, cfgowner, cfgparser "
                                "FROM pg_ts_config");

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    ntups = PQntuples(res);
    *numTSConfigs = ntups;

    // Allocate array for configuration info
    cfginfo = (TSConfigInfo *) pg_malloc(ntups * sizeof(TSConfigInfo));

    // Get column indices
    int i_tableoid = PQfnumber(res, "tableoid");
    int i_oid = PQfnumber(res, "oid");
    int i_cfgname = PQfnumber(res, "cfgname");
    int i_cfgnamespace = PQfnumber(res, "cfgnamespace");
    int i_cfgowner = PQfnumber(res, "cfgowner");
    int i_cfgparser = PQfnumber(res, "cfgparser");

    // Process each configuration
    for (i = 0; i < ntups; i++) {
        // Set object type and catalog info
        cfginfo[i].dobj.objType = DO_TSCONFIG;
        cfginfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
        cfginfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
        AssignDumpId(&cfginfo[i].dobj);

        // Set configuration name and namespace
        cfginfo[i].dobj.name = pg_strdup(PQgetvalue(res, i, i_cfgname));
        cfginfo[i].dobj.namespace = findNamespace(atooid(PQgetvalue(res, i, i_cfgnamespace)));

        // Set owner and parser reference
        cfginfo[i].rolname = getRoleName(PQgetvalue(res, i, i_cfgowner));
        cfginfo[i].cfgparser = atooid(PQgetvalue(res, i, i_cfgparser));

        // Determine if configuration should be dumped
        selectDumpableObject(&(cfginfo[i].dobj), fout);
    }

    // Cleanup and return
    PQclear(res);
    destroyPQExpBuffer(query);
    return cfginfo;
}
```