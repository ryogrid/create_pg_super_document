# getTSDictionaries

## Location
[src/bin/pg_dump/pg_dump.c:9460-9531](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L9460-L9531)

## Overview
This function reads all text search dictionaries from the PostgreSQL system catalogs and returns them in a TSDictInfo structure array for use by pg_dump.

## Definition
TSDictInfo *getTSDictionaries(Archive *fout, int *numTSDicts)

## Detailed Description
The getTSDictionaries function is part of the pg_dump utility that extracts metadata about text search dictionaries from the pg_ts_dict system catalog. It performs a comprehensive query to retrieve all text search dictionary objects and packages them into a structured format for dumping.

The function constructs a SQL query to select all relevant fields from pg_ts_dict, executes the query, and processes each result row to populate a TSDictInfo structure. Each dictionary object contains information about its name, namespace, owner, template, and initialization options. The function handles null initialization options appropriately and assigns dump IDs for dependency tracking.

## Parameters / Member Variables
- : Pointer to Archive structure representing the output destination for the dump
- : Pointer to integer that will be set to the total number of dictionaries retrieved

## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md): Executes the SQL query against the database
  - [pg_malloc](../p/pg_malloc.md): Allocates memory for the TSDictInfo array  
  - atooid: Converts string OID values to Oid type
  - [AssignDumpId](../A/AssignDumpId.md): Assigns unique dump ID to each dictionary object
  - [findNamespace](../f/findNamespace.md): Looks up namespace information for the dictionary
  - [getRoleName](getRoleName.md): Retrieves role name for the dictionary owner
  - [PQgetisnull](../P/PQgetisnull.md): Checks if a result field is null
  - [selectDumpableObject](../s/selectDumpableObject.md): Determines if the dictionary should be included in dump
- Called from (representative examples):
  - [getSchemaData](getSchemaData.md): Main schema data collection function

## Notes and Other Information
- The function queries pg_ts_dict system catalog to retrieve dictionary metadata including name, namespace, owner, template, and initialization options
- Initialization options (dictinitoption) can be null and are handled with appropriate null checking
- Memory is allocated for the entire array of dictionaries at once using pg_malloc
- Each dictionary references a template via the dicttemplate OID field
- The TSDictInfo structure contains both dump object metadata and dictionary-specific configuration information

## Simplified Source

```c
TSDictInfo *
getTSDictionaries(Archive *fout, int *numTSDicts)
{
    PGresult *res;
    int ntups, i;
    PQExpBuffer query;
    TSDictInfo *dictinfo;

    query = createPQExpBuffer();

    // Query all text search dictionaries from system catalog
    appendPQExpBufferStr(query, "SELECT tableoid, oid, dictname, "
                                "dictnamespace, dictowner, "
                                "dicttemplate, dictinitoption "
                                "FROM pg_ts_dict");

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    ntups = PQntuples(res);
    *numTSDicts = ntups;

    // Allocate array for dictionary info
    dictinfo = (TSDictInfo *) pg_malloc(ntups * sizeof(TSDictInfo));

    // Get column indices
    int i_tableoid = PQfnumber(res, "tableoid");
    int i_oid = PQfnumber(res, "oid");
    int i_dictname = PQfnumber(res, "dictname");
    int i_dictnamespace = PQfnumber(res, "dictnamespace");
    int i_dictowner = PQfnumber(res, "dictowner");
    int i_dicttemplate = PQfnumber(res, "dicttemplate");
    int i_dictinitoption = PQfnumber(res, "dictinitoption");

    // Process each dictionary
    for (i = 0; i < ntups; i++) {
        // Set object type and catalog info
        dictinfo[i].dobj.objType = DO_TSDICT;
        dictinfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
        dictinfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
        AssignDumpId(&dictinfo[i].dobj);

        // Set dictionary name and namespace
        dictinfo[i].dobj.name = pg_strdup(PQgetvalue(res, i, i_dictname));
        dictinfo[i].dobj.namespace = findNamespace(atooid(PQgetvalue(res, i, i_dictnamespace)));

        // Set owner and template
        dictinfo[i].rolname = getRoleName(PQgetvalue(res, i, i_dictowner));
        dictinfo[i].dicttemplate = atooid(PQgetvalue(res, i, i_dicttemplate));

        // Handle optional initialization options
        if (PQgetisnull(res, i, i_dictinitoption))
            dictinfo[i].dictinitoption = NULL;
        else
            dictinfo[i].dictinitoption = pg_strdup(PQgetvalue(res, i, i_dictinitoption));

        // Determine if dictionary should be dumped
        selectDumpableObject(&(dictinfo[i].dobj), fout);
    }

    // Cleanup and return
    PQclear(res);
    destroyPQExpBuffer(query);
    return dictinfo;
}
```