# getTSTemplates

## Location
[src/bin/pg_dump/pg_dump.c:9532-9596](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L9532-L9596)

## Overview
This function reads all text search templates from the PostgreSQL system catalogs and returns them in a TSTemplateInfo structure array for use by pg_dump.

## Definition
TSTemplateInfo *getTSTemplates(Archive *fout, int *numTSTemplates)

## Detailed Description
The getTSTemplates function is part of the pg_dump utility that extracts metadata about text search templates from the pg_ts_template system catalog. Text search templates are used as blueprints for creating text search dictionaries, defining the initialization and lexize functions that dictionaries will use.

The function constructs a SQL query to select all relevant fields from pg_ts_template, executes the query, and processes each result row to populate a TSTemplateInfo structure. Each template object contains references to its initialization function (tmplinit) and lexize function (tmpllexize), which define the template's behavior when used to create dictionaries.

## Parameters / Member Variables
- : Pointer to Archive structure representing the output destination for the dump
- : Pointer to integer that will be set to the total number of templates retrieved

## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md): Executes the SQL query against the database
  - [pg_malloc](../p/pg_malloc.md): Allocates memory for the TSTemplateInfo array
  - atooid: Converts string OID values to Oid type
  - [AssignDumpId](../A/AssignDumpId.md): Assigns unique dump ID to each template object
  - [findNamespace](../f/findNamespace.md): Looks up namespace information for the template
  - [selectDumpableObject](../s/selectDumpableObject.md): Determines if the template should be included in dump
- Called from (representative examples):
  - [getSchemaData](getSchemaData.md): Main schema data collection function

## Notes and Other Information
- The function queries pg_ts_template system catalog to retrieve template metadata including name, namespace, and function OIDs
- Templates define the fundamental behavior for text search dictionaries through tmplinit and tmpllexize function references
- The tmplinit function handles dictionary initialization, while tmpllexize handles the actual text processing
- Memory is allocated for the entire array of templates at once using pg_malloc
- The TSTemplateInfo structure contains both dump object metadata and template-specific function references
- Templates serve as the foundation for creating custom text search dictionaries with specific linguistic processing capabilities

## Simplified Source

```c
TSTemplateInfo *
getTSTemplates(Archive *fout, int *numTSTemplates)
{
    PGresult *res;
    int ntups, i;
    PQExpBuffer query;
    TSTemplateInfo *tmplinfo;

    query = createPQExpBuffer();

    // Query all text search templates from system catalog
    appendPQExpBufferStr(query, "SELECT tableoid, oid, tmplname, "
                                "tmplnamespace, tmplinit::oid, tmpllexize::oid "
                                "FROM pg_ts_template");

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    ntups = PQntuples(res);
    *numTSTemplates = ntups;

    // Allocate array for template info
    tmplinfo = (TSTemplateInfo *) pg_malloc(ntups * sizeof(TSTemplateInfo));

    // Get column indices
    int i_tableoid = PQfnumber(res, "tableoid");
    int i_oid = PQfnumber(res, "oid");
    int i_tmplname = PQfnumber(res, "tmplname");
    int i_tmplnamespace = PQfnumber(res, "tmplnamespace");
    int i_tmplinit = PQfnumber(res, "tmplinit");
    int i_tmpllexize = PQfnumber(res, "tmpllexize");

    // Process each template
    for (i = 0; i < ntups; i++) {
        // Set object type and catalog info
        tmplinfo[i].dobj.objType = DO_TSTEMPLATE;
        tmplinfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
        tmplinfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
        AssignDumpId(&tmplinfo[i].dobj);

        // Set template name and namespace
        tmplinfo[i].dobj.name = pg_strdup(PQgetvalue(res, i, i_tmplname));
        tmplinfo[i].dobj.namespace = findNamespace(atooid(PQgetvalue(res, i, i_tmplnamespace)));

        // Store template function OIDs
        tmplinfo[i].tmplinit = atooid(PQgetvalue(res, i, i_tmplinit));
        tmplinfo[i].tmpllexize = atooid(PQgetvalue(res, i, i_tmpllexize));

        // Determine if template should be dumped
        selectDumpableObject(&(tmplinfo[i].dobj), fout);
    }

    // Cleanup and return
    PQclear(res);
    destroyPQExpBuffer(query);
    return tmplinfo;
}
```