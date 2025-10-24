# getTransforms

## Location
[src/bin/pg_dump/pg_dump.c:8714-8804](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L8714-L8804)

## Overview
Retrieves basic information about every transform in the PostgreSQL system for use by pg_dump, handling version compatibility for features introduced in PostgreSQL 9.5.

## Definition

```c
TransformInfo *
getTransforms(Archive *fout, int *numTransforms)
```
## Detailed Description
The  function queries the  system catalog to retrieve information about all transform definitions in the database. Transforms define how to convert data types to and from procedural languages (introduced in PostgreSQL 9.5). The function includes version checking to ensure compatibility, returning NULL for PostgreSQL versions prior to 9.5.

For each transform found, it creates a  structure containing the type OID, language OID, and function OIDs for both directions of conversion (fromsql and tosql). The function constructs descriptive names by concatenating the type name and language name for sorting purposes.

## Parameters / Member Variables
- `*fout`: Archive pointer for the pg_dump operation, used for executing SQL queries and version checking
- `*numTransforms`: Output parameter that receives the number of transforms found
## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - TransformInfo
  - [pg_malloc](../p/pg_malloc.md)
  - atooid
  - [AssignDumpId](../A/AssignDumpId.md)
  - [findTypeByOid](../f/findTypeByOid.md)
  - [get_language_name](get_language_name.md)
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - [selectDumpableObject](../s/selectDumpableObject.md)
- Called from (representative examples):
  - [getSchemaData](getSchemaData.md)

## Notes and Other Information
- Only available in PostgreSQL 9.5 and later; returns NULL for earlier versions
- Queries both fromsql and tosql function OIDs, casting them explicitly to oid type
- Transform names are constructed by concatenating type and language names for sorting
- Results are ordered by type OID, then language OID (ORDER BY 3,4)
- If type or language information cannot be found, the transform name remains empty
- Each transform's dumpability is determined by selectDumpableObject()
- Memory is properly managed by freeing the language name after use

## Simplified Source

```c
TransformInfo *
getTransforms(Archive *fout, int *numTransforms)
{
    PGresult *res;
    int ntups, i;
    TransformInfo *transforminfo;

    // Transforms didn't exist pre-9.5
    if (fout->remoteVersion < 90500) {
        *numTransforms = 0;
        return NULL;
    }

    // Query pg_transform catalog for all transforms
    query = createPQExpBuffer();
    appendPQExpBufferStr(query, "SELECT tableoid, oid, "
                                "trftype, trflang, trffromsql::oid, trftosql::oid "
                                "FROM pg_transform "
                                "ORDER BY 3,4");

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    ntups = PQntuples(res);
    *numTransforms = ntups;

    // Allocate array for transform info
    transforminfo = (TransformInfo *) pg_malloc(ntups * sizeof(TransformInfo));

    // Get column indices for result fields
    i_tableoid = PQfnumber(res, "tableoid");
    i_oid = PQfnumber(res, "oid");
    i_trftype = PQfnumber(res, "trftype");
    i_trflang = PQfnumber(res, "trflang");
    i_trffromsql = PQfnumber(res, "trffromsql");
    i_trftosql = PQfnumber(res, "trftosql");

    // Process each transform
    for (i = 0; i < ntups; i++) {
        // Set object type and catalog info
        transforminfo[i].dobj.objType = DO_TRANSFORM;
        transforminfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
        transforminfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
        AssignDumpId(&transforminfo[i].dobj);

        // Store transform properties
        transforminfo[i].trftype = atooid(PQgetvalue(res, i, i_trftype));
        transforminfo[i].trflang = atooid(PQgetvalue(res, i, i_trflang));
        transforminfo[i].trffromsql = atooid(PQgetvalue(res, i, i_trffromsql));
        transforminfo[i].trftosql = atooid(PQgetvalue(res, i, i_trftosql));

        // Create descriptive name for sorting: "typename languagename"
        typeInfo = findTypeByOid(transforminfo[i].trftype);
        lanname = get_language_name(fout, transforminfo[i].trflang);
        if (typeInfo && lanname) {
            appendPQExpBuffer(&namebuf, "%s %s", typeInfo->dobj.name, lanname);
        }
        transforminfo[i].dobj.name = namebuf.data;

        // Determine if transform should be dumped
        selectDumpableObject(&(transforminfo[i].dobj), fout);
    }

    // Cleanup and return
    PQclear(res);
    destroyPQExpBuffer(query);
    return transforminfo;
}
```