# getPublicationNamespaces

## Location
[src/bin/pg_dump/pg_dump.c:4435-4521](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L4435-L4521)

## Overview
Retrieves information about publication membership for dumpable schemas, creating objects that represent the relationship between publications and namespaces in PostgreSQL.

## Definition

```c
void
getPublicationNamespaces(Archive *fout)
```
## Detailed Description
This function queries the  system catalog to collect information about which schemas are included in publications. It creates  objects for each publication-namespace relationship that should be dumped. The function is part of pg_dump's schema discovery phase and only operates on PostgreSQL version 15.0 and later, as publication namespaces were introduced in that version.

The function filters results based on dump options and only processes relationships where both the publication and namespace are of interest to the dump operation. Each qualifying relationship results in a  dumpable object.

## Parameters / Member Variables
- `*fout`: Archive structure containing dump configuration and state information
## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md) - executes the catalog query
  - [findPublicationByOid](../f/findPublicationByOid.md) - looks up publication info by OID
  - [findNamespaceByOid](../f/findNamespaceByOid.md) - looks up namespace info by OID
  - [AssignDumpId](../A/AssignDumpId.md) - assigns unique dump ID to the object
  - [selectDumpablePublicationObject](../s/selectDumpablePublicationObject.md) - determines if object should be dumped
  - [pg_malloc](../p/pg_malloc.md) - allocates memory for publication schema info array
  - atooid - converts string to OID
- Called from (representative examples):
  - [getSchemaData](getSchemaData.md) - part of the schema discovery process

## Notes and Other Information
- Only active when  option is not set and PostgreSQL version >= 15.0
- Creates  type dumpable objects
- Skips relationships where either the publication or namespace is not being dumped
- Memory allocation may be more than needed as it allocates for all tuples before filtering

## Simplified Source

```c
void
getPublicationNamespaces(Archive *fout)
{
    PQExpBuffer query;
    PGresult *res;
    PublicationSchemaInfo *pubsinfo;
    DumpOptions *dopt = fout->dopt;
    int i_tableoid, i_oid, i_pnpubid, i_pnnspid;
    int i, j, ntups;

    // Skip if publications disabled or version < 15.0 (when feature was added)
    if (dopt->no_publications || fout->remoteVersion < 150000)
        return;

    query = createPQExpBuffer();

    // Query all publication-namespace relationships
    appendPQExpBufferStr(query,
                         "SELECT tableoid, oid, pnpubid, pnnspid "
                         "FROM pg_catalog.pg_publication_namespace");
    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);

    ntups = PQntuples(res);

    // Get column indices
    i_tableoid = PQfnumber(res, "tableoid");
    i_oid = PQfnumber(res, "oid");
    i_pnpubid = PQfnumber(res, "pnpubid");
    i_pnnspid = PQfnumber(res, "pnnspid");

    // Allocate array for publication schema info
    pubsinfo = pg_malloc(ntups * sizeof(PublicationSchemaInfo));
    j = 0;

    // Process each publication-namespace relationship
    for (i = 0; i < ntups; i++) {
        Oid pnpubid = atooid(PQgetvalue(res, i, i_pnpubid));
        Oid pnnspid = atooid(PQgetvalue(res, i, i_pnnspid));
        PublicationInfo *pubinfo;
        NamespaceInfo *nspinfo;

        // Find publication and namespace objects
        pubinfo = findPublicationByOid(pnpubid);
        if (pubinfo == NULL) continue;

        nspinfo = findNamespaceByOid(pnnspid);
        if (nspinfo == NULL) continue;

        // Skip if namespace is excluded from dump
        if (nspinfo->dobj.dump == DUMP_COMPONENT_NONE)
            continue;

        // Create dumpable object for publication-namespace relationship
        pubsinfo[j].dobj.objType = DO_PUBLICATION_TABLE_IN_SCHEMA;
        pubsinfo[j].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
        pubsinfo[j].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
        AssignDumpId(&pubsinfo[j].dobj);
        pubsinfo[j].dobj.namespace = nspinfo->dobj.namespace;
        pubsinfo[j].dobj.name = nspinfo->dobj.name;
        pubsinfo[j].publication = pubinfo;
        pubsinfo[j].pubschema = nspinfo;

        // Determine if this object should be dumped
        selectDumpablePublicationObject(&(pubsinfo[j].dobj), fout);

        j++;
    }

    PQclear(res);
    destroyPQExpBuffer(query);
}
```