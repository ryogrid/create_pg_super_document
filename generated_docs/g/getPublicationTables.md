# getPublicationTables

## Location
[src/bin/pg_dump/pg_dump.c:4522-4653](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L4522-L4653)

## Overview
Retrieves information about publication membership for dumpable tables, creating objects that represent the relationship between publications and specific tables in PostgreSQL.

## Definition

```c
void
getPublicationTables(Archive *fout, TableInfo tblinfo[], int numTables)
```
## Detailed Description
This function queries the  system catalog to collect information about which tables are included in publications. It creates  objects for each publication-table relationship that should be dumped. The function handles version-specific features, supporting row filters (prrelqual) and column lists (prattrs) for PostgreSQL 15.0 and later, while maintaining compatibility with earlier versions (10.0+).

The function filters results based on dump options and only processes relationships where both the publication and table are of interest to the dump operation. Each qualifying relationship results in a  dumpable object.

## Parameters / Member Variables
- `*fout`: Archive structure containing dump configuration and state information
- `tblinfo[]`: Array of table information structures
- `numTables`: Number of tables in the tblinfo array
## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md) - executes the catalog query
  - [findPublicationByOid](../f/findPublicationByOid.md) - looks up publication info by OID
  - [findTableByOid](../f/findTableByOid.md) - looks up table info by OID
  - [AssignDumpId](../A/AssignDumpId.md) - assigns unique dump ID to the object
  - [selectDumpablePublicationObject](../s/selectDumpablePublicationObject.md) - determines if object should be dumped
  - [pg_malloc](../p/pg_malloc.md) - allocates memory for publication relation info array
  - atooid - converts string to OID
  - [parsePGArray](../p/parsePGArray.md) - parses PostgreSQL array format for column lists
  - [fmtId](../f/fmtId.md) - formats identifiers safely
  - [pg_strdup](../p/pg_strdup.md) - duplicates strings safely
- Called from (representative examples):
  - [getSchemaData](getSchemaData.md) - part of the schema discovery process

## Notes and Other Information
- Only active when  option is not set and PostgreSQL version >= 10.0
- Creates  type dumpable objects
- Supports row filters and column lists for PostgreSQL 15.0+
- Skips relationships where either the publication or table is not being dumped
- Only processes tables whose definitions are being dumped (DUMP_COMPONENT_DEFINITION)
- Memory allocation may be more than needed as it allocates for all tuples before filtering
- Handles NULL values for row filters and column attributes appropriately

## Simplified Source

```c
void getPublicationTables(Archive *fout, TableInfo tblinfo[], int numTables) {
    PQExpBuffer query;
    PGresult *res;
    PublicationRelInfo *pubrinfo;

    // Skip if publications disabled or old version
    if (fout->dopt->no_publications || fout->remoteVersion < 100000)
        return;

    query = createPQExpBuffer();

    // Build query for publication relationships (version-dependent)
    if (fout->remoteVersion >= 150000) {
        // PostgreSQL 15+: Include row filters and column lists
        appendPQExpBufferStr(query,
            "SELECT tableoid, oid, prpubid, prrelid, "
            "pg_catalog.pg_get_expr(prqual, prrelid) AS prrelqual, "
            "(CASE WHEN pr.prattrs IS NOT NULL THEN "
            "  (SELECT array_agg(attname) FROM ...) "
            "ELSE NULL END) prattrs "
            "FROM pg_catalog.pg_publication_rel pr");
    } else {
        // Older versions: Basic publication relationships only
        appendPQExpBufferStr(query,
            "SELECT tableoid, oid, prpubid, prrelid, "
            "NULL AS prrelqual, NULL AS prattrs "
            "FROM pg_catalog.pg_publication_rel");
    }

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    int ntups = PQntuples(res);

    // Get column indices
    int i_tableoid = PQfnumber(res, "tableoid");
    int i_oid = PQfnumber(res, "oid");
    int i_prpubid = PQfnumber(res, "prpubid");
    int i_prrelid = PQfnumber(res, "prrelid");
    int i_prrelqual = PQfnumber(res, "prrelqual");
    int i_prattrs = PQfnumber(res, "prattrs");

    // Allocate storage for publication relations
    pubrinfo = pg_malloc(ntups * sizeof(PublicationRelInfo));
    int j = 0;

    // Process each publication-table relationship
    for (int i = 0; i < ntups; i++) {
        Oid prpubid = atooid(PQgetvalue(res, i, i_prpubid));
        Oid prrelid = atooid(PQgetvalue(res, i, i_prrelid));

        // Find corresponding publication and table objects
        PublicationInfo *pubinfo = findPublicationByOid(prpubid);
        TableInfo *tbinfo = findTableByOid(prrelid);

        // Skip if publication or table not found or not being dumped
        if (pubinfo == NULL || tbinfo == NULL)
            continue;
        if (!(tbinfo->dobj.dump & DUMP_COMPONENT_DEFINITION))
            continue;

        // Create publication relation object
        pubrinfo[j].dobj.objType = DO_PUBLICATION_REL;
        pubrinfo[j].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
        pubrinfo[j].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
        AssignDumpId(&pubrinfo[j].dobj);

        pubrinfo[j].dobj.namespace = tbinfo->dobj.namespace;
        pubrinfo[j].dobj.name = tbinfo->dobj.name;
        pubrinfo[j].publication = pubinfo;
        pubrinfo[j].pubtable = tbinfo;

        // Store row filter if present
        if (PQgetisnull(res, i, i_prrelqual))
            pubrinfo[j].pubrelqual = NULL;
        else
            pubrinfo[j].pubrelqual = pg_strdup(PQgetvalue(res, i, i_prrelqual));

        // Parse and store column list if present
        if (!PQgetisnull(res, i, i_prattrs)) {
            char **attnames;
            int nattnames;
            PQExpBuffer attribs;

            // Parse column array and format as comma-separated list
            parsePGArray(PQgetvalue(res, i, i_prattrs), &attnames, &nattnames);
            attribs = createPQExpBuffer();
            for (int k = 0; k < nattnames; k++) {
                if (k > 0)
                    appendPQExpBufferStr(attribs, ", ");
                appendPQExpBufferStr(attribs, fmtId(attnames[k]));
            }
            pubrinfo[j].pubrattrs = attribs->data;
        } else {
            pubrinfo[j].pubrattrs = NULL;
        }

        // Determine if this object should be dumped
        selectDumpablePublicationObject(&(pubrinfo[j].dobj), fout);
        j++;
    }

    PQclear(res);
    destroyPQExpBuffer(query);
}
```