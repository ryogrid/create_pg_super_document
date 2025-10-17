# getPublications

## Location
[src/bin/pg_dump/pg_dump.c:4235-4338](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L4235-L4338)

## Overview
Retrieves information about all logical replication publications from the PostgreSQL system catalogs and creates PublicationInfo objects for them.

## Definition

```c
PublicationInfo *
getPublications(Archive *fout, int *numPublications)
```
## Detailed Description
The `getPublications` function queries the `pg_publication` system catalog to gather information about all publications in the database. Publications are a key component of PostgreSQL's logical replication feature, defining which tables and what types of changes (INSERT, UPDATE, DELETE, TRUNCATE) should be replicated.

The function handles different PostgreSQL versions gracefully:
- PostgreSQL 13.0+: Full support including `pubviaroot` (publish_via_partition_root)
- PostgreSQL 11.0+: Support for truncate operations
- PostgreSQL 10.0+: Basic publication support

For each publication found, it creates a PublicationInfo structure containing all the publication attributes and marks it as dumpable based on the current dump options.

## Parameters / Member Variables
- `fout`: Archive pointer containing dump options and database connection
- `numPublications`: Output parameter - pointer to int that will receive the count of publications found

## Dependencies
- Functions called/Symbols referenced:
  - `DumpOptions`, `PublicationInfo` (data structures)
  - `[createPQExpBuffer](../c/createPQExpBuffer.md)`, `appendPQExpBufferStr` (query building)
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md) (SQL execution)
  - [PQfnumber](../P/PQfnumber.md), `PQgetvalue`, `PQntuples` (result processing)
  - `[pg_malloc](../p/pg_malloc.md)`, `pg_strdup` (memory management)
  - [AssignDumpId](../A/AssignDumpId.md) (dump object ID assignment)
  - [getRoleName](getRoleName.md) (owner name resolution)
  - [selectDumpableObject](../s/selectDumpableObject.md) (dumpability determination)
  - `atooid` (OID conversion)
- Called from (representative examples):
  - [getSchemaData](getSchemaData.md) (main schema information gathering)

## Notes and Other Information
- Returns NULL and sets *numPublications to 0 if publications are disabled via `--no-publications` option
- Only available for PostgreSQL 10.0 and later (returns NULL for older versions)
- Publications are part of PostgreSQL's built-in logical replication infrastructure
- Each PublicationInfo contains boolean flags for supported DML operations (insert, update, delete, truncate)
- The `puballtables` flag indicates whether the publication includes all tables in the database
- Memory allocated for the returned array should be managed by the caller
- Part of the logical replication backup and restore functionality in pg_dump

## Simplified Source

```c
PublicationInfo *
getPublications(Archive *fout, int *numPublications)
{
    DumpOptions *dopt = fout->dopt;

    // Skip if publications disabled or unsupported version
    if (dopt->no_publications || fout->remoteVersion < 100000) {
        *numPublications = 0;
        return NULL;
    }

    PQExpBuffer query = createPQExpBuffer();

    // Build version-specific query for pg_publication
    if (fout->remoteVersion >= 130000) {
        // PostgreSQL 13+: Full support including pubviaroot
        appendPQExpBufferStr(query,
                           "SELECT p.tableoid, p.oid, p.pubname, p.pubowner, "
                           "p.puballtables, p.pubinsert, p.pubupdate, p.pubdelete, "
                           "p.pubtruncate, p.pubviaroot "
                           "FROM pg_publication p");
    } else if (fout->remoteVersion >= 110000) {
        // PostgreSQL 11-12: Has truncate, no pubviaroot
        appendPQExpBufferStr(query,
                           "SELECT p.tableoid, p.oid, p.pubname, p.pubowner, "
                           "p.puballtables, p.pubinsert, p.pubupdate, p.pubdelete, "
                           "p.pubtruncate, false AS pubviaroot "
                           "FROM pg_publication p");
    } else {
        // PostgreSQL 10: Basic support, no truncate or pubviaroot
        appendPQExpBufferStr(query,
                           "SELECT p.tableoid, p.oid, p.pubname, p.pubowner, "
                           "p.puballtables, p.pubinsert, p.pubupdate, p.pubdelete, "
                           "false AS pubtruncate, false AS pubviaroot "
                           "FROM pg_publication p");
    }

    PGresult *res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    int ntups = PQntuples(res);

    // Get column indices
    int i_tableoid = PQfnumber(res, "tableoid");
    int i_oid = PQfnumber(res, "oid");
    int i_pubname = PQfnumber(res, "pubname");
    int i_pubowner = PQfnumber(res, "pubowner");
    int i_puballtables = PQfnumber(res, "puballtables");
    int i_pubinsert = PQfnumber(res, "pubinsert");
    int i_pubupdate = PQfnumber(res, "pubupdate");
    int i_pubdelete = PQfnumber(res, "pubdelete");
    int i_pubtruncate = PQfnumber(res, "pubtruncate");
    int i_pubviaroot = PQfnumber(res, "pubviaroot");

    PublicationInfo *pubinfo = pg_malloc(ntups * sizeof(PublicationInfo));

    // Process each publication
    for (int i = 0; i < ntups; i++) {
        pubinfo[i].dobj.objType = DO_PUBLICATION;
        pubinfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
        pubinfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
        pubinfo[i].dobj.name = pg_strdup(PQgetvalue(res, i, i_pubname));
        pubinfo[i].rolname = getRoleName(PQgetvalue(res, i, i_pubowner));

        // Convert boolean flags from string to bool
        pubinfo[i].puballtables = (strcmp(PQgetvalue(res, i, i_puballtables), "t") == 0);
        pubinfo[i].pubinsert = (strcmp(PQgetvalue(res, i, i_pubinsert), "t") == 0);
        pubinfo[i].pubupdate = (strcmp(PQgetvalue(res, i, i_pubupdate), "t") == 0);
        pubinfo[i].pubdelete = (strcmp(PQgetvalue(res, i, i_pubdelete), "t") == 0);
        pubinfo[i].pubtruncate = (strcmp(PQgetvalue(res, i, i_pubtruncate), "t") == 0);
        pubinfo[i].pubviaroot = (strcmp(PQgetvalue(res, i, i_pubviaroot), "t") == 0);

        AssignDumpId(&pubinfo[i].dobj);
        selectDumpableObject(&pubinfo[i].dobj, fout);
    }

    PQclear(res);
    destroyPQExpBuffer(query);

    *numPublications = ntups;
    return pubinfo;
}
```