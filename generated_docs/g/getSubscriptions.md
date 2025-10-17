This documentation is for getSubscriptions function in PostgreSQL pg_dump utility.

# getSubscriptions

## Location
[src/bin/pg_dump/pg_dump.c:4798-4997](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L4798-L4997)

## Overview
Retrieves information about logical replication subscriptions from the PostgreSQL database and creates SubscriptionInfo objects for dumping subscription definitions.

## Definition
```c
void getSubscriptions(Archive *fout)
```

## Detailed Description
This function queries the `pg_subscription` system catalog to gather information about all subscriptions in the current database. It handles version compatibility by conditionally querying fields that were introduced in different PostgreSQL versions (14.0, 15.0, 16.0, 17.0). The function performs security checks to ensure only superusers can dump subscriptions, as subscription information is sensitive. For each subscription found, it creates a SubscriptionInfo structure containing all relevant subscription properties including connection info, publications, replication settings, and binary upgrade specific information like replication origin LSNs.

## Parameters / Member Variables
- `fout`: Archive handle containing database connection and dump options

## Dependencies
- Functions called/Symbols referenced:
  - DumpOptions
  - SubscriptionInfo
  - [is_superuser](../i/is_superuser.md)
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - PGRES_TUPLES_OK
  - pg_log_warning
  - LOGICALREP_TWOPHASE_STATE_DISABLED
  - LOGICALREP_ORIGIN_ANY
  - [pg_malloc](../p/pg_malloc.md)
  - DO_SUBSCRIPTION
  - atooid
  - [AssignDumpId](../A/AssignDumpId.md)
  - [getRoleName](getRoleName.md)
  - [PQgetisnull](../P/PQgetisnull.md)
  - [selectDumpableObject](../s/selectDumpableObject.md)
- Called from (representative examples):
  - [getSchemaData](getSchemaData.md)
  - [SubRelInfo](../S/SubRelInfo.md)

## Notes and Other Information
- Only works with PostgreSQL 10.0 and later (when subscriptions were introduced)
- Requires superuser privileges to access subscription information
- Handles version-specific features like binary format (14.0+), streaming (14.0+), two-phase commit (15.0+), password requirements (16.0+), and failover support (17.0+)
- In binary upgrade mode, also captures replication origin remote LSN for preserving replication state
- Skips subskiplsn field as it becomes irrelevant after restore

## Simplified Source

```c
void getSubscriptions(Archive *fout) {
    DumpOptions *dopt = fout->dopt;
    PQExpBuffer query;
    PGresult *res;
    SubscriptionInfo *subinfo;

    // Skip if subscriptions disabled or unsupported version
    if (dopt->no_subscriptions || fout->remoteVersion < 100000)
        return;

    // Check superuser privileges (required for subscription access)
    if (!is_superuser(fout)) {
        res = ExecuteSqlQuery(fout,
                              "SELECT count(*) FROM pg_subscription "
                              "WHERE subdbid = (SELECT oid FROM pg_database "
                              "                 WHERE datname = current_database())",
                              PGRES_TUPLES_OK);
        int n = atoi(PQgetvalue(res, 0, 0));
        if (n > 0)
            pg_log_warning("subscriptions not dumped because current user is not a superuser");
        PQclear(res);
        return;
    }

    query = createPQExpBuffer();

    // Build version-dependent query for subscription information
    appendPQExpBufferStr(query,
                         "SELECT s.tableoid, s.oid, s.subname,\n"
                         " s.subowner,\n"
                         " s.subconninfo, s.subslotname, s.subsynccommit,\n"
                         " s.subpublications,\n");

    // Add version-specific fields with fallback defaults
    if (fout->remoteVersion >= 140000)
        appendPQExpBufferStr(query, " s.subbinary,\n");
    else
        appendPQExpBufferStr(query, " false AS subbinary,\n");

    if (fout->remoteVersion >= 140000)
        appendPQExpBufferStr(query, " s.substream,\n");
    else
        appendPQExpBufferStr(query, " 'f' AS substream,\n");

    if (fout->remoteVersion >= 150000)
        appendPQExpBufferStr(query,
                             " s.subtwophasestate,\n"
                             " s.subdisableonerr,\n");
    else
        appendPQExpBuffer(query,
                          " '%c' AS subtwophasestate,\n"
                          " false AS subdisableonerr,\n",
                          LOGICALREP_TWOPHASE_STATE_DISABLED);

    if (fout->remoteVersion >= 160000)
        appendPQExpBufferStr(query,
                             " s.subpasswordrequired,\n"
                             " s.subrunasowner,\n"
                             " s.suborigin,\n");
    else
        appendPQExpBuffer(query,
                          " 't' AS subpasswordrequired,\n"
                          " 't' AS subrunasowner,\n"
                          " '%s' AS suborigin,\n",
                          LOGICALREP_ORIGIN_ANY);

    // Handle binary upgrade specific fields
    if (dopt->binary_upgrade && fout->remoteVersion >= 170000)
        appendPQExpBufferStr(query, " o.remote_lsn AS suboriginremotelsn,\n"
                             " s.subenabled,\n");
    else
        appendPQExpBufferStr(query, " NULL AS suboriginremotelsn,\n"
                             " false AS subenabled,\n");

    if (fout->remoteVersion >= 170000)
        appendPQExpBufferStr(query, " s.subfailover\n");
    else
        appendPQExpBuffer(query, " false AS subfailover\n");

    appendPQExpBufferStr(query, "FROM pg_subscription s\n");

    // Join with replication origin for binary upgrade
    if (dopt->binary_upgrade && fout->remoteVersion >= 170000)
        appendPQExpBufferStr(query,
                             "LEFT JOIN pg_catalog.pg_replication_origin_status o \n"
                             "    ON o.external_id = 'pg_' || s.oid::text \n");

    // Filter to current database only
    appendPQExpBufferStr(query,
                         "WHERE s.subdbid = (SELECT oid FROM pg_database\n"
                         "                   WHERE datname = current_database())");

    res = ExecuteSqlQuery(fout, query->data, PGRES_TUPLES_OK);
    int ntups = PQntuples(res);

    // Get column indices for all subscription fields
    int i_tableoid = PQfnumber(res, "tableoid");
    int i_oid = PQfnumber(res, "oid");
    int i_subname = PQfnumber(res, "subname");
    int i_subowner = PQfnumber(res, "subowner");
    // ... (other field indices)

    // Allocate storage for subscription info
    subinfo = pg_malloc(ntups * sizeof(SubscriptionInfo));

    // Process each subscription
    for (int i = 0; i < ntups; i++) {
        subinfo[i].dobj.objType = DO_SUBSCRIPTION;
        subinfo[i].dobj.catId.tableoid = atooid(PQgetvalue(res, i, i_tableoid));
        subinfo[i].dobj.catId.oid = atooid(PQgetvalue(res, i, i_oid));
        AssignDumpId(&subinfo[i].dobj);
        subinfo[i].dobj.name = pg_strdup(PQgetvalue(res, i, i_subname));
        subinfo[i].rolname = getRoleName(PQgetvalue(res, i, i_subowner));

        // Copy all subscription properties
        subinfo[i].subbinary = pg_strdup(PQgetvalue(res, i, i_subbinary));
        subinfo[i].substream = pg_strdup(PQgetvalue(res, i, i_substream));
        subinfo[i].subtwophasestate = pg_strdup(PQgetvalue(res, i, i_subtwophasestate));
        subinfo[i].subdisableonerr = pg_strdup(PQgetvalue(res, i, i_subdisableonerr));
        subinfo[i].subpasswordrequired = pg_strdup(PQgetvalue(res, i, i_subpasswordrequired));
        subinfo[i].subrunasowner = pg_strdup(PQgetvalue(res, i, i_subrunasowner));
        subinfo[i].subconninfo = pg_strdup(PQgetvalue(res, i, i_subconninfo));
        subinfo[i].subsynccommit = pg_strdup(PQgetvalue(res, i, i_subsynccommit));
        subinfo[i].subpublications = pg_strdup(PQgetvalue(res, i, i_subpublications));
        subinfo[i].suborigin = pg_strdup(PQgetvalue(res, i, i_suborigin));

        // Handle nullable fields
        if (PQgetisnull(res, i, i_subslotname))
            subinfo[i].subslotname = NULL;
        else
            subinfo[i].subslotname = pg_strdup(PQgetvalue(res, i, i_subslotname));

        if (PQgetisnull(res, i, i_suboriginremotelsn))
            subinfo[i].suboriginremotelsn = NULL;
        else
            subinfo[i].suboriginremotelsn = pg_strdup(PQgetvalue(res, i, i_suboriginremotelsn));

        subinfo[i].subenabled = pg_strdup(PQgetvalue(res, i, i_subenabled));
        subinfo[i].subfailover = pg_strdup(PQgetvalue(res, i, i_subfailover));

        // Determine if this subscription should be dumped
        selectDumpableObject(&(subinfo[i].dobj), fout);
    }

    PQclear(res);
    destroyPQExpBuffer(query);
}
```