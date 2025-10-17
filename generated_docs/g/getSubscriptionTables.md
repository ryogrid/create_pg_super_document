Documentation for getSubscriptionTables function.

# getSubscriptionTables

## Location
[src/bin/pg_dump/pg_dump.c:4998-5083](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L4998-L5083)

## Overview
Retrieves subscription table membership information from pg_subscription_rel system catalog, used exclusively in binary-upgrade mode for PostgreSQL 17 and later versions.

## Definition
```c
void getSubscriptionTables(Archive *fout)
```

## Detailed Description
This function queries the `pg_subscription_rel` system catalog to get information about which tables belong to which subscriptions and their replication states. It is specifically designed for binary upgrade scenarios where the exact subscription-table relationships and their synchronization states must be preserved. The function creates SubRelInfo objects for each subscription-table relationship, including the subscription state (ready, syncing, etc.) and the subscription LSN position. It validates that both the subscription and table exist during the process and creates dumpable objects that can be restored to maintain subscription table memberships across upgrades.

## Parameters / Member Variables
- `fout`: Archive handle containing database connection and dump options, must have binary_upgrade enabled

## Dependencies
- Functions called/Symbols referenced:
  - DumpOptions
  - [SubRelInfo](../S/SubRelInfo.md)
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - PGRES_TUPLES_OK
  - [pg_malloc](../p/pg_malloc.md)
  - atooid
  - [findSubscriptionByOid](../f/findSubscriptionByOid.md)
  - [pg_fatal](../p/pg_fatal.md)
  - [findTableByOid](../f/findTableByOid.md)
  - DO_SUBSCRIPTION_REL
  - [AssignDumpId](../A/AssignDumpId.md)
  - [pg_strdup](../p/pg_strdup.md)
  - [PQgetisnull](../P/PQgetisnull.md)
  - [selectDumpableObject](../s/selectDumpableObject.md)
- Called from (representative examples):
  - Binary upgrade restoration process

## Notes and Other Information
- Only active when no_subscriptions is false, binary_upgrade is true, and PostgreSQL version >= 17.0
- Processes pg_subscription_rel entries ordered by subscription ID for efficient processing
- Maintains subscription state information (srsubstate) and LSN positions (srsublsn) for each table
- Critical for preserving exact replication state during major version upgrades
- Performs sanity checks to ensure referenced subscriptions and tables exist in the dump

## Simplified Source

```c
void getSubscriptionTables(Archive *fout) {
    DumpOptions *dopt = fout->dopt;
    SubscriptionInfo *subinfo = NULL;
    SubRelInfo *subrinfo;
    PGresult *res;

    // Only for binary upgrade mode with PostgreSQL 17+
    if (dopt->no_subscriptions || !dopt->binary_upgrade || fout->remoteVersion < 170000)
        return;

    // Query subscription-relation mappings
    res = ExecuteSqlQuery(fout,
                          "SELECT srsubid, srrelid, srsubstate, srsublsn "
                          "FROM pg_catalog.pg_subscription_rel "
                          "ORDER BY srsubid",
                          PGRES_TUPLES_OK);

    int ntups = PQntuples(res);
    if (ntups == 0)
        goto cleanup;

    // Get column indices
    int i_srsubid = PQfnumber(res, "srsubid");
    int i_srrelid = PQfnumber(res, "srrelid");
    int i_srsubstate = PQfnumber(res, "srsubstate");
    int i_srsublsn = PQfnumber(res, "srsublsn");

    // Allocate storage for subscription relation info
    subrinfo = pg_malloc(ntups * sizeof(SubRelInfo));
    Oid last_srsubid = InvalidOid;

    // Process each subscription-table relationship
    for (int i = 0; i < ntups; i++) {
        Oid cur_srsubid = atooid(PQgetvalue(res, i, i_srsubid));
        Oid relid = atooid(PQgetvalue(res, i, i_srrelid));

        // Check if we switched to a new subscription
        if (cur_srsubid != last_srsubid) {
            subinfo = findSubscriptionByOid(cur_srsubid);
            if (subinfo == NULL)
                pg_fatal("subscription with OID %u does not exist", cur_srsubid);
            last_srsubid = cur_srsubid;
        }

        // Find the corresponding table
        TableInfo *tblinfo = findTableByOid(relid);
        if (tblinfo == NULL)
            pg_fatal("failed sanity check, table with OID %u not found", relid);

        // Create subscription relation object
        subrinfo[i].dobj.objType = DO_SUBSCRIPTION_REL;
        subrinfo[i].dobj.catId.tableoid = relid;
        subrinfo[i].dobj.catId.oid = cur_srsubid;
        AssignDumpId(&subrinfo[i].dobj);
        subrinfo[i].dobj.name = pg_strdup(subinfo->dobj.name);
        subrinfo[i].tblinfo = tblinfo;
        subrinfo[i].srsubstate = PQgetvalue(res, i, i_srsubstate)[0];

        // Handle nullable LSN field
        if (PQgetisnull(res, i, i_srsublsn))
            subrinfo[i].srsublsn = NULL;
        else
            subrinfo[i].srsublsn = pg_strdup(PQgetvalue(res, i, i_srsublsn));

        subrinfo[i].subinfo = subinfo;

        // Determine if this object should be dumped
        selectDumpableObject(&(subrinfo[i].dobj), fout);
    }

cleanup:
    PQclear(res);
}
```