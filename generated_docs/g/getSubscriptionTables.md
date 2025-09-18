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
  - pg_malloc
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