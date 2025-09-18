This documentation is for getSubscriptions function in PostgreSQL pg_dump utility.

# getSubscriptions

## Location
src/bin/pg_dump/pg_dump.c: 4798 - 4997

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
  - pg_malloc
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