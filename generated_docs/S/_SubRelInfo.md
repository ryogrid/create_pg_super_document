# _SubRelInfo

## Location
src/bin/pg_dump/pg_dump.h: 701 - 707

## Overview
The `_SubRelInfo` struct represents a subscription relation, used by pg_dump to store information about individual tables that are part of a logical replication subscription.

## Definition
```c
typedef struct _SubRelInfo
{
    DumpableObject dobj;
    SubscriptionInfo *subinfo;
    TableInfo  *tblinfo;
    char        srsubstate;
    char       *srsublsn;
} SubRelInfo;
```

## Detailed Description
This structure is part of PostgreSQL's pg_dump utility and represents the relationship between a subscription and a specific table that is being replicated. It tracks the synchronization state and LSN (Log Sequence Number) for each table within a subscription, which is crucial for maintaining consistency during logical replication. The structure includes important metadata about the replication status of individual tables, allowing for proper restoration of subscription states during database dumps and restores.

## Parameters / Member Variables
- `dobj`: Base DumpableObject structure containing common metadata for dump objects
- `subinfo`: Pointer to the SubscriptionInfo structure representing the subscription this table belongs to
- `tblinfo`: Pointer to the TableInfo structure representing the table that is part of the subscription
- `srsubstate`: Single character representing the subscription relation state (e.g., 'i' for initialize, 's' for synchronized, 'r' for ready)
- `srsublsn`: String containing the LSN (Log Sequence Number) up to which this table has been synchronized

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject
  - SubscriptionInfo  
  - TableInfo
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This structure is defined in src/bin/pg_dump/pg_dump.h at lines 701-707
- It's used specifically by the pg_dump utility for logical replication subscription handling
- The struct maintains the many-to-many relationship between subscriptions and tables
- Important note from source: Currently subscription tables are added after enabling the subscription in binary-upgrade mode
- The ordering of operations (adding tables vs enabling subscription) may need consideration for future non-binary-upgrade mode support
- The LSN tracking is essential for maintaining replication consistency across dump/restore operations
- States typically follow the progression: initialize → synchronized → ready