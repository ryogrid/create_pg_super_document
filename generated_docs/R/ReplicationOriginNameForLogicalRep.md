# ReplicationOriginNameForLogicalRep

## Location
src/backend/replication/logical/worker.c: 430 - 469

## Overview
Forms the replication origin name for logical replication subscriptions, supporting both table synchronization and general apply workers with distinct naming conventions.

## Definition
```c
void ReplicationOriginNameForLogicalRep(Oid suboid, Oid relid, char *originname, Size szoriginname)
```

## Detailed Description
This function generates standardized replication origin names used throughout PostgreSQL's logical replication system. It creates different naming patterns based on the type of worker:
- For tablesync workers: uses format "pg_{suboid}_{relid}" to create unique origins per table
- For apply workers and other non-tablesync contexts: uses format "pg_{suboid}" for subscription-level origins

The function ensures consistent origin naming across all logical replication components, enabling proper tracking of replication progress and conflict resolution.

## Parameters / Member Variables
- `suboid`: The OID of the logical replication subscription
- `relid`: The OID of the relation (table) - must be valid for tablesync workers, InvalidOid for others
- `originname`: Output buffer to store the generated origin name
- `szoriginname`: Size of the output buffer to prevent overflow

## Dependencies
- Functions called/Symbols referenced:
  - OidIsValid (macro to check if OID is valid)
  - snprintf (standard C library function for safe string formatting)
- Called from (representative examples):
  - [CreateSubscription](../C/CreateSubscription.md) (subscription creation)
  - [AlterSubscription](../A/AlterSubscription.md) (subscription modification)
  - [LogicalRepSyncTableStart](../L/LogicalRepSyncTableStart.md) (table synchronization)
  - [run_apply_worker](../r/run_apply_worker.md) (apply worker main loop)
  - [ParallelApplyWorkerMain](../P/ParallelApplyWorkerMain.md) (parallel apply worker)

## Notes and Other Information
- The function uses a simple but effective naming scheme that prevents conflicts between different types of replication workers
- Tablesync workers get unique origins per table to track individual table synchronization progress
- Apply workers use subscription-level origins for overall replication state tracking
- The "pg_" prefix follows PostgreSQL's internal naming conventions for system-generated objects
- Buffer size checking is handled by snprintf to prevent buffer overflows