# DropSubscription

## Location
[src/backend/commands/subscriptioncmds.c:1553-1843](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/subscriptioncmds.c#L1553-L1843)

## Overview
DropSubscription removes a logical replication subscription, cleaning up all associated resources including replication slots, workers, origins, and catalog entries.

## Definition

```c
void
DropSubscription(DropSubscriptionStmt *stmt, bool isTopLevel)
```
## Detailed Description
DropSubscription is responsible for the complete removal of a logical replication subscription from the PostgreSQL system. It performs comprehensive cleanup operations in a specific order to ensure data consistency and proper resource management.

The function handles multiple cleanup phases:
1. Validates subscription existence and ownership permissions
2. Stops all active replication workers (apply and tablesync workers) immediately
3. Removes catalog entries from pg_subscription and related tables
4. Cleans up replication origins for both main subscription and tablesync operations
5. Drops replication slots on the publisher node via network connection
6. Updates cumulative statistics and removes dependency records

The function enforces transaction block restrictions when dropping replication slots, since slot dropping is not transactional and cannot be rolled back. It also handles missing_ok behavior for graceful handling of non-existent subscriptions.

## Parameters / Member Variables
- : DROP SUBSCRIPTION statement containing subscription name and options like missing_ok flag
- : Boolean indicating if this is a top-level command, used to prevent running in transaction blocks when dropping slots

## Dependencies
- Functions called/Symbols referenced:
  - [GetSubscriptionRelations](../G/GetSubscriptionRelations.md): Retrieves subscription relation states for cleanup
  - [RemoveSubscriptionRel](../R/RemoveSubscriptionRel.md): Removes subscription relation mappings from catalog
  - [ReplicationSlotDropAtPubNode](../R/ReplicationSlotDropAtPubNode.md): Drops replication slots on the publisher node
  - [logicalrep_workers_find](../l/logicalrep_workers_find.md): Finds active replication workers for the subscription
  - [logicalrep_worker_stop](../l/logicalrep_worker_stop.md): Stops specific replication workers
  - [PreventInTransactionBlock](../P/PreventInTransactionBlock.md): Prevents operation from running in transaction blocks
  - [replorigin_drop_by_name](../r/replorigin_drop_by_name.md): Removes replication origin tracking entries
  - [pgstat_drop_subscription](../p/pgstat_drop_subscription.md): Updates cumulative statistics for dropped subscription
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md): Main utility command processor in tcop/utility.c:1869

## Notes and Other Information
- Requires exclusive lock on subscription to prevent concurrent modifications during deletion
- Stops all workers immediately to make replication slots accessible for dropping
- Network connection to publisher is established only when necessary for slot cleanup
- Handles both main subscription slots and tablesync-specific slots separately
- Uses PG_TRY/PG_FINALLY blocks to ensure proper connection cleanup even on errors
- Supports graceful degradation when publisher connection fails but subscription cleanup can continue
- Updates event triggers and dependency system for proper DROP cascade handling
- Cannot be rolled back when replication slots are involved due to non-transactional nature of slot operations