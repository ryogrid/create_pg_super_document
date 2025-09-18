# AlterSubscription_refresh

## Location
src/backend/commands/subscriptioncmds.c: 859 - 871

## Overview
Refreshes a subscription by synchronizing its table list with the publisher, adding new tables and removing tables that are no longer published.

## Definition


## Detailed Description
This function performs a comprehensive refresh operation on a logical replication subscription. It connects to the publisher, fetches the current list of published tables, compares it with the local subscription's table list, and synchronizes the differences. New tables are added to the subscription with appropriate initial state (INIT or READY depending on copy_data), while tables no longer published are removed along with their associated replication slots and origins. The function ensures data consistency by using appropriate locking mechanisms and handles cleanup of replication infrastructure gracefully.

## Parameters / Member Variables
- : Pointer to the Subscription structure containing subscription details including connection info, publications, and configuration
- : Boolean flag indicating whether initial table synchronization should copy existing data (INIT state) or start from current position (READY state)  
- : List of publication names to validate before performing the refresh operation

## Dependencies
- Functions called/Symbols referenced:
  - [load_file](../l/load_file.md)
  - walrcv_connect
  - [check_publications](../c/check_publications.md)
  - [fetch_table_list](../f/fetch_table_list.md)
  - [GetSubscriptionRelations](../G/GetSubscriptionRelations.md)
  - RangeVarGetRelid
  - [CheckSubscriptionRelkind](../C/CheckSubscriptionRelkind.md)
  - [AddSubscriptionRelState](AddSubscriptionRelState.md)
  - [GetSubscriptionRelState](../G/GetSubscriptionRelState.md)
  - [RemoveSubscriptionRel](../R/RemoveSubscriptionRel.md)
  - logicalrep_worker_stop
  - [ReplicationOriginNameForLogicalRep](../R/ReplicationOriginNameForLogicalRep.md)
  - [replorigin_drop_by_name](../r/replorigin_drop_by_name.md)
  - [ReplicationSlotNameForTablesync](../R/ReplicationSlotNameForTablesync.md)
  - [ReplicationSlotDropAtPubNode](../R/ReplicationSlotDropAtPubNode.md)
  - walrcv_disconnect
  - table_open
  - table_close
- Called from (representative examples):
  - [AlterSubscription](AlterSubscription.md) (at src/backend/commands/subscriptioncmds.c:1341)
  - [AlterSubscription](AlterSubscription.md) (at src/backend/commands/subscriptioncmds.c:1400)
  - [AlterSubscription](AlterSubscription.md) (at src/backend/commands/subscriptioncmds.c:1442)

## Notes and Other Information
- Uses WAL receiver connection to communicate with publisher
- Employs binary search on sorted OID arrays for efficient table lookup performance
- Implements proper error handling with PG_TRY/PG_FINALLY blocks to ensure connection cleanup
- Requires AccessExclusiveLock on pg_subscription_rel to prevent race conditions with apply workers
- Handles cleanup of tablesync origins and replication slots for removed tables
- Supports password authentication when required by subscription configuration
- Location: src/backend/commands/subscriptioncmds.c:859-1078