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
  - load_file
  - walrcv_connect
  - check_publications
  - fetch_table_list
  - GetSubscriptionRelations
  - RangeVarGetRelid
  - CheckSubscriptionRelkind
  - AddSubscriptionRelState
  - GetSubscriptionRelState
  - RemoveSubscriptionRel
  - logicalrep_worker_stop
  - ReplicationOriginNameForLogicalRep
  - replorigin_drop_by_name
  - ReplicationSlotNameForTablesync
  - ReplicationSlotDropAtPubNode
  - walrcv_disconnect
  - table_open
  - table_close
- Called from (representative examples):
  - AlterSubscription (at src/backend/commands/subscriptioncmds.c:1341)
  - AlterSubscription (at src/backend/commands/subscriptioncmds.c:1400)
  - AlterSubscription (at src/backend/commands/subscriptioncmds.c:1442)

## Notes and Other Information
- Uses WAL receiver connection to communicate with publisher
- Employs binary search on sorted OID arrays for efficient table lookup performance
- Implements proper error handling with PG_TRY/PG_FINALLY blocks to ensure connection cleanup
- Requires AccessExclusiveLock on pg_subscription_rel to prevent race conditions with apply workers
- Handles cleanup of tablesync origins and replication slots for removed tables
- Supports password authentication when required by subscription configuration
- Location: src/backend/commands/subscriptioncmds.c:859-1078