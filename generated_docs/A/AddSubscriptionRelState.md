# AddSubscriptionRelState

## Location
src/backend/catalog/pg_subscription.c: 236 - 289

## Overview
Adds a new state record for a subscription table to the pg_subscription_rel system catalog, establishing the replication state tracking for a specific relation within a logical replication subscription.

## Definition


## Detailed Description
This function creates a new entry in the pg_subscription_rel catalog to track the replication state of a specific table within a logical replication subscription. It validates that the subscription-relation pair doesn't already exist, then inserts a new tuple with the provided state information. The function handles proper locking of both the subscription and the catalog relation, with an option to retain locks for binary upgrade scenarios.

The function performs duplicate checking by searching the SUBSCRIPTIONRELMAP cache before insertion. It constructs a heap tuple with the subscription ID, relation ID, state character, and optionally the LSN position, then inserts it into the catalog using the standard catalog insertion mechanism.

## Parameters / Member Variables
- : The OID of the subscription that will track this relation
- : The OID of the relation (table) to be tracked
- : Character representing the replication state (e.g., 'i' for initialize, 's' for synchronized)
- : XLogRecPtr indicating the LSN position for replication tracking (can be InvalidXLogRecPtr)
- : Boolean flag indicating whether to retain locks after insertion (used in binary upgrade mode)

## Dependencies
- Functions called/Symbols referenced:
  - LockSharedObject
  - SearchSysCacheCopy2
  - CharGetDatum
  - LSNGetDatum
  - heap_form_tuple
  - CatalogTupleInsert
  - heap_freetuple
  - UnlockSharedObject
- Called from (representative examples):
  - CreateSubscription
  - binary_upgrade_add_sub_rel_state

## Notes and Other Information
- The function enforces uniqueness by checking for existing subscription-relation mappings before insertion
- Proper error handling is implemented with ERROR level logging for duplicate entries
- Lock management varies based on the retain_lock parameter, supporting both normal operation and binary upgrade scenarios
- The sublsn parameter can be NULL/InvalidXLogRecPtr, which is handled by setting the corresponding null flag in the tuple
- Located in src/backend/catalog/pg_subscription.c:236-289