# UpdateSubscriptionRelStateEx

## Location
src/backend/catalog/pg_subscription.c: 290 - 353

## Overview
Updates the replication state of an existing subscription table in the pg_subscription_rel system catalog, providing flexible lock management for different execution contexts.

## Definition


## Detailed Description
This function modifies an existing entry in the pg_subscription_rel catalog to update the replication state and LSN position of a specific table within a logical replication subscription. It provides extended functionality compared to the basic UpdateSubscriptionRelState by allowing callers to specify whether appropriate locks are already held, enabling more efficient operation in contexts where locking has been handled externally.

The function performs validation to ensure the subscription-relation mapping exists before attempting the update. It uses heap_modify_tuple to update only the state and LSN fields while preserving other tuple data, then commits the changes using the catalog update mechanism.

## Parameters / Member Variables
- : The OID of the subscription containing the relation to update
- : The OID of the relation (table) whose state should be updated
- : New character representing the replication state
- : New XLogRecPtr indicating the LSN position for replication tracking
- : Boolean flag indicating whether necessary locks are already held by the caller

## Dependencies
- Functions called/Symbols referenced:
  - LOCKTAG
  - CheckRelationOidLockedByMe
  - SET_LOCKTAG_OBJECT
  - LockHeldByMe
  - LockSharedObject
  - SearchSysCacheCopy2
  - CharGetDatum
  - LSNGetDatum
  - heap_modify_tuple
  - CatalogTupleUpdate
- Called from (representative examples):
  - UpdateSubscriptionRelState
  - tablesync_start_time_mapping

## Notes and Other Information
- Includes comprehensive assertion checking when USE_ASSERT_CHECKING is defined to verify lock state
- The already_locked parameter enables optimization in scenarios where locks are managed at a higher level
- Validates existence of the subscription-relation mapping before attempting updates
- Only modifies the state and LSN fields, preserving other tuple attributes
- Error handling includes ERROR level logging for non-existent subscription-relation pairs
- Located in src/backend/catalog/pg_subscription.c:290-353