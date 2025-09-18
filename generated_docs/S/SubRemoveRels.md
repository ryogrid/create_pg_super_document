# SubRemoveRels

## Location
src/backend/commands/subscriptioncmds.c: 872 - 876

## Overview
SubRemoveRels is a local structure used within the  function to track relations that need to be removed from a subscription along with their current replication state.

## Definition


## Detailed Description
The SubRemoveRels structure is a temporary data structure used internally during subscription refresh operations to maintain information about relations that are being removed from a subscription. It stores both the relation identifier and its last known subscription state, which is crucial for proper cleanup operations. The structure is used within  function to handle the removal of tables that are no longer part of the subscription's publication set, ensuring that associated replication workers are stopped and cleanup operations (like dropping tablesync origins) are performed based on the relation's state.

## Parameters / Member Variables
- : Object identifier (Oid) of the relation being removed from the subscription
- : Character representing the last known replication state of the relation (e.g., SUBREL_STATE_INIT, SUBREL_STATE_READY, SUBREL_STATE_SYNCDONE)

## Dependencies
- Functions called/Symbols referenced:
  - Oid (PostgreSQL object identifier type)
- Called from (representative examples):
  - [AlterSubscription_refresh](../A/AlterSubscription_refresh.md) (local usage within the function)

## Notes and Other Information
This structure is defined locally within the  function and is not exposed as part of the public API. It serves as a temporary container during the subscription refresh process where tables that were previously part of the subscription but are no longer in the publication need to be properly cleaned up. The state information is critical because different subscription states require different cleanup procedures - for instance, relations in SUBREL_STATE_READY don't need tablesync origin cleanup since it would have already been dropped, while other states do require this cleanup operation.