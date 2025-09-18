# GetSubscriptionRelState

## Location
[src/backend/catalog/pg_subscription.c:366-415](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_subscription.c#L366-L415)

## Overview
Retrieves the current replication state and LSN position of a specific table within a logical replication subscription from the pg_subscription_rel system catalog.

## Definition


## Detailed Description
This function queries the pg_subscription_rel catalog to retrieve the current replication state and associated LSN position for a specific subscription-relation pair. It provides race condition protection against AlterSubscription operations by acquiring an AccessShareLock on the catalog relation. The function returns both the state character and the LSN position through an output parameter.

The function handles cases where the subscription-relation mapping doesn't exist by returning SUBREL_STATE_UNKNOWN and setting the LSN to InvalidXLogRecPtr. It properly manages null LSN values in the catalog by checking the isnull flag from SysCacheGetAttr.

## Parameters / Member Variables
- : The OID of the subscription to query
- : The OID of the relation (table) to look up
- : Output parameter that receives the LSN position (pointer to XLogRecPtr)

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache2](../S/SearchSysCache2.md)
  - SUBREL_STATE_UNKNOWN
  - Form_pg_subscription_rel
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md)
  - DatumGetLSN
- Called from (representative examples):
  - logicalrep_rel_open
  - [wait_for_relation_state_change](../w/wait_for_relation_state_change.md)
  - [LogicalRepSyncTableStart](../L/LogicalRepSyncTableStart.md)

## Notes and Other Information
- Provides race condition protection by holding AccessShareLock during the lookup operation
- Returns SUBREL_STATE_UNKNOWN when the subscription-relation mapping doesn't exist
- Properly handles null LSN values in the catalog by setting output parameter to InvalidXLogRecPtr
- Uses system cache for efficient lookup of subscription relation mappings
- Critical for logical replication state management and synchronization processes
- Located in src/backend/catalog/pg_subscription.c:366-415