# UpdateSubscriptionRelStateEx

## Location
[src/backend/catalog/pg_subscription.c:290-353](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_subscription.c#L290-L353)

## Overview
Updates the replication state of an existing subscription table in the pg_subscription_rel system catalog, providing flexible lock management for different execution contexts.

## Definition

```c
void
UpdateSubscriptionRelStateEx(Oid subid, Oid relid, char state,
							 XLogRecPtr sublsn, bool already_locked)
```
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
  - [LOCKTAG](../L/LOCKTAG.md)
  - [CheckRelationOidLockedByMe](../C/CheckRelationOidLockedByMe.md)
  - SET_LOCKTAG_OBJECT
  - [LockHeldByMe](../L/LockHeldByMe.md)
  - [LockSharedObject](../L/LockSharedObject.md)
  - SearchSysCacheCopy2
  - [CharGetDatum](../C/CharGetDatum.md)
  - [LSNGetDatum](../L/LSNGetDatum.md)
  - [heap_modify_tuple](../h/heap_modify_tuple.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
- Called from (representative examples):
  - [UpdateSubscriptionRelState](UpdateSubscriptionRelState.md)
  - [tablesync_start_time_mapping](../t/tablesync_start_time_mapping.md)

## Notes and Other Information
- Includes comprehensive assertion checking when USE_ASSERT_CHECKING is defined to verify lock state
- The already_locked parameter enables optimization in scenarios where locks are managed at a higher level
- Validates existence of the subscription-relation mapping before attempting updates
- Only modifies the state and LSN fields, preserving other tuple attributes
- Error handling includes ERROR level logging for non-existent subscription-relation pairs
- Located in src/backend/catalog/pg_subscription.c:290-353