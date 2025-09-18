# DisableSubscription

## Location
src/backend/catalog/pg_subscription.c: 169 - 209

## Overview
Disables a subscription by setting its enabled flag to false in the pg_subscription system catalog.

## Definition
```c
void DisableSubscription(Oid subid)
```

## Detailed Description
DisableSubscription modifies a subscription's enabled status in the pg_subscription catalog by setting the subenabled field to false. The function performs a catalog lookup to find the subscription tuple, creates a modified version with the enabled flag set to false, and updates the catalog entry. It uses proper locking mechanisms including RowExclusiveLock on the relation and AccessShareLock on the subscription object to ensure safe concurrent access. After updating the catalog, it cleans up the modified tuple memory.

## Parameters / Member Variables
- `subid`: The OID (Object Identifier) of the subscription to disable

## Dependencies
- Functions called/Symbols referenced:
  - table_open (open system table with lock)
  - SearchSysCacheCopy1 (get copy of catalog tuple)
  - HeapTupleIsValid (validate heap tuple)
  - [LockSharedObject](../L/LockSharedObject.md) (acquire shared object lock)
  - [heap_modify_tuple](../h/heap_modify_tuple.md) (create modified tuple)
  - RelationGetDescr (get relation descriptor)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md) (update catalog tuple)
  - [heap_freetuple](../h/heap_freetuple.md) (free tuple memory)
  - table_close (close system table)
- Called from (representative examples):
  - [DisableSubscriptionAndExit](DisableSubscriptionAndExit.md) (logical replication worker error handling)

## Notes and Other Information
- Uses RowExclusiveLock to prevent concurrent modifications to the subscription relation
- Acquires AccessShareLock on the specific subscription object for consistency
- Creates arrays for values, nulls, and replaces to specify which fields to modify
- Only modifies the subenabled field, leaving all other subscription attributes unchanged
- Properly manages memory by freeing the modified heap tuple after catalog update
- Essential for logical replication error handling and subscription lifecycle management
- Part of PostgreSQL's logical replication subscription management system
- Raises ERROR if the subscription is not found in the cache