# UpdateTwoPhaseState

## Location
[src/backend/replication/logical/tablesync.c:1782-1816](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/tablesync.c#L1782-L1816)

## Overview
UpdateTwoPhaseState updates the two-phase commit state of a specified subscription in the pg_subscription catalog table. It modifies the subtwophasestate column to reflect the current two-phase commit capability.

## Definition
```c
void UpdateTwoPhaseState(Oid suboid, char new_state)
```

## Detailed Description
This function performs a catalog update operation to change the two-phase commit state of a logical replication subscription. It opens the pg_subscription catalog table with exclusive row lock, locates the subscription record by OID, and updates the subtwophasestate column with the new state value.

The function validates that the new state is one of the three allowed values: DISABLED, PENDING, or ENABLED. It uses PostgreSQL's standard catalog update pattern: search for the tuple, create a modified version, update the catalog, and clean up resources. The operation is performed within the current transaction context and will be committed or rolled back along with other transaction operations.

This function is critical for managing the progression of two-phase commit support during logical replication setup and operation, allowing subscriptions to transition between different levels of two-phase commit support.

## Parameters / Member Variables
- `suboid`: The OID of the subscription whose two-phase state should be updated
- `new_state`: The new two-phase state value (must be one of LOGICALREP_TWOPHASE_STATE_DISABLED, LOGICALREP_TWOPHASE_STATE_PENDING, or LOGICALREP_TWOPHASE_STATE_ENABLED)

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)
  - SearchSysCacheCopy1
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - HeapTupleIsValid
  - [CharGetDatum](../C/CharGetDatum.md)
  - [heap_modify_tuple](../h/heap_modify_tuple.md)
  - RelationGetDescr
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - [table_close](../t/table_close.md)
  - Constants: LOGICALREP_TWOPHASE_STATE_DISABLED, LOGICALREP_TWOPHASE_STATE_PENDING, LOGICALREP_TWOPHASE_STATE_ENABLED
- Called from (representative examples):
  - [CreateSubscription](../C/CreateSubscription.md)
  - [run_apply_worker](../r/run_apply_worker.md)

## Notes and Other Information
- Located in src/backend/replication/logical/tablesync.c:1782-1816
- Uses RowExclusiveLock on the pg_subscription table during the update
- Includes assertion to validate the new_state parameter values
- Throws ERROR if the subscription OID is not found in the catalog
- Uses standard PostgreSQL heap tuple modification pattern
- Part of the logical replication two-phase commit infrastructure
- The function handles all necessary memory management for heap tuples
- Updates are made to the live catalog and will affect subsequent subscription operations

## Simplified Source

```c
void
UpdateTwoPhaseState(Oid suboid, char new_state)
{
    Relation rel;
    HeapTuple tup;
    bool nulls[Natts_pg_subscription];
    bool replaces[Natts_pg_subscription];
    Datum values[Natts_pg_subscription];

    // Validate new state value
    Assert(new_state == LOGICALREP_TWOPHASE_STATE_DISABLED ||
           new_state == LOGICALREP_TWOPHASE_STATE_PENDING ||
           new_state == LOGICALREP_TWOPHASE_STATE_ENABLED);

    // Open subscription catalog table
    rel = table_open(SubscriptionRelationId, RowExclusiveLock);

    // Find the subscription tuple
    tup = SearchSysCacheCopy1(SUBSCRIPTIONOID, ObjectIdGetDatum(suboid));
    if (!HeapTupleIsValid(tup))
        elog(ERROR, "cache lookup failed for subscription oid %u", suboid);

    // Prepare tuple modification arrays
    memset(values, 0, sizeof(values));
    memset(nulls, false, sizeof(nulls));
    memset(replaces, false, sizeof(replaces));

    // Set new two-phase state
    values[Anum_pg_subscription_subtwophasestate - 1] = CharGetDatum(new_state);
    replaces[Anum_pg_subscription_subtwophasestate - 1] = true;

    // Update the catalog tuple
    tup = heap_modify_tuple(tup, RelationGetDescr(rel), values, nulls, replaces);
    CatalogTupleUpdate(rel, &tup->t_self, tup);

    // Clean up
    heap_freetuple(tup);
    table_close(rel, RowExclusiveLock);
}
```