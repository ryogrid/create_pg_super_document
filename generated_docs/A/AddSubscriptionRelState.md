# AddSubscriptionRelState

## Location
[src/backend/catalog/pg_subscription.c:236-289](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_subscription.c#L236-L289)

## Overview
Adds a new state record for a subscription table to the pg_subscription_rel system catalog, establishing the replication state tracking for a specific relation within a logical replication subscription.

## Definition

```c
void
AddSubscriptionRelState(Oid subid, Oid relid, char state,
						XLogRecPtr sublsn, bool retain_lock)
```
## Detailed Description
This function creates a new entry in the pg_subscription_rel catalog to track the replication state of a specific table within a logical replication subscription. It validates that the subscription-relation pair doesn't already exist, then inserts a new tuple with the provided state information. The function handles proper locking of both the subscription and the catalog relation, with an option to retain locks for binary upgrade scenarios.

The function performs duplicate checking by searching the SUBSCRIPTIONRELMAP cache before insertion. It constructs a heap tuple with the subscription ID, relation ID, state character, and optionally the LSN position, then inserts it into the catalog using the standard catalog insertion mechanism.

## Parameters / Member Variables
- `subid`: The OID of the subscription that will track this relation
- `relid`: The OID of the relation (table) to be tracked
- `state`: Character representing the replication state (e.g., 'i' for initialize, 's' for synchronized)
- `sublsn`: XLogRecPtr indicating the LSN position for replication tracking (can be InvalidXLogRecPtr)
- `retain_lock`: Boolean flag indicating whether to retain locks after insertion (used in binary upgrade mode)
## Dependencies
- Functions called/Symbols referenced:
  - [LockSharedObject](../L/LockSharedObject.md)
  - SearchSysCacheCopy2
  - [CharGetDatum](../C/CharGetDatum.md)
  - [LSNGetDatum](../L/LSNGetDatum.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [CatalogTupleInsert](../C/CatalogTupleInsert.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - [UnlockSharedObject](../U/UnlockSharedObject.md)
- Called from (representative examples):
  - [CreateSubscription](../C/CreateSubscription.md)
  - [binary_upgrade_add_sub_rel_state](../b/binary_upgrade_add_sub_rel_state.md)

## Notes and Other Information
- The function enforces uniqueness by checking for existing subscription-relation mappings before insertion
- Proper error handling is implemented with ERROR level logging for duplicate entries
- Lock management varies based on the retain_lock parameter, supporting both normal operation and binary upgrade scenarios
- The sublsn parameter can be NULL/InvalidXLogRecPtr, which is handled by setting the corresponding null flag in the tuple
- Located in src/backend/catalog/pg_subscription.c:236-289

## Simplified Source

```c
void
AddSubscriptionRelState(Oid subid, Oid relid, char state,
                       XLogRecPtr sublsn, bool retain_lock)
{
    Relation rel;
    HeapTuple tup;
    bool nulls[Natts_pg_subscription_rel];
    Datum values[Natts_pg_subscription_rel];

    // Lock the subscription to prevent concurrent changes
    LockSharedObject(SubscriptionRelationId, subid, 0, AccessShareLock);

    // Open the subscription relation catalog
    rel = table_open(SubscriptionRelRelationId, RowExclusiveLock);

    // Check if mapping already exists
    tup = SearchSysCacheCopy2(SUBSCRIPTIONRELMAP,
                             ObjectIdGetDatum(relid),
                             ObjectIdGetDatum(subid));
    if (HeapTupleIsValid(tup))
        elog(ERROR, "subscription table %u in subscription %u already exists",
             relid, subid);

    // Prepare the new tuple
    memset(values, 0, sizeof(values));
    memset(nulls, false, sizeof(nulls));

    values[Anum_pg_subscription_rel_srsubid - 1] = ObjectIdGetDatum(subid);
    values[Anum_pg_subscription_rel_srrelid - 1] = ObjectIdGetDatum(relid);
    values[Anum_pg_subscription_rel_srsubstate - 1] = CharGetDatum(state);

    // Handle LSN - can be invalid
    if (sublsn != InvalidXLogRecPtr)
        values[Anum_pg_subscription_rel_srsublsn - 1] = LSNGetDatum(sublsn);
    else
        nulls[Anum_pg_subscription_rel_srsublsn - 1] = true;

    // Insert the new subscription relation state
    tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);
    CatalogTupleInsert(rel, tup);
    heap_freetuple(tup);

    // Clean up with appropriate locking behavior
    if (retain_lock)
        table_close(rel, NoLock);
    else
    {
        table_close(rel, RowExclusiveLock);
        UnlockSharedObject(SubscriptionRelationId, subid, 0, AccessShareLock);
    }
}
```