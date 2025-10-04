# GetSubscriptionRelState

## Location
[src/backend/catalog/pg_subscription.c:366-415](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_subscription.c#L366-L415)

## Overview
Retrieves the current replication state and LSN position of a specific table within a logical replication subscription from the pg_subscription_rel system catalog.

## Definition

```c
char
GetSubscriptionRelState(Oid subid, Oid relid, XLogRecPtr *sublsn)
```
## Detailed Description
This function queries the pg_subscription_rel catalog to retrieve the current replication state and associated LSN position for a specific subscription-relation pair. It provides race condition protection against AlterSubscription operations by acquiring an AccessShareLock on the catalog relation. The function returns both the state character and the LSN position through an output parameter.

The function handles cases where the subscription-relation mapping doesn't exist by returning SUBREL_STATE_UNKNOWN and setting the LSN to InvalidXLogRecPtr. It properly manages null LSN values in the catalog by checking the isnull flag from SysCacheGetAttr.

## Parameters / Member Variables
- `subid`: The OID of the subscription to query
- `relid`: The OID of the relation (table) to look up
- `*sublsn`: Output parameter that receives the LSN position (pointer to XLogRecPtr)
## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache2](../S/SearchSysCache2.md)
  - SUBREL_STATE_UNKNOWN
  - Form_pg_subscription_rel
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md)
  - [DatumGetLSN](../D/DatumGetLSN.md)
- Called from (representative examples):
  - [logicalrep_rel_open](../l/logicalrep_rel_open.md)
  - [wait_for_relation_state_change](../w/wait_for_relation_state_change.md)
  - [LogicalRepSyncTableStart](../L/LogicalRepSyncTableStart.md)

## Notes and Other Information
- Provides race condition protection by holding AccessShareLock during the lookup operation
- Returns SUBREL_STATE_UNKNOWN when the subscription-relation mapping doesn't exist
- Properly handles null LSN values in the catalog by setting output parameter to InvalidXLogRecPtr
- Uses system cache for efficient lookup of subscription relation mappings
- Critical for logical replication state management and synchronization processes
- Located in src/backend/catalog/pg_subscription.c:366-415

## Simplified Source

```c
char GetSubscriptionRelState(Oid subid, Oid relid, XLogRecPtr *sublsn) {
    // Open subscription_rel catalog with shared lock for race protection
    Relation rel = table_open(SubscriptionRelRelationId, AccessShareLock);

    // Search for the subscription-relation mapping
    HeapTuple tup = SearchSysCache2(SUBSCRIPTIONRELMAP,
                                  ObjectIdGetDatum(relid),
                                  ObjectIdGetDatum(subid));

    if (!HeapTupleIsValid(tup)) {
        // No mapping found - return unknown state
        table_close(rel, AccessShareLock);
        *sublsn = InvalidXLogRecPtr;
        return SUBREL_STATE_UNKNOWN;
    }

    // Extract state from the tuple
    char substate = ((Form_pg_subscription_rel) GETSTRUCT(tup))->srsubstate;

    // Extract LSN, handle null values
    bool isnull;
    Datum d = SysCacheGetAttr(SUBSCRIPTIONRELMAP, tup,
                             Anum_pg_subscription_rel_srsublsn, &isnull);
    *sublsn = isnull ? InvalidXLogRecPtr : DatumGetLSN(d);

    // Cleanup
    ReleaseSysCache(tup);
    table_close(rel, AccessShareLock);

    return substate;
}
```