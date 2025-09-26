# GetSubscriptionRelations

## Location
[src/backend/catalog/pg_subscription.c:526-578](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_subscription.c#L526-L578)

## Overview
Retrieves a list of subscription relations for a given subscription, with optional filtering to return only relations that are not in a ready state.

## Definition
```c
List *GetSubscriptionRelations(Oid subid, bool not_ready)
```

## Detailed Description
GetSubscriptionRelations queries the pg_subscription_rel system catalog to retrieve information about all relations associated with a subscription. The function builds and returns a List of SubscriptionRelState structures, each containing the relation OID, subscription state, and LSN information.

The function supports conditional filtering through the not_ready parameter. When not_ready is true, it adds an additional scan key to exclude relations that are in SUBREL_STATE_READY state, returning only relations that still need processing. When false, it returns all relations regardless of their state.

For each matching tuple, the function constructs a SubscriptionRelState structure containing:
- The relation OID from srrelid
- The subscription state from srsubstate  
- The LSN from srsublsn (handling null values appropriately)

The returned list is allocated in the current memory context and must be managed by the caller.

## Parameters / Member Variables
- `subid`: The OID of the subscription whose relations to retrieve
- `not_ready`: If true, return only relations not in ready state; if false, return all relations

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - HeapTupleIsValid
  - GETSTRUCT
  - [palloc](../p/palloc.md)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md)
  - [DatumGetLSN](../D/DatumGetLSN.md)
  - [lappend](../l/lappend.md)
  - [systable_endscan](../s/systable_endscan.md)
  - [table_close](../t/table_close.md)
  - [SysScanDesc](../S/SysScanDesc.md)
  - Form_pg_subscription_rel
  - SubscriptionRelState
  - SUBREL_STATE_READY
  - [CharGetDatum](../C/CharGetDatum.md)
- Called from (representative examples):
  - [DropSubscription](../D/DropSubscription.md)
  - [FetchTableStates](../F/FetchTableStates.md)

## Notes and Other Information
- Returns a palloc'ed List in the current memory context
- Each list element is a SubscriptionRelState structure containing relid, state, and lsn
- The not_ready parameter allows efficient filtering at the database level rather than in application code
- Handles null LSN values by setting them to InvalidXLogRecPtr
- Uses AccessShareLock for safe concurrent access to the system catalog
- More comprehensive than HasSubscriptionRelations but with higher overhead when you only need existence information