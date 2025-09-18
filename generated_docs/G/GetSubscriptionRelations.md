# GetSubscriptionRelations

## Location
src/backend/catalog/pg_subscription.c: 526 - 578

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
  - table_open
  - ScanKeyInit
  - systable_beginscan
  - systable_getnext
  - HeapTupleIsValid
  - GETSTRUCT
  - palloc
  - SysCacheGetAttr
  - DatumGetLSN
  - lappend
  - systable_endscan
  - table_close
  - SysScanDesc
  - Form_pg_subscription_rel
  - SubscriptionRelState
  - SUBREL_STATE_READY
  - CharGetDatum
- Called from (representative examples):
  - DropSubscription
  - FetchTableStates

## Notes and Other Information
- Returns a palloc'ed List in the current memory context
- Each list element is a SubscriptionRelState structure containing relid, state, and lsn
- The not_ready parameter allows efficient filtering at the database level rather than in application code
- Handles null LSN values by setting them to InvalidXLogRecPtr
- Uses AccessShareLock for safe concurrent access to the system catalog
- More comprehensive than HasSubscriptionRelations but with higher overhead when you only need existence information