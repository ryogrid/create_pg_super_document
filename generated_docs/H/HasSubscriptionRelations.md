# HasSubscriptionRelations

## Location
[src/backend/catalog/pg_subscription.c:491-525](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_subscription.c#L491-L525)

## Overview
Determines whether a subscription has any associated relation subscriptions by checking for the existence of records in the pg_subscription_rel system catalog.

## Definition

```c
bool
HasSubscriptionRelations(Oid subid)
```
## Detailed Description
HasSubscriptionRelations is a utility function that performs a quick existence check to determine if a given subscription has any table relations associated with it. The function opens the pg_subscription_rel system catalog and performs a scan looking for any tuples with the specified subscription ID. It's designed as an efficient boolean check that avoids the overhead of building a complete list of relations when only a true/false answer is needed.

The function uses a system catalog scan with an equality condition on the subscription ID column (srsubid). It only needs to find a single matching tuple to return true, making it more efficient than GetSubscriptionRelations when you don't need the actual list of relations.

## Parameters / Member Variables
- `subid`: The OID of the subscription to check for associated relations
## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - HeapTupleIsValid
  - [systable_endscan](../s/systable_endscan.md)
  - [table_close](../t/table_close.md)
  - [SysScanDesc](../S/SysScanDesc.md)
- Called from (representative examples):
  - [FetchTableStates](../F/FetchTableStates.md)

## Notes and Other Information
- This function is specifically designed for cases where you only need to know if relations exist, not what they are
- More efficient than GetSubscriptionRelations when you don't need the actual relation list
- Uses AccessShareLock for safe concurrent access to the system catalog
- Returns immediately after finding the first matching tuple for optimal performance
- The function comment explicitly notes it should be used when you have no need for the List returned by GetSubscriptionRelations