# get_subscription_oid

## Location
[src/backend/utils/cache/lsyscache.c:3675-3694](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L3675-L3694)

## Overview
Looks up the object identifier (OID) of a subscription given its name, with optional error handling for missing subscriptions.

## Definition
```c
Oid get_subscription_oid(const char *subname, bool missing_ok)
```

## Detailed Description
This function performs a lookup in the PostgreSQL system cache to find the OID of a subscription identified by its name. Subscriptions are fundamental components of PostgreSQL's logical replication system, representing the subscriber side that receives data changes from publications on remote servers.

The function uses the SUBSCRIPTIONNAME system cache along with the current database ID to efficiently retrieve the subscription's OID. Note that subscriptions are database-specific objects, which is why MyDatabaseId is used as part of the cache key. The behavior when a subscription is not found depends on the missing_ok parameter.

## Parameters / Member Variables
- `subname`: The name of the subscription to look up
- `missing_ok`: If false, throw an error when subscription is not found; if true, return InvalidOid instead

## Dependencies
- Functions called/Symbols referenced:
  - GetSysCacheOid2
  - [CStringGetDatum](../C/CStringGetDatum.md)
  - MyDatabaseId
  - OidIsValid
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
- Called from (representative examples):
  - [get_object_address_unqualified](get_object_address_unqualified.md)
  - [binary_upgrade_add_sub_rel_state](../b/binary_upgrade_add_sub_rel_state.md)
  - [binary_upgrade_replorigin_advance](../b/binary_upgrade_replorigin_advance.md)

## Notes and Other Information
- This function is part of the logical replication infrastructure in PostgreSQL
- Subscriptions are database-scoped objects, hence the use of MyDatabaseId in the cache lookup
- The function provides both strict (error-throwing) and lenient (InvalidOid-returning) lookup modes
- Used during binary upgrades to maintain subscription relationship state
- Located in src/backend/utils/cache/lsyscache.c:3675-3694
- Uses the SUBSCRIPTIONNAME system cache for efficient lookups with database-specific scoping