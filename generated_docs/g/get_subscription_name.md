# get_subscription_name

## Location
src/backend/utils/cache/lsyscache.c: 3695 - 3716

## Overview
Retrieves the name of a subscription given its object identifier (OID), with optional error handling for missing subscriptions.

## Definition
```c
char *get_subscription_name(Oid subid, bool missing_ok)
```

## Detailed Description
This function performs a reverse lookup in the PostgreSQL system cache to find the name of a subscription identified by its OID. Subscriptions are key components of PostgreSQL's logical replication system on the subscriber side, and this function is commonly used when displaying subscription information, generating error messages, or performing operations that need to include subscription names.

The function searches the SUBSCRIPTIONOID system cache to retrieve the subscription's metadata, extracts the subscription name from the Form_pg_subscription structure, and returns a dynamically allocated copy of the name string. The behavior when a subscription is not found depends on the missing_ok parameter.

## Parameters / Member Variables
- `subid`: The object identifier (OID) of the subscription to look up
- `missing_ok`: If false, throw an error when subscription is not found; if true, return NULL instead

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache1
  - ObjectIdGetDatum
  - HeapTupleIsValid
  - elog
  - GETSTRUCT
  - pstrdup
  - NameStr
  - ReleaseSysCache
  - Form_pg_subscription
- Called from (representative examples):
  - getObjectDescription
  - getObjectIdentityParts
  - RemoveSubscriptionRel

## Notes and Other Information
- Returns a palloc'd string that should be freed by the caller when no longer needed
- This function is the reverse operation of get_subscription_oid
- Used extensively in object description and identity functions for system catalogs
- Part of the logical replication infrastructure in PostgreSQL
- Located in src/backend/utils/cache/lsyscache.c:3695-3716
- The returned string is a copy, so modifications won't affect the system catalog
- Used during subscription removal operations and system catalog introspection