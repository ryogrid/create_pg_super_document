# get_subscription_name

## Location
[src/backend/utils/cache/lsyscache.c:3695-3716](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L3695-L3716)

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
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - HeapTupleIsValid
  - elog
  - GETSTRUCT
  - [pstrdup](../p/pstrdup.md)
  - NameStr
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - Form_pg_subscription
- Called from (representative examples):
  - [getObjectDescription](getObjectDescription.md)
  - [getObjectIdentityParts](getObjectIdentityParts.md)
  - [RemoveSubscriptionRel](../R/RemoveSubscriptionRel.md)

## Notes and Other Information
- Returns a palloc'd string that should be freed by the caller when no longer needed
- This function is the reverse operation of get_subscription_oid
- Used extensively in object description and identity functions for system catalogs
- Part of the logical replication infrastructure in PostgreSQL
- Located in src/backend/utils/cache/lsyscache.c:3695-3716
- The returned string is a copy, so modifications won't affect the system catalog
- Used during subscription removal operations and system catalog introspection

## Simplified Source

```c
char *
get_subscription_name(Oid subid, bool missing_ok)
{
    HeapTuple tup;
    char *subname;
    Form_pg_subscription subform;

    // Look up subscription by OID
    tup = SearchSysCache1(SUBSCRIPTIONOID, ObjectIdGetDatum(subid));

    if (!HeapTupleIsValid(tup)) {
        if (!missing_ok)
            elog(ERROR, "cache lookup failed for subscription %u", subid);
        return NULL;
    }

    // Extract and copy subscription name
    subform = (Form_pg_subscription) GETSTRUCT(tup);
    subname = pstrdup(NameStr(subform->subname));

    ReleaseSysCache(tup);
    return subname;
}
```