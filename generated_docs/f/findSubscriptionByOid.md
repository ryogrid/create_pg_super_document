# findSubscriptionByOid

## Location
[src/bin/pg_dump/common.c:1033-1051](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/common.c#L1033-L1051)

## Overview
Finds and returns the DumpableObject for a PostgreSQL logical replication subscription with the specified OID during the pg_dump process.

## Definition
```c
SubscriptionInfo *findSubscriptionByOid(Oid oid)
```

## Detailed Description
This function is part of the pg_dump utility's object lookup system for PostgreSQL logical replication subscriptions. It searches for a subscription object by its Object Identifier (OID) and returns the corresponding SubscriptionInfo structure. Subscriptions are the consumer side of PostgreSQL's logical replication feature, defining which publications to subscribe to and receive data changes from. The function operates by creating a CatalogId structure with the subscription's OID and utilizing the generic findObjectByCatalogId function to locate the object. It includes an assertion to verify that any found object is indeed of type DO_SUBSCRIPTION, ensuring type safety during the dump process.

## Parameters / Member Variables
- `oid`: The Object Identifier (OID) of the subscription to find

## Dependencies
- Functions called/Symbols referenced:
  - [findObjectByCatalogId](findObjectByCatalogId.md)
  - [CatalogId](../C/CatalogId.md) (struct)
  - DumpableObject (struct)
  - SubscriptionInfo (struct)
  - DO_SUBSCRIPTION (enum value)
  - SubscriptionRelationId (constant)
- Called from (representative examples):
  - [getSubscriptionTables](../g/getSubscriptionTables.md) (src/bin/pg_dump/pg_dump.c:5043)

## Notes and Other Information
- Returns NULL if the subscription with the given OID is not found
- Uses an assertion to ensure type safety - the found object must be of DO_SUBSCRIPTION type
- Part of the pg_dump utility's support for logical replication features
- Subscriptions are the receiving end of PostgreSQL's logical replication system
- The function follows the same pattern as other findXXXByOid functions in the codebase
- Used when dumping subscription-related metadata and table relationships
- Complements findPublicationByOid for complete logical replication support