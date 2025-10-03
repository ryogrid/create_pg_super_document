# AlterSubscriptionOwner_oid

## Location
[src/backend/commands/subscriptioncmds.c:1995-2031](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/subscriptioncmds.c#L1995-L2031)

## Overview
Changes the ownership of a PostgreSQL logical replication subscription using the subscription's OID (Object Identifier) as the lookup key.

## Definition

```c
void
AlterSubscriptionOwner_oid(Oid subid, Oid newOwnerId)
```
## Detailed Description
This function serves as a public interface for changing subscription ownership when the subscription is identified by its OID rather than by name. It performs the necessary system catalog lookups to validate the subscription exists, then delegates the actual ownership change to the internal implementation function. The function handles error reporting if the specified subscription OID does not exist in the system.

## Parameters / Member Variables
- `subid`: The OID of the subscription whose ownership is to be changed
- `newOwnerId`: The OID of the new owner (user) who will own the subscription
## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)
  - SearchSysCacheCopy1
  - HeapTupleIsValid
  - ereport
  - [AlterSubscriptionOwner_internal](AlterSubscriptionOwner_internal.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - [table_close](../t/table_close.md)
- Called from (representative examples):
  - [shdepReassignOwned_Owner](../s/shdepReassignOwned_Owner.md)

## Notes and Other Information
- This function is typically used during ownership reassignment operations, particularly when reassigning objects during role/user management
- It opens the subscription system catalog with RowExclusiveLock to ensure exclusive access during the ownership change
- Error handling ensures that invalid subscription OIDs are properly reported with appropriate error codes
- The function follows PostgreSQL's standard pattern of public wrapper functions that perform validation before calling internal implementation functions

## Simplified Source

```c
void AlterSubscriptionOwner_oid(Oid subid, Oid newOwnerId)
{
    // Open subscription catalog table
    Relation rel = table_open(SubscriptionRelationId, RowExclusiveLock);

    // Find subscription by OID
    HeapTuple tup = SearchSysCacheCopy1(SUBSCRIPTIONOID, ObjectIdGetDatum(subid));

    // Validate subscription exists
    if (!HeapTupleIsValid(tup))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("subscription with OID %u does not exist", subid)));

    // Perform ownership change
    AlterSubscriptionOwner_internal(rel, tup, newOwnerId);

    // Cleanup resources
    heap_freetuple(tup);
    table_close(rel, RowExclusiveLock);
}
```