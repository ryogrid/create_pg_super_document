# TransferExpandedObject

## Location
[src/backend/utils/adt/expandeddatum.c:118-135](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/expandeddatum.c#L118-L135)

## Overview
Transfers ownership of an expanded object to a new parent memory context while returning the object's standard read-write pointer.

## Definition

```c
Datum
TransferExpandedObject(Datum d, MemoryContext new_parent)
```
## Detailed Description
TransferExpandedObject is a utility function that manages memory context ownership for expanded objects in PostgreSQL. It takes an expanded object referenced by a read-write pointer and transfers its ownership to a new parent memory context. The function ensures that the returned pointer is always the "standard" read-write pointer, which has the same lifespan as the object itself and provides a unique identifier for the object.

The function performs ownership transfer by changing the parent memory context of the expanded object header's context, ensuring proper memory management hierarchy within PostgreSQL's memory context system.

## Parameters / Member Variables
- `d`: A Datum containing a read-write pointer to the expanded object to be transferred
- `new_parent`: The target MemoryContext that will become the new parent for the expanded object
## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetEOHP](../D/DatumGetEOHP.md)
  - [ExpandedObjectHeader](../E/ExpandedObjectHeader.md)
  - VARATT_IS_EXTERNAL_EXPANDED_RW
  - [MemoryContextSetParent](../M/MemoryContextSetParent.md)
  - [EOHPGetRWDatum](../E/EOHPGetRWDatum.md)
- Called from (representative examples):
  - [datumTransfer](../d/datumTransfer.md)
  - MakeExpandedObjectReadOnly

## Notes and Other Information
- The input Datum must contain a read-write pointer to an expanded object (verified by assertion)
- The function always returns the object's standard read-write pointer, ensuring consistency
- This function is essential for proper memory management when expanded objects need to outlive their original memory context
- The transfer operation maintains the expanded object's internal structure while changing its memory context hierarchy