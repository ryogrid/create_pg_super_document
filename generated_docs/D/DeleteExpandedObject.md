# DeleteExpandedObject

## Location
src/backend/utils/adt/expandeddatum.c: 136 - 145

## Overview
Deletes an expanded object by destroying its associated memory context, effectively freeing all memory allocated for the object.

## Definition
```c
void DeleteExpandedObject(Datum d)
```

## Detailed Description
DeleteExpandedObject is a memory management function that completely destroys an expanded object in PostgreSQL. It takes a Datum containing a read-write pointer to an expanded object and deletes the entire object by destroying its memory context. This operation is irreversible and frees all memory associated with the expanded object, including the object header and any allocated data structures within the object's memory context.

The function performs a complete cleanup by calling MemoryContextDelete on the expanded object's memory context, which recursively deletes all child contexts and frees all allocated memory blocks within the context hierarchy.

## Parameters / Member Variables
- `d`: A Datum containing a read-write pointer to the expanded object to be deleted

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetEOHP
  - [ExpandedObjectHeader](../E/ExpandedObjectHeader.md)
  - VARATT_IS_EXTERNAL_EXPANDED_RW
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
- Called from (representative examples):
  - [ExecAggCopyTransValue](../E/ExecAggCopyTransValue.md)
  - [advance_windowaggregate](../a/advance_windowaggregate.md)
  - [advance_windowaggregate_base](../a/advance_windowaggregate_base.md)
  - MakeExpandedObjectReadOnly

## Notes and Other Information
- The input Datum must contain a read-write pointer to an expanded object (verified by assertion)
- This function performs complete object destruction and memory deallocation
- After calling this function, the Datum pointer becomes invalid and must not be used
- Commonly used in aggregate operations and window functions for cleaning up temporary expanded objects
- The deletion is performed through PostgreSQL's memory context system, ensuring proper cleanup of nested allocations