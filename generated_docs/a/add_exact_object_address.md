# add_exact_object_address

## Location
[src/backend/catalog/dependency.c:2533-2557](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/dependency.c#L2533-L2557)

## Overview
A public utility function that adds a complete ObjectAddress structure to an ObjectAddresses array, providing a convenient interface for adding pre-constructed object references.

## Definition

```c
void
add_exact_object_address(const ObjectAddress *object,
						 ObjectAddresses *addrs)
```
## Detailed Description
This function serves as a public interface for adding ObjectAddress entries to an ObjectAddresses collection when the caller already has a complete ObjectAddress structure. Unlike the static add_object_address function that takes individual components, this function accepts a pointer to an existing ObjectAddress and copies its entire contents.

The function follows the same memory management strategy as add_object_address, doubling the array capacity when expansion is needed and using repalloc for memory reallocation. It includes the same assertion that the 'extras' array should be NULL during expansion.

This is the preferred interface for external callers throughout the PostgreSQL codebase when they need to add object dependencies and already have ObjectAddress structures available, which is common during catalog operations and dependency tracking.

## Parameters / Member Variables
- `*object`: Pointer to a complete ObjectAddress structure to be copied into the array
- `*addrs`: Pointer to the ObjectAddresses structure to modify
## Dependencies
- Functions called/Symbols referenced:
  - [repalloc](../r/repalloc.md) (memory reallocation)
  - ObjectAddresses (struct type)
  - [ObjectAddress](../O/ObjectAddress.md) (struct type)
  - Assert (debugging macro)
- Called from (representative examples):
  - [heap_create_with_catalog](../h/heap_create_with_catalog.md)
  - [index_create](../i/index_create.md)
  - [AggregateCreate](../A/AggregateCreate.md)
  - [ProcedureCreate](../P/ProcedureCreate.md)
  - [CreateConstraintEntry](../C/CreateConstraintEntry.md)
  - [GenerateTypeDependencies](../G/GenerateTypeDependencies.md)
  - [InsertExtensionTuple](../I/InsertExtensionTuple.md)

## Notes and Other Information
- Public function (non-static) - available to external callers
- Uses structure copy (*item = *object) for efficient copying
- Same array growth strategy as add_object_address (doubling)
- Extensively used throughout catalog operations for dependency tracking
- Preferred interface when ObjectAddress structures are already available
- Does not perform duplicate checking - caller responsibility
- Memory managed through PostgreSQL's palloc/repalloc system
- Critical for maintaining object dependency relationships during DDL operations

## Simplified Source

```c
void
add_exact_object_address(const ObjectAddress *object,
                         ObjectAddresses *addrs)
{
    ObjectAddress *item;

    // Expand array if needed (double the capacity)
    if (addrs->numrefs >= addrs->maxrefs)
    {
        addrs->maxrefs *= 2;
        addrs->refs = (ObjectAddress *)
            repalloc(addrs->refs, addrs->maxrefs * sizeof(ObjectAddress));
        Assert(!addrs->extras);  // Should be NULL during expansion
    }

    // Copy the ObjectAddress structure and increment count
    item = addrs->refs + addrs->numrefs;
    *item = *object;  // Structure copy
    addrs->numrefs++;
}
```