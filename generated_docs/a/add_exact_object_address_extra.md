# add_exact_object_address_extra

## Location
[src/backend/catalog/dependency.c:2558-2592](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/dependency.c#L2558-L2592)

## Overview
A static utility function that adds an ObjectAddress entry along with associated extra metadata to an ObjectAddresses array, managing parallel arrays for both main object data and supplementary information.

## Definition

```c
static void
add_exact_object_address_extra(const ObjectAddress *object,
							   const ObjectAddressExtra *extra,
							   ObjectAddresses *addrs)
```
## Detailed Description
This function extends the basic object address addition functionality by supporting additional metadata storage through the ObjectAddressExtra structure. It maintains two parallel arrays: the main ObjectAddress array and a corresponding ObjectAddressExtra array that stores supplementary information.

The function handles lazy allocation of the extras array - it's only allocated when first needed, optimizing memory usage for cases where extra data isn't required. When the array needs expansion, both the main refs array and the extras array are reallocated together to maintain their parallel relationship.

This is the most comprehensive version of the object address addition functions, used in scenarios where additional context or metadata needs to be preserved alongside the basic object reference information, particularly during complex dependency analysis operations.

## Parameters / Member Variables
- `*object`: Pointer to the ObjectAddress structure to be added
- `*extra`: Pointer to the ObjectAddressExtra structure containing supplementary metadata
- `*addrs`: Pointer to the ObjectAddresses structure to modify
## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (memory allocation)
  - [repalloc](../r/repalloc.md) (memory reallocation)
  - [ObjectAddress](../O/ObjectAddress.md) (struct type)
  - ObjectAddresses (struct type)
  - ObjectAddressExtra (struct type)
- Called from (representative examples):
  - find_expr_references_context
  - [findDependentObjects](../f/findDependentObjects.md)

## Notes and Other Information
- Static function, used internally within dependency.c
- Implements lazy allocation of extras array (allocated only when first needed)
- Maintains parallel arrays for ObjectAddress and ObjectAddressExtra data
- Both arrays are reallocated together during expansion to preserve alignment
- Used in complex dependency scenarios requiring additional metadata
- Less frequently used than basic add_exact_object_address function
- Essential for dependency analysis that needs context beyond basic object identification
- Memory managed through PostgreSQL's palloc/repalloc system

## Simplified Source

```c
static void
add_exact_object_address_extra(const ObjectAddress *object,
                               const ObjectAddressExtra *extra,
                               ObjectAddresses *addrs)
{
    // Lazy allocation: create extras array if not already allocated
    if (!addrs->extras) {
        addrs->extras = palloc(addrs->maxrefs * sizeof(ObjectAddressExtra));
    }

    // Expand arrays if we've reached capacity
    if (addrs->numrefs >= addrs->maxrefs) {
        addrs->maxrefs *= 2;
        addrs->refs = repalloc(addrs->refs, addrs->maxrefs * sizeof(ObjectAddress));
        addrs->extras = repalloc(addrs->extras, addrs->maxrefs * sizeof(ObjectAddressExtra));
    }

    // Add the new object and extra data to parallel arrays
    addrs->refs[addrs->numrefs] = *object;
    addrs->extras[addrs->numrefs] = *extra;
    addrs->numrefs++;
}
```