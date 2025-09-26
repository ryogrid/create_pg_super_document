# ResourceElem

## Location
[src/backend/utils/resowner/resowner.c:62-66](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/resowner/resowner.c#L62-L66)

## Overview
ResourceElem represents a reference associated with a resource owner, serving as a fundamental data structure for tracking resources that need cleanup in PostgreSQL.

## Definition
```c
typedef struct ResourceElem
{
    Datum          item;
    const ResourceOwnerDesc *kind;    /* NULL indicates a free hash table slot */
} ResourceElem;
```

## Detailed Description
ResourceElem is a lightweight structure that serves as the basic building block for PostgreSQL's resource tracking system. It encapsulates a resource reference (stored as a Datum for flexibility) along with metadata about what kind of resource it represents. All objects managed by the resource owner system are required to fit into a Datum, which works well since they are generally pointers or integers.

The structure is designed to be stored in hash tables within ResourceOwnerData, where NULL values in the `kind` field indicate free slots available for reuse. This design allows efficient storage and lookup of tracked resources.

## Parameters / Member Variables
- `item`: A Datum containing the actual resource reference (typically a pointer or integer identifier)
- `kind`: Pointer to a ResourceOwnerDesc that defines the callbacks and metadata for this type of resource; NULL indicates an unused hash table slot

## Dependencies
- Functions called/Symbols referenced:
  - [ResourceOwnerDesc](ResourceOwnerDesc.md) (struct type for resource kind metadata)
- Called from (representative examples):
  - [ResourceOwnerData](ResourceOwnerData.md) (as array member for resource storage)
  - [resource_priority_cmp](../r/resource_priority_cmp.md) (for sorting during resource release)
  - [ResourceOwnerSort](ResourceOwnerSort.md) (for organizing resources by priority)
  - [ResourceOwnerReleaseAll](ResourceOwnerReleaseAll.md) (during resource cleanup)
  - [ResourceOwnerEnlarge](ResourceOwnerEnlarge.md) (when expanding resource arrays)

## Notes and Other Information
- The Datum type allows ResourceElem to handle various resource types uniformly, as most PostgreSQL resources can be represented as pointers or integers
- The `kind` field being NULL serves a dual purpose: indicating free slots and providing type safety
- [ResourceElem](ResourceElem.md) instances are typically stored in arrays within ResourceOwnerData structures for efficient bulk operations
- The design supports priority-based resource release ordering through the associated ResourceOwnerDesc metadata