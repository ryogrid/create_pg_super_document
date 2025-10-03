# iteratorFromContainer

## Location
[src/backend/utils/adt/jsonb_util.c:1005-1046](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_util.c#L1005-L1046)

## Overview
Creates and initializes a JsonbIterator structure for traversing elements within a specific JsonbContainer, setting up the appropriate state and data pointers based on container type.

## Definition

```c
static JsonbIterator *
iteratorFromContainer(JsonbContainer *container, JsonbIterator *parent)
```
## Detailed Description
iteratorFromContainer is an internal static function that constructs a new JsonbIterator for a given JsonbContainer. It determines the container type (array or object) by examining header flags and initializes the iterator's state machine accordingly. For arrays, it sets up data pointers to skip over the JEntry array and initializes array-specific state. For objects, it allocates space for both key and value JEntry arrays before setting up data pointers and object-specific state.

The function handles both regular arrays/objects and scalar containers, with special logic for scalar arrays that must contain exactly one element. It establishes parent-child relationships between iterators to support proper memory management and nested traversal.

## Parameters / Member Variables
- `*container`: Pointer to the JsonbContainer to iterate over
- `*parent`: Pointer to parent JsonbIterator (NULL for root level iterators)
## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md)
  - JsonContainerSize
  - JsonContainerIsScalar
  - JB_FARRAY, JB_FOBJECT (header flags)
  - JBI_ARRAY_START, JBI_OBJECT_START (iterator states)
  - JEntry
- Called from (representative examples):
  - [JsonbIteratorInit](../J/JsonbIteratorInit.md)
  - [JsonbIteratorNext](../J/JsonbIteratorNext.md)

## Notes and Other Information
- Static function - only used internally within jsonb_util.c
- Handles both array and object container types with different initialization logic
- Arrays: dataProper points after JEntry array (nElems * sizeof(JEntry) offset)
- Objects: dataProper points after double JEntry array (nElems * sizeof(JEntry) * 2 offset)
- Supports scalar arrays (isScalar flag) which must contain exactly one element
- Establishes parent-child iterator relationships for proper memory management
- Sets initial state to JBI_ARRAY_START or JBI_OBJECT_START based on container type
- Critical for creating child iterators during recursive descent in JsonbIteratorNext

## Simplified Source

```c
static JsonbIterator *
iteratorFromContainer(JsonbContainer *container, JsonbIterator *parent)
{
    JsonbIterator *it;

    // Allocate and initialize iterator
    it = palloc0(sizeof(JsonbIterator));
    it->container = container;
    it->parent = parent;
    it->nElems = JsonContainerSize(container);
    it->children = container->children;

    // Setup iterator based on container type
    if (container->header & JB_FARRAY) {
        // Array: data follows JEntry array
        it->dataProper = (char *) it->children + it->nElems * sizeof(JEntry);
        it->isScalar = JsonContainerIsScalar(container);
        it->state = JBI_ARRAY_START;
    } else if (container->header & JB_FOBJECT) {
        // Object: data follows double JEntry array (keys + values)
        it->dataProper = (char *) it->children + it->nElems * sizeof(JEntry) * 2;
        it->state = JBI_OBJECT_START;
    } else {
        elog(ERROR, "unknown type of jsonb container");
    }

    return it;
}
```