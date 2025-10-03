# JsonbIteratorNext

## Location
[src/backend/utils/adt/jsonb_util.c:852-1004](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_util.c#L852-L1004)

## Overview
Advances a JsonbIterator to the next token in a JSONB structure, handling complex nested containers with automatic memory management and state tracking.

## Definition

```c
JsonbIteratorToken
JsonbIteratorNext(JsonbIterator **it, JsonbValue *val, bool skipNested)
```
## Detailed Description
JsonbIteratorNext is the core iteration function for traversing JSONB structures in PostgreSQL. It implements a state machine that processes different types of JSONB tokens including arrays, objects, keys, values, and scalars. The function automatically handles recursion into nested containers by creating child iterators and manages their lifecycle to prevent memory leaks in highly nested structures.

The function operates through several states (JBI_ARRAY_START, JBI_ARRAY_ELEM, JBI_OBJECT_START, JBI_OBJECT_KEY, JBI_OBJECT_VALUE) and returns corresponding tokens (WJB_BEGIN_ARRAY, WJB_ELEM, WJB_BEGIN_OBJECT, WJB_KEY, WJB_VALUE, WJB_END_ARRAY, WJB_END_OBJECT, WJB_DONE). When encountering nested containers, it automatically creates child iterators and recurses into them unless skipNested is true.

The function includes sophisticated memory management that automatically frees child iterators to prevent excessive memory usage during deep traversal, though callers who end iteration early may need to manually walk the ancestral tree to free remaining allocated iterators.

## Parameters / Member Variables
- `**it`: Double pointer to JsonbIterator, may be replaced with child iterator during traversal
- `*val`: Pointer to JsonbValue structure to be filled with current token's value
- `skipNested`: Boolean flag to skip recursion into nested containers
## Dependencies
- Functions called/Symbols referenced:
  - [freeAndGetParent](../f/freeAndGetParent.md)
  - [fillJsonbValue](../f/fillJsonbValue.md)
  - JBE_ADVANCE_OFFSET
  - IsAJsonbScalar
  - [iteratorFromContainer](../i/iteratorFromContainer.md)
  - [getJsonbOffset](../g/getJsonbOffset.md)
  - jbvNull, jbvArray, jbvObject, jbvString
  - WJB_DONE, WJB_BEGIN_ARRAY, WJB_END_ARRAY, WJB_ELEM, WJB_BEGIN_OBJECT, WJB_END_OBJECT, WJB_KEY, WJB_VALUE
  - JBI_ARRAY_START, JBI_ARRAY_ELEM, JBI_OBJECT_START, JBI_OBJECT_KEY, JBI_OBJECT_VALUE
- Called from (representative examples):
  - [JsonbToCStringWorker](JsonbToCStringWorker.md)
  - [JsonbDeepContains](JsonbDeepContains.md)
  - [jsonb_hash](../j/jsonb_hash.md)
  - [compareJsonbContainers](../c/compareJsonbContainers.md)
  - [pushJsonbValue](../p/pushJsonbValue.md)
  - [each_worker_jsonb](../e/each_worker_jsonb.md)
  - [populate_array_dim_jsonb](../p/populate_array_dim_jsonb.md)
  - [IteratorConcat](../I/IteratorConcat.md)
  - [setPath](../s/setPath.md) functions

## Notes and Other Information
- Implements a state machine for JSONB traversal with automatic memory management
- Handles recursive descent into nested structures via child iterator creation
- Callers should not handle jbvBinary values directly as the function expands them automatically
- Array/Object element/pair buffers have garbage pointers and should not be accessed directly
- val->type is set to jbvNull for non-meaningful tokens (WJB_DONE, WJB_END_ARRAY, WJB_END_OBJECT)
- Critical for memory management in nested structures - automatically frees child iterators
- Early termination scenarios may require manual cleanup of ancestral iterator chain
- Used extensively throughout PostgreSQL's JSONB implementation for all traversal operations

## Simplified Source

```c
JsonbIteratorToken
JsonbIteratorNext(JsonbIterator **it, JsonbValue *val, bool skipNested)
{
    if (*it == NULL) {
        val->type = jbvNull;
        return WJB_DONE;
    }

recurse:
    switch ((*it)->state) {
        case JBI_ARRAY_START:
            // Begin array iteration
            val->type = jbvArray;
            val->val.array.nElems = (*it)->nElems;
            (*it)->curIndex = 0;
            (*it)->state = JBI_ARRAY_ELEM;
            return WJB_BEGIN_ARRAY;

        case JBI_ARRAY_ELEM:
            // Process array elements
            if ((*it)->curIndex >= (*it)->nElems) {
                *it = freeAndGetParent(*it);
                val->type = jbvNull;
                return WJB_END_ARRAY;
            }

            fillJsonbValue((*it)->container, (*it)->curIndex,
                          (*it)->dataProper, (*it)->curDataOffset, val);
            (*it)->curIndex++;

            // Recurse into nested containers if needed
            if (!IsAJsonbScalar(val) && !skipNested) {
                *it = iteratorFromContainer(val->val.binary.data, *it);
                goto recurse;
            }
            return WJB_ELEM;

        case JBI_OBJECT_START:
            // Begin object iteration
            val->type = jbvObject;
            val->val.object.nPairs = (*it)->nElems;
            (*it)->curIndex = 0;
            (*it)->state = JBI_OBJECT_KEY;
            return WJB_BEGIN_OBJECT;

        case JBI_OBJECT_KEY:
            // Process object keys
            if ((*it)->curIndex >= (*it)->nElems) {
                *it = freeAndGetParent(*it);
                val->type = jbvNull;
                return WJB_END_OBJECT;
            }

            fillJsonbValue((*it)->container, (*it)->curIndex,
                          (*it)->dataProper, (*it)->curDataOffset, val);
            (*it)->state = JBI_OBJECT_VALUE;
            return WJB_KEY;

        case JBI_OBJECT_VALUE:
            // Process object values
            (*it)->state = JBI_OBJECT_KEY;
            fillJsonbValue((*it)->container, (*it)->curIndex + (*it)->nElems,
                          (*it)->dataProper, (*it)->curValueOffset, val);
            (*it)->curIndex++;

            // Recurse into nested containers if needed
            if (!IsAJsonbScalar(val) && !skipNested) {
                *it = iteratorFromContainer(val->val.binary.data, *it);
                goto recurse;
            }
            return WJB_VALUE;
    }

    // Should never reach here
    val->type = jbvNull;
    return WJB_DONE;
}
```