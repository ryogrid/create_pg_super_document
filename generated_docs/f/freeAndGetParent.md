# freeAndGetParent

## Location
[src/backend/utils/adt/jsonb_util.c:1047-1067](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_util.c#L1047-L1067)

## Overview
Helper function that frees the memory of a child JsonbIterator and returns its parent iterator, enabling proper cleanup during iterator traversal.

## Definition


## Detailed Description
freeAndGetParent is a simple but critical memory management utility function used internally by JsonbIteratorNext. When a child iterator has finished processing all elements in its container (array or object), this function ensures proper cleanup by freeing the child iterator's allocated memory and returning control to the parent iterator. This mechanism is essential for preventing memory leaks during traversal of nested JSONB structures.

The function implements the bottom-up cleanup pattern used in the JSONB iterator hierarchy, where child iterators are automatically freed when their processing is complete, allowing the parent iterator to continue processing at its level of nesting.

## Parameters / Member Variables
- : Pointer to the JsonbIterator to be freed

## Dependencies
- Functions called/Symbols referenced:
  - [pfree](../p/pfree.md)
  - JsonbIterator
- Called from (representative examples):
  - [JsonbIteratorNext](../J/JsonbIteratorNext.md) (when ending array processing)
  - [JsonbIteratorNext](../J/JsonbIteratorNext.md) (when ending object processing)

## Notes and Other Information
- Static function - only used internally within jsonb_util.c
- Simple but critical for memory management in nested iterator hierarchies
- Returns parent iterator to continue processing at the next higher nesting level
- Part of the automatic memory management system that prevents memory leaks during deep JSONB traversal
- Called when JBI_ARRAY_ELEM or JBI_OBJECT_KEY states detect all elements have been processed
- Essential component of the iterator cleanup mechanism described in JsonbIteratorNext documentation