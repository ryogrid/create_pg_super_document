# iteratorFromContainer

## Location
src/backend/utils/adt/jsonb_util.c: 1005 - 1046

## Overview
Creates and initializes a JsonbIterator structure for traversing elements within a specific JsonbContainer, setting up the appropriate state and data pointers based on container type.

## Definition


## Detailed Description
iteratorFromContainer is an internal static function that constructs a new JsonbIterator for a given JsonbContainer. It determines the container type (array or object) by examining header flags and initializes the iterator's state machine accordingly. For arrays, it sets up data pointers to skip over the JEntry array and initializes array-specific state. For objects, it allocates space for both key and value JEntry arrays before setting up data pointers and object-specific state.

The function handles both regular arrays/objects and scalar containers, with special logic for scalar arrays that must contain exactly one element. It establishes parent-child relationships between iterators to support proper memory management and nested traversal.

## Parameters / Member Variables
- : Pointer to the JsonbContainer to iterate over
- : Pointer to parent JsonbIterator (NULL for root level iterators)

## Dependencies
- Functions called/Symbols referenced:
  - palloc0
  - JsonContainerSize
  - JsonContainerIsScalar
  - JB_FARRAY, JB_FOBJECT (header flags)
  - JBI_ARRAY_START, JBI_OBJECT_START (iterator states)
  - JEntry
- Called from (representative examples):
  - JsonbIteratorInit
  - JsonbIteratorNext

## Notes and Other Information
- Static function - only used internally within jsonb_util.c
- Handles both array and object container types with different initialization logic
- Arrays: dataProper points after JEntry array (nElems * sizeof(JEntry) offset)
- Objects: dataProper points after double JEntry array (nElems * sizeof(JEntry) * 2 offset)
- Supports scalar arrays (isScalar flag) which must contain exactly one element
- Establishes parent-child iterator relationships for proper memory management
- Sets initial state to JBI_ARRAY_START or JBI_OBJECT_START based on container type
- Critical for creating child iterators during recursive descent in JsonbIteratorNext