# ArrayIterator

## Location
src/include/utils/array.h: 258 - 260

## Overview
ArrayIterator is an opaque pointer type used for efficient iteration through PostgreSQL arrays, supporting both element-by-element and slice-based iteration patterns.

## Definition


## Detailed Description
ArrayIterator provides a high-level interface for iterating through PostgreSQL arrays in a memory-efficient manner. The actual implementation details are encapsulated in the ArrayIteratorData structure, which is private to arrayfuncs.c. This design supports two iteration modes: element-by-element iteration where individual datums of the array's element type are returned, and slice-based iteration where sub-arrays of the original array type are returned.

The iterator maintains state about the current position, type information, and slicing parameters. It supports arrays with null elements and handles proper memory alignment for different data types. The iterator must be created using array_create_iterator(), used with array_iterate(), and freed with array_free_iterator().

## Parameters / Member Variables
Since ArrayIterator is an opaque pointer to ArrayIteratorData, the actual members are private, but the underlying structure contains:
- : Pointer to the array being iterated
- : Null bitmap of the array, if any
- : Total number of elements in the array
- : Element type's length property
- : Element type's pass-by-value property
- : Element type's alignment property
- : Slice dimension (0 for element iteration)
- : Number of elements per slice
- : Dimensions array for slices
- : Lower bounds array for slices
- : Workspace for building slice values
- : Workspace for slice null indicators
- : Current position pointer in array data
- : Current item number being processed

## Dependencies
- Functions called/Symbols referenced:
  - [ArrayIteratorData](ArrayIteratorData.md)
- Called from (representative examples):
  - [array_create_iterator](../a/array_create_iterator.md)
  - [array_iterate](../a/array_iterate.md)
  - [array_free_iterator](../a/array_free_iterator.md)
  - [array_position_common](../a/array_position_common.md)
  - [array_positions](../a/array_positions.md)
  - [arraycontained](../a/arraycontained.md)

## Notes and Other Information
- The iterator supports both scalar (element-by-element) and slice-based iteration modes
- Proper memory management requires calling array_free_iterator() when done
- The passed-in array must remain valid for the lifetime of the iterator
- Slice iteration returns sub-arrays of the same type as the original array
- Used extensively in PostgreSQL's array manipulation functions
- Provides efficient traversal without loading entire arrays into memory
- Located in src/include/utils/array.h:258-260 with implementation in src/backend/utils/adt/arrayfuncs.c