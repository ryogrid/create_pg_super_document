# ArrayIteratorData

## Location
src/backend/utils/adt/arrayfuncs.c: 68 - 89

## Overview
ArrayIteratorData is a private structure that maintains the working state for array iteration operations in PostgreSQL. It provides efficient sequential access to array elements with support for both complete array traversal and array slicing.

## Definition


## Detailed Description
ArrayIteratorData is the internal working structure used by PostgreSQL's array iteration mechanism. It encapsulates all necessary information for efficiently traversing arrays, including support for both complete array iteration and array slicing operations. The structure is designed to maintain state across multiple function calls during array processing, allowing for memory-efficient streaming of array elements.

The structure is divided into three logical sections: basic array metadata established during initialization, slice-specific configuration for partial array operations, and dynamic position tracking that updates during iteration. This design enables both simple element-by-element iteration and more complex slicing operations while maintaining optimal performance characteristics.

## Parameters / Member Variables
**Basic Array Information:**
- : Pointer to the ArrayType being iterated through
- : Bitmap indicating which array elements are NULL, if any exist
- : Total count of elements in the array
- : Length of the array's element type (-1 for variable-length types)
- : Boolean indicating if the element type is passed by value
- : Alignment requirement for the element type

**Slice Configuration:**
- : Number of dimensions for slicing operation (0 indicates no slicing)
- : Number of elements per slice when slicing is active
- : Array of slice dimensions
- : Array of lower bounds for each slice dimension
- : Workspace array for storing slice element values
- : Workspace array for storing slice element null flags

**Current Position State:**
- : Pointer to current position within the array's data
- : Zero-based index of current element being processed

## Dependencies
- Functions called/Symbols referenced:
  - bits8 (for null bitmap handling)
  - ArrayType (array structure definition)
  - Datum (PostgreSQL data value type)

- Called from (representative examples):
  - array_create_iterator (creates and initializes iterator instances)
  - ArrayIterator (typedef pointer to this structure)

## Notes and Other Information
- This structure is declared as private within arrayfuncs.c and is not exposed in header files
- External code accesses this structure through the ArrayIterator typedef, which is a pointer to ArrayIteratorData
- The slice-related members are only populated when performing array slicing operations; they remain unused for simple iteration
- Memory management for workspace arrays (slice_values, slice_nulls) is handled by the array iteration framework
- The structure is designed to support PostgreSQL's set-returning function (SRF) protocol for streaming results