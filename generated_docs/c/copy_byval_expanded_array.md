# copy_byval_expanded_array

## Location
[src/backend/utils/adt/array_expanded.c:185-232](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/array_expanded.c#L185-L232)

## Overview
A helper function for expand_array() that efficiently copies the deconstructed representation from one expanded array to another for pass-by-value element types.

## Definition


## Detailed Description
This static helper function is specifically designed to optimize the copying of expanded arrays when elements are pass-by-value types and a deconstructed Datum-array representation already exists. Rather than reconstructing the array from a flat representation, it directly copies the metadata and Datum/isnull arrays from the source expanded array to the destination.

The function performs a deep copy of all array metadata including dimensions, bounds, element type information, and the deconstructed representation (dvalues and dnulls arrays). It allocates new memory in the destination array's context for all copied data structures, ensuring proper memory management separation between source and destination arrays.

## Parameters / Member Variables
- : Pointer to the destination ExpandedArrayHeader that will receive the copied data
- : Pointer to the source ExpandedArrayHeader to copy from

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
  - memcpy
- Called from (representative examples):
  - [expand_array](../e/expand_array.md)

## Notes and Other Information
- This is a static function internal to array_expanded.c, used only as an optimization path in expand_array()
- Only used when source array has pass-by-value elements and an existing deconstructed representation
- Allocates dimension arrays (dims and lbound) together in a single allocation for efficiency
- The destination array will have no flat representation (fvalue, fstartptr, fendptr are set to NULL)
- Handles the optional dnulls array correctly - only allocates and copies if it exists in the source