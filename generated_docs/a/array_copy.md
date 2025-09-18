# array_copy

## Location
[src/backend/utils/adt/arrayfuncs.c:4924-4953](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L4924-L4953)

## Overview
Copies a specified number of array elements from a source memory location to a destination memory location.

## Definition


## Detailed Description
This function performs efficient copying of array elements from source to destination memory locations. It first calculates the total byte size of the elements to be copied using , then performs a bulk memory copy using . The function handles both fixed-length and variable-length data types and respects element alignment requirements. It's important to note that this function only copies the data elements themselves and does not handle the destination's null bitmap setup.

## Parameters
- : Starting destination memory location (caller must ensure sufficient space is allocated)
- : Number of array elements to copy (must be >= 0)
- : Starting location in the source array data
- : 0-based linear element number of the first element (the one at *srcptr)
- : Pointer to the start of the source array's null bitmap, or NULL if the array has no nulls
- : Storage length of the array element datatype (-1 for variable-length types)
- : Whether the array element datatype is passed by value
- : Alignment requirement of the array element datatype

## Dependencies
- Functions called/Symbols referenced:
  - : Used to calculate the total byte size of elements to copy
  - : Standard C library function for memory copying
  - : Type used for null bitmap representation
- Called from:
  - : Used for extracting slices from arrays
  - : Multiple calls for copying array segments during slice insertion operations

## Notes and Other Information
- This is a static function internal to arrayfuncs.c
- Returns the number of bytes copied, which can be useful for tracking copy operations
- The caller is responsible for ensuring the destination has sufficient space allocated
- **Important limitation**: This function does NOT handle setting up the destination's null bitmap - this must be done separately by the caller
- The function leverages  for efficient bulk copying after determining the exact size
- Used primarily in array slice operations where segments of arrays need to be copied efficiently