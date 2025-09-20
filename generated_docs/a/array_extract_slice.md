# array_extract_slice

## Location
[src/backend/utils/adt/arrayfuncs.c:5085-5157](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L5085-L5157)

## Overview
Extracts a slice of a multidimensional array into consecutive elements in a destination array, performing the actual data and null bitmap copying for array slice operations.

## Definition

```c
structed.  destArray must already
 * have been allocated and its header initialized.
 *
 * st[]/endp[] identify the slice to be replaced.  Elements within the slice
 * volume are taken from consecutive elements of the srcArray;
```
## Detailed Description
This function performs the core logic for extracting a slice from a multidimensional array. It iterates through the specified slice coordinates, copying each element from the source array to consecutive positions in the destination array. The function handles both the data copying (using ) and null bitmap copying (using ) to ensure complete slice extraction. It uses PostgreSQL's multidimensional array navigation utilities to efficiently traverse the slice boundaries and skip unwanted elements. The destination array is assumed to be properly allocated and initialized by the caller.

## Parameters
- : Destination ArrayType structure (must be properly allocated and initialized)
- : Number of dimensions in the source array
- : Array of dimension sizes for the source array
- : Array of lower bounds for each dimension in the source array
- : Pointer to the start of the source array's data portion
- : Pointer to the source array's null bitmap, or NULL if no nulls
- : Array of starting coordinates for the slice (inclusive)
- : Array of ending coordinates for the slice (inclusive)
- : Storage length of the array element datatype (-1 for variable-length types)
- : Whether the array element datatype is passed by value
- : Alignment requirement of the array element datatype

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to get pointer to array data portion
  - : Macro to get pointer to array null bitmap
  - : Converts multidimensional coordinates to linear offset
  - : Advances pointer over specified number of array elements
  - : Computes products array for multidimensional indexing
  - : Computes the span of the slice in each dimension
  - : Computes offset increments for efficient traversal
  - : Copies individual array elements from source to destination
  - : Copies null bitmap bits from source to destination
  - : Advances to the next tuple in multidimensional iteration
  - : Type used for null bitmap representation
  - : Maximum number of array dimensions supported
- Called from:
  - : Used as the core implementation for array slice extraction

## Notes and Other Information
- This is a static function internal to arrayfuncs.c
- The function assumes that the caller has verified slice coordinates are valid
- Assumes the destination array has been properly allocated with sufficient storage
- Assumes the destination array header has been properly initialized
- Uses element-by-element copying to handle variable-length types and null values correctly
- Efficiently skips unwanted elements using the multidimensional navigation system
- Maintains proper null bitmap information in the destination array
- The iteration pattern ensures that slice elements are copied to consecutive positions in the destination
- Critical component of PostgreSQL's array slicing functionality, providing the low-level extraction logic