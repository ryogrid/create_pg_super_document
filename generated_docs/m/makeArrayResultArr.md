# makeArrayResultArr

## Location
src/backend/utils/adt/arrayfuncs.c: 5691 - 5769

## Overview
Produces the final N+1-dimensional array result from an accumulated ArrayBuildStateArr structure.

## Definition


## Detailed Description
This function is the final step in the array-from-arrays building process. It constructs the final ArrayType result from the accumulated data in the ArrayBuildStateArr. The function handles both empty array cases and normal construction, properly managing memory layout including data offset calculations for null bitmaps.

The function performs bounds checking on the array dimensions, computes the required space including overhead for null bitmaps if present, and constructs a properly formatted ArrayType structure. It copies all accumulated data, dimensions, bounds, and null bitmap information into the final result array.

## Parameters / Member Variables
- : Working ArrayBuildStateArr containing accumulated data (must not be NULL)
- : Memory context where the result array should be constructed
- : Whether it's safe to release/delete the working state memory context

## Dependencies
- Functions called/Symbols referenced:
  - construct_empty_array
  - ArrayGetNItems
  - ArrayCheckBounds
  - array_bitmap_copy
  - MemoryContextSwitchTo
  - MemoryContextDelete
  - SET_VARSIZE
  - ARR_OVERHEAD_WITHNULLS, ARR_OVERHEAD_NONULLS
  - ARR_DIMS, ARR_LBOUND, ARR_DATA_PTR, ARR_NULLBITMAP
- Called from (representative examples):
  - array_agg_array_finalfn
  - makeArrayResultAny

## Notes and Other Information
- **Empty array handling**: Returns a proper empty array if no inputs were accumulated (ndims == 0)
- **Memory management**: Properly calculates data offset based on presence of null bitmap
- **Bounds checking**: Validates array dimensions don't cause overflow using ArrayCheckBounds
- **Cleanup**: If release=true and astate was created with its own context, deletes the working context
- **Array structure**: Constructs a complete ArrayType with proper VARSIZE, dimensions, bounds, and data
- **Null bitmap**: Copies null bitmap if present, calculating proper overhead
- Part of the three-function API: initArrayResultArr/accumArrayResultArr/makeArrayResultArr
- The resulting array has N+1 dimensions where N is the dimensionality of the input sub-arrays