# array_contain_compare

## Location
src/backend/utils/adt/arrayfuncs.c: 4369 - 4511

## Overview
Internal function that implements array overlap and containment comparisons by checking if elements from one array exist in another array based on configurable matching criteria.

## Definition


## Detailed Description
The  function provides the core logic for array overlap and containment operations. It compares two arrays element-by-element to determine either overlap (any elements in common) or containment (all elements of one array exist in another), depending on the  parameter.

When  is true, the function returns true only if all elements of array1 are found in array2 (containment). When  is false, it returns true if any element of array1 is found in array2 (overlap). The function optimizes performance by deconstructing array2 into separate values and nulls arrays for efficient multiple scans, while iterating through array1 using the array iterator interface.

The function handles NULL values by treating them as non-matchable - NULL elements cannot match anything, including other NULLs, which differs from the behavior in .

## Parameters / Member Variables
- : First array to compare (source array for containment/overlap check)
- : Second array to compare (target array to search within)
- : Collation OID for element comparisons
- : Boolean flag controlling comparison mode:
  - : All elements of array1 must be in array2 (containment)
  - : Any element of array1 must be in array2 (overlap)
- : Pointer to cached type information for performance optimization

## Dependencies
- Functions called/Symbols referenced:
  -  - Get array element type
  -  - Get cached equality operator information
  -  - Check if array is in expanded format
  -  - Extract elements from expanded array
  -  - Extract elements from regular array format
  -  - Calculate total number of elements in array1
  -  /  - Get array dimensions for element count
  -  - [Initialize](../I/Initialize.md) iterator for array1
  -  - Get next element from array1
  -  - Set up function call for equality operator
  -  - Call equality operator on element pairs
  -  - Extract boolean result from equality comparison

- Called from (representative examples):
  -  - Array overlap operator (&& operator)
  -  - Array contains operator (@> operator) 
  -  - Array contained by operator (<@ operator)

## Notes and Other Information
- Returns boolean result indicating whether the containment/overlap condition is met
- Requires arrays to have the same element type; raises error for type mismatches
- Uses type cache to avoid repeated equality operator lookups for performance
- Optimizes array2 access by deconstructing it once into values/nulls arrays
- Handles expanded array format efficiently for better performance with large arrays
- NULL elements are treated as non-matchable (different from array equality semantics)
- Uses strict equality operators, so comparison results are never NULL
- Performance is O(n*m) where n and m are the number of elements in each array
- Early termination when result can be determined (first match for overlap, first non-match for containment)