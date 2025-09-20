# arraycontains

## Location
[src/backend/utils/adt/arrayfuncs.c:4530-4547](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L4530-L4547)

## Overview
This function determines whether one array is contained within (is a subset of) another array by checking if all elements of the second array exist in the first array.

## Definition

```c
Datum
arraycontains(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function implements the '@>' operator for arrays in PostgreSQL. It takes two arrays as input and returns a boolean indicating whether the first array contains all elements of the second array. The function delegates the actual comparison logic to  with parameters configured for containment checking. The function handles memory management by freeing any toasted input arrays to prevent memory leaks.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  -  (argument 0): The array that potentially contains the elements (left operand of '@>')
  -  (argument 1): The array whose elements are being checked for containment (right operand of '@>')
  - Collation information for element comparison

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts array arguments from function call
  - : Gets collation information for string comparisons
  - : Core comparison function that performs the containment check
  - : Frees memory for toasted arrays
  - : Returns boolean result to PostgreSQL
- Called from (representative examples):
  - SQL expressions using the '@>' operator
  - Array containment operations in queries

## Notes and Other Information
- This function implements the '@>' containment operator for arrays
- The order of arguments to  is reversed (array2, array1) to implement the containment semantics correctly
- The function includes memory management to handle toasted (compressed/external) arrays
- Uses collation-aware comparison for elements that support collation (like text)
- Returns true if array1 contains all elements of array2, false otherwise