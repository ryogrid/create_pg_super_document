# arraycontained

## Location
src/backend/utils/adt/arrayfuncs.c: 4548 - 4584

## Overview
This function determines whether one array is contained within (is a subset of) another array by checking if all elements of the first array exist in the second array.

## Definition


## Detailed Description
The  function implements the '<@' operator for arrays in PostgreSQL. It takes two arrays as input and returns a boolean indicating whether the first array is contained within the second array (i.e., all elements of the first array exist in the second array). The function delegates the actual comparison logic to  with the arrays in their natural order for containment checking. Like its counterpart , it handles memory management by freeing any toasted input arrays.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  -  (argument 0): The array being checked for containment (left operand of '<@')
  -  (argument 1): The array that potentially contains the elements (right operand of '<@')
  - Collation information for element comparison

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts array arguments from function call
  - : Gets collation information for string comparisons
  - : Core comparison function that performs the containment check
  - : Frees memory for toasted arrays
  - : Returns boolean result to PostgreSQL
- Called from (representative examples):
  - SQL expressions using the '<@' operator
  - Array containment operations in queries

## Notes and Other Information
- This function implements the '<@' contained-by operator for arrays
- The arguments are passed to  in their natural order (array1, array2) unlike 
- Complementary to the  function -  is equivalent to 
- The function includes memory management to handle toasted (compressed/external) arrays
- Uses collation-aware comparison for elements that support collation (like text)
- Returns true if all elements of array1 exist in array2, false otherwise