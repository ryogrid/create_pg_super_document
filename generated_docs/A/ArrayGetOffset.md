# ArrayGetOffset

## Location
src/backend/utils/adt/arrayutils.c: 32 - 56

## Overview
Converts a multidimensional array subscript list into a linear element offset for array element access calculations.

## Definition


## Detailed Description
ArrayGetOffset calculates the linear offset (0-based index) for accessing an element in a multidimensional array stored in row-major order. The function performs the mathematical conversion from n-dimensional subscripts to a single linear index by multiplying each dimension's offset by the cumulative size of all subsequent dimensions.

The algorithm iterates backwards through the dimensions, calculating the offset contribution of each dimension and accumulating the scale factor. This approach efficiently handles arbitrary numbers of dimensions while maintaining the correct row-major ordering expected by PostgreSQL's array storage format.

The function assumes that all input parameters have been validated by the caller - specifically that dimensions and subscripts are within valid ranges to prevent arithmetic overflow.

## Parameters / Member Variables
- : Number of dimensions in the array
- : Array of dimension sizes for each dimension
- : Array of lower bound values for each dimension 
- : Array of subscript indices to convert to linear offset

## Dependencies
- Functions called/Symbols referenced:
  - (No external functions called)
- Called from (representative examples):
  - array_get_element
  - array_get_element_expanded
  - array_set_element
  - array_set_element_expanded
  - array_slice_size
  - array_extract_slice
  - array_insert_slice

## Notes and Other Information
- Assumes caller has performed range checking on dimensions and subscripts to prevent overflow
- Uses row-major ordering consistent with PostgreSQL's internal array representation  
- The algorithm processes dimensions in reverse order for efficiency in the row-major layout
- Critical utility function for all array element access operations in PostgreSQL