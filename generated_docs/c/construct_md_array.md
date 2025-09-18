# construct_md_array

## Location
src/backend/utils/adt/arrayfuncs.c: 3482 - 3567

## Overview
Creates a multi-dimensional array object with arbitrary dimensions and support for NULL elements, serving as the core array construction function for complex array operations.

## Definition


## Detailed Description
The construct_md_array function provides the most flexible and comprehensive method for constructing PostgreSQL array objects. It supports arrays with arbitrary numbers of dimensions (up to MAXDIM), custom lower bounds for each dimension, and NULL element values. This function serves as the foundation for other array construction functions like construct_array.

The function performs extensive validation on the input parameters, calculates the required memory allocation size, handles data alignment and toasted values, and creates the complete ArrayType structure with proper headers, dimension information, and element data. It can create zero-dimensional arrays and handles overflow detection for very large arrays.

## Parameters / Member Variables
- : Array of Datum items that will become the contents of the constructed array
- : Array of boolean flags indicating which elements are NULL (can be NULL if no null elements)
- : Number of dimensions for the array (0 or positive, up to MAXDIM)
- : Integer array specifying the size of each dimension
- : Integer array specifying the lower bound of each dimension
- : OID of the data type for the array elements
- : Length of the element data type (-1 for variable-length types)
- : Boolean indicating whether elements are passed by value or by reference
- : Alignment requirement for the element data type

## Dependencies
- Functions called/Symbols referenced:
  - ArrayGetNItems
  - ArrayCheckBounds
  - construct_empty_array
  - PG_DETOAST_DATUM
  - att_addlength_datum
  - att_align_nominal
  - AllocSizeIsValid
  - CopyArrayEls
  - ARR_OVERHEAD_WITHNULLS
  - ARR_OVERHEAD_NONULLS
  - SET_VARSIZE
  - ARR_DIMS
  - ARR_LBOUND
  - MAXDIM (constant)
  - MaxAllocSize (constant)
- Called from (representative examples):
  - construct_array
  - strlist_to_textarray
  - ExecEvalArrayExpr
  - array_shuffle_n
  - array_set_element
  - array_set_slice
  - array_iterate
  - makeMdArrayResult
  - percentile_disc_multi_final
  - build_regexp_match_result

## Notes and Other Information
- Validates that ndims is between 0 and MAXDIM, raising errors for invalid ranges
- Automatically detours empty arrays (ndims <= 0 or any dimension is 0) to construct_empty_array
- Handles variable-length elements by detoasting them before storage
- Calculates proper memory alignment and detects potential overflow conditions
- Creates null bitmaps only when necessary (when hasnulls is true)
- The resulting array uses palloc0 for zero-initialized memory allocation
- Essential foundation function that other array construction utilities depend upon