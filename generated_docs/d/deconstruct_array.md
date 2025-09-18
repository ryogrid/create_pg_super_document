# deconstruct_array

## Location
src/backend/utils/adt/arrayfuncs.c: 3619 - 3684

## Overview
Extracts individual elements and null markers from a PostgreSQL array structure into separate Datum and boolean arrays for easier processing by C code.

## Definition


## Detailed Description
The  function is a fundamental utility for decomposing PostgreSQL array objects into their constituent elements. It extracts all array elements into a palloc'd array of Datum values and optionally creates a corresponding array of null indicators. The function handles the complex internal structure of PostgreSQL arrays, including null bitmaps, element alignment, and variable-length data types.

The function performs element-by-element extraction, properly handling null values through the array's null bitmap. For pass-by-reference data types, the returned Datums point directly into the original array object, making this an efficient operation that avoids unnecessary copying.

The caller is responsible for providing element type information (length, alignment, pass-by-value status) rather than the function looking it up from system catalogs. This design allows callers to cache type information across multiple calls for better performance.

## Parameters / Member Variables
- : The PostgreSQL array object to deconstruct (must not be NULL)
- : The OID of the array element data type (used for assertion checking)
- : Length of each array element (-1 for variable-length types)
- : Whether elements are passed by value (true) or by reference (false)
- : Alignment requirement for array elements ('c', 's', 'i', or 'd')
- : Output parameter, set to point to palloc'd array of Datum values
- : Output parameter, set to point to palloc'd array of null indicators (may be NULL if nulls not supported)
- : Output parameter, set to the number of elements extracted

## Dependencies
- Functions called/Symbols referenced:
  - ARR_ELEMTYPE (macro for getting array element type)
  - ArrayGetNItems (calculates total number of elements)
  - ARR_NDIM (macro for array dimensions)
  - ARR_DIMS (macro for dimension sizes)
  - ARR_DATA_PTR (macro for array data pointer)
  - ARR_NULLBITMAP (macro for null bitmap)
  - fetch_att (extracts attribute value)
  - att_addlength_pointer (advances pointer by attribute length)
  - att_align_nominal (aligns pointer to required boundary)
  - bits8 (type for bitmap manipulation)

- Called from (representative examples):
  - ginarrayextract (GIN index array extraction)
  - _bt_preprocess_array_keys (B-tree array key preprocessing)
  - ExecIndexEvalArrayKeys (executor array key evaluation)
  - array_set_slice (array slice assignment)
  - deconstruct_array_builtin (specialized version for built-in types)
  - array_contain_compare (array containment operations)
  - scalararraysel (selectivity estimation for scalar array operations)

## Notes and Other Information
- The function includes an assertion to verify that the provided element type matches the array's actual element type
- If  is NULL and a null element is encountered, the function throws an error with ERRCODE_NULL_VALUE_NOT_ALLOWED
- The null bitmap is processed bit by bit, with bitmask manipulation to check each element's null status
- Memory allocation uses palloc() for the elements array and palloc0() for the nulls array (zero-initialized)
- This function is performance-critical and used extensively throughout PostgreSQL for array processing operations
- The design assumes the caller has already validated the array and type information, focusing on efficient extraction rather than extensive error checking