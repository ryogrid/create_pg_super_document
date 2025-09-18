# array_to_text_internal

## Location
[src/backend/utils/adt/varlena.c:4808-4929](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L4808-L4929)

## Overview
Core internal function that implements the array-to-text conversion logic shared by both array_to_text and array_to_text_null functions, handling element iteration, NULL value processing, and output formatting.

## Definition


## Detailed Description
This function performs the actual work of converting a PostgreSQL array to a concatenated text string. It handles multi-dimensional arrays by flattening them and processes each element through the appropriate output function for the element type. The function implements sophisticated caching of type metadata to avoid repeated lookups when processing multiple arrays of the same element type. It correctly handles NULL elements based on whether a null replacement string is provided, and manages proper memory alignment when traversing variable-length array elements. The function uses PostgreSQL's StringInfo buffer for efficient string concatenation and properly handles the null bitmap for sparse arrays.

## Parameters / Member Variables
- : Function call information structure containing context and caching capabilities
- : Pointer to the ArrayType structure representing the input array
- : C string containing the field separator to use between array elements
- : Optional C string to substitute for NULL array elements (NULL means skip NULLs)

## Dependencies
- Functions called/Symbols referenced:
  - ARR_NDIM, ARR_DIMS, ARR_ELEMTYPE (array metadata access macros)
  - ArrayGetNItems (calculates total number of elements)
  - cstring_to_text_with_len (converts C string to PostgreSQL text type)
  - [get_type_io_data](../g/get_type_io_data.md) (retrieves type metadata and output function)
  - [fmgr_info_cxt](../f/fmgr_info_cxt.md) (caches function call information)
  - ARR_DATA_PTR, ARR_NULLBITMAP (array data access macros)
  - fetch_att (extracts individual array elements with proper alignment)
  - [OutputFunctionCall](../O/OutputFunctionCall.md) (converts elements to string representation)
  - att_addlength_pointer, att_align_nominal (memory alignment utilities)
  - initStringInfo, appendStringInfo, appendStringInfoString (string buffer operations)
- Called from:
  - [array_to_text](array_to_text.md) (two-parameter version)
  - [array_to_text_null](array_to_text_null.md) (three-parameter version)
  - [concat_internal](../c/concat_internal.md) (used in string concatenation operations)

## Notes and Other Information
The function employs a sophisticated caching mechanism using ArrayMetaState stored in the function's extra space to avoid repeated type lookups when the same function is called multiple times with arrays of the same element type. This optimization is particularly important for performance in loops or bulk operations. The function handles both fixed-length and variable-length element types correctly, managing memory alignment requirements for each. The null bitmap processing allows efficient handling of sparse arrays where not all positions contain values. The function is marked static as it's an internal implementation detail shared between the public array_to_text functions.