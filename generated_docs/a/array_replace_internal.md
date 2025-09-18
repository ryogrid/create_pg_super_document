# array_replace_internal

## Location
src/backend/utils/adt/arrayfuncs.c: 6369 - 6626

## Overview
An internal static function that provides the core implementation for both array_remove() and array_replace() operations by finding and either removing or replacing array elements that match a search value.

## Definition


## Detailed Description
The array_replace_internal function serves as the unified backend implementation for PostgreSQL's array_remove() and array_replace() SQL functions. It performs a comprehensive scan of an input array, using the element type's equality operator to find elements that match the search criteria. Based on the 'remove' parameter, matching elements are either deleted from the result array or replaced with the specified replacement value.

The function handles various complex scenarios including NULL values, multi-dimensional arrays (with restrictions for removal), proper memory management, and type-specific operations. It uses PostgreSQL's type cache system to efficiently look up and cache the equality operator for the array's element type across multiple function calls. The function also handles TOAST (The Oversized-Attribute Storage Technique) decompression for variable-length data types.

For remove operations on multi-dimensional arrays, the function raises an error since removing elements would break the rectangular structure requirement. The function optimizes performance by returning the original array unchanged when no modifications are needed.

## Parameters / Member Variables
- : The input ArrayType structure to process
- : The Datum value to search for within the array
- : Boolean indicating if the search value is NULL
- : The Datum value to use as replacement (ignored if removing)
- : Boolean indicating if the replacement value is NULL
- : Boolean flag determining operation mode (true=remove, false=replace)
- : OID of the collation to use for element comparisons
- : FunctionCallInfo structure for caching type information across calls

## Dependencies
- Functions called/Symbols referenced:
  - ARR_ELEMTYPE, ARR_NDIM, ARR_DIMS, ARR_DATA_PTR, ARR_NULLBITMAP
  - ArrayGetNItems
  - lookup_type_cache
  - PG_DETOAST_DATUM
  - InitFunctionCallInfoData
  - FunctionCallInvoke
  - fetch_att, att_addlength_datum, att_align_nominal
  - construct_empty_array
  - CopyArrayEls
  - AllocSizeIsValid
- Called from (representative examples):
  - array_remove (src/backend/utils/adt/arrayfuncs.c:6637)
  - array_replace (src/backend/utils/adt/arrayfuncs.c:6661)

## Notes and Other Information
- This is a static function, not directly accessible outside arrayfuncs.c
- Uses type cache to avoid repeated equality operator lookups for better performance
- Handles both regular and NULL comparisons with special case logic for NULL search values
- Supports TOAST decompression for variable-length data types (-1 typlen)
- Enforces rectangular array constraint by prohibiting removal from multi-dimensional arrays  
- Performs size overflow checking to prevent exceeding MaxAllocSize limits
- Returns original array unmodified when no changes are made for efficiency
- Maintains original array dimensions and bounds in the result (except first dimension for removals)