# array_iter_setup

## Location
[src/include/utils/arrayaccess.h:49-80](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/arrayaccess.h#L49-L80)

## Overview
Initializes an array_iter structure for sequential element access from either a flat or expanded PostgreSQL array, setting up appropriate pointers based on the array storage format.

## Definition
```c
static inline void
array_iter_setup(array_iter *it, AnyArrayType *a)
```

## Detailed Description
The `array_iter_setup` function prepares an `array_iter` structure for iterating through elements of a PostgreSQL array. It handles both expanded arrays (which store elements as separate Datum values) and flat arrays (which store elements in a compact binary format). The function examines the array type and configures the iterator with the appropriate set of pointers:

- For expanded arrays with deconstructed values (dvalues), it sets up datumptr and isnullptr for direct access to Datum array and null flags
- For expanded arrays without deconstructed values, it works with the embedded flat array using dataptr and bitmapptr
- For regular flat arrays, it uses dataptr for the data area and bitmapptr for the null bitmap

The function ensures all iterator fields are properly initialized to prevent compiler warnings and sets the initial bitmask to 1 for null bitmap processing.

## Parameters / Member Variables
- `it`: Pointer to the array_iter structure to be initialized
- `a`: Pointer to the AnyArrayType array (either expanded or flat format)

## Dependencies
- Functions called/Symbols referenced:
  - VARATT_IS_EXPANDED_HEADER
  - ARR_DATA_PTR
  - ARR_NULLBITMAP
- Called from (representative examples):
  - array_out
  - array_send
  - array_map
  - array_eq
  - array_cmp
  - hash_array
  - hash_array_extended
  - array_contain_compare
  - array_unnest_fctx

## Notes and Other Information
- This is an inline function defined in arrayaccess.h for performance
- Must be called before using array_iter_next to iterate through array elements
- The function handles the complexity of PostgreSQL's dual array storage formats transparently
- The bitmask is initialized to 1, corresponding to the first bit in the null bitmap
- All pointer fields are explicitly set (including unused ones) to prevent compiler warnings
- Works with both regular ArrayType and expanded array representations through the AnyArrayType union