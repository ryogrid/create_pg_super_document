# array_fill_internal

## Location
[src/backend/utils/adt/arrayfuncs.c:6073-6241](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L6073-L6241)

## Overview
The core implementation function for creating filled PostgreSQL arrays, handling all the complex memory allocation, type management, and data population logic for both null and non-null filled arrays.

## Definition

```c
struct_empty_array(elmtype);
```
## Detailed Description
This is the workhorse function behind PostgreSQL's array_fill functionality. It performs comprehensive validation of input parameters, calculates memory requirements, manages element type metadata caching, and constructs arrays filled with a specified value. The function handles both NULL and non-NULL fill values, supports multi-dimensional arrays with custom or default lower bounds, and includes extensive error checking for edge cases and resource limits.

The function implements sophisticated optimizations including element type metadata caching between calls, overflow detection for large arrays, and efficient memory layout for both dense (non-null) and sparse (null) array representations. It leverages the create_array_envelope helper for structure creation and various PostgreSQL array utilities for bounds checking and size calculations.

## Parameters / Member Variables
Function parameters:
- `dims`: ArrayType containing dimension sizes (must be 1-dimensional int array)
- `lbs`: ArrayType containing lower bounds for each dimension (can be NULL for default bounds of 1)
- `value`: Datum value to fill the array with
- `isnull`: Boolean indicating if the fill value is NULL
- `elmtype`: OID of the element type
- `fcinfo`: Function call information structure for metadata caching

Key internal variables:
- `dimv`: Pointer to dimension size array data
- `lbsv`: Pointer to lower bounds array data (or default bounds)
- `ndims`: Number of array dimensions
- `nitems`: Total number of elements in the array
- `deflbs`: Default lower bounds array (all 1s) used when `lbs` is NULL
- `my_extra`: Cached element type metadata for performance optimization

## Dependencies
- Functions called/Symbols referenced:
  - [array_contains_nulls](array_contains_nulls.md) (validates no nulls in dimension/bounds arrays)
  - ArrayGetNItems (calculates total element count)
  - ArrayCheckBounds (validates array bounds)
  - [construct_empty_array](../c/construct_empty_array.md) (handles zero-element case)
  - [get_typlenbyvalalign](../g/get_typlenbyvalalign.md) (retrieves element type properties)
  - [create_array_envelope](../c/create_array_envelope.md) (creates array structure)
  - ArrayCastAndSet (populates array elements)
  - PG_DETOAST_DATUM (ensures data is not compressed)
  - att_addlength_datum, att_align_nominal (calculates aligned element sizes)
- Called from (representative examples):
  - [array_fill_with_lower_bounds](array_fill_with_lower_bounds.md)
  - [array_fill](array_fill.md)

## Notes and Other Information
- Static function providing internal implementation for public array_fill functions
- Implements comprehensive parameter validation with specific error codes and messages
- Supports arrays up to MAXDIM dimensions with overflow detection
- Uses function-local caching (`fn_extra`) to avoid repeated element type lookups
- Handles both dense arrays (non-null values) and sparse arrays (null values) efficiently
- For non-null arrays: allocates contiguous data space and populates each element
- For null arrays: creates array with null bitmap, all elements marked as NULL
- Includes protection against integer overflow and memory allocation limits
- Located in src/backend/utils/adt/arrayfuncs.c at lines 6073-6241
- Critical component of PostgreSQL's array construction infrastructure
- Demonstrates sophisticated memory management and type system integration
- Provides foundation for SQL functions array_fill(value, dims) and array_fill(value, dims, lbs)