# array_in

## Location
[src/backend/utils/adt/arrayfuncs.c:179-401](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L179-L401)

## Overview
Converts an array from its external string format to the internal PostgreSQL ArrayType representation, serving as the input conversion function for PostgreSQL array types.

## Definition


## Detailed Description
The  function is the primary input conversion function for PostgreSQL arrays. It parses a string representation of an array (e.g., "{1,2,3}" or "[1:3]={1,2,3}") and converts it into PostgreSQL's internal ArrayType structure. The function handles multi-dimensional arrays with optional explicit dimension specifications and lower bounds.

The function implements a sophisticated parsing strategy that:
1. Caches element type metadata (ArrayMetaState) for performance across multiple calls
2. Parses optional dimension information using ReadArrayDimensions
3. Parses array values using ReadArrayStr 
4. Constructs the final ArrayType structure with proper memory layout
5. Handles null values, data alignment, and size validation

The parsing supports two formats:
- Simple format: "{val1,val2,val3}"
- Explicit dimensions: "[lower:upper]={val1,val2,val3}"

## Parameters / Member Variables
- : External string representation of the array to parse
- : OID of the array's element type
- : Type modifier for array elements
- : Error context for soft error handling

## Dependencies
- Functions called/Symbols referenced:
  - [ReadArrayDimensions](../R/ReadArrayDimensions.md)
  - [ReadArrayStr](../R/ReadArrayStr.md)  
  - [get_type_io_data](../g/get_type_io_data.md)
  - [construct_empty_array](../c/construct_empty_array.md)
  - [CopyArrayEls](../C/CopyArrayEls.md)
  - ARR_OVERHEAD_WITHNULLS/ARR_OVERHEAD_NONULLS
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
- Called from (representative examples):
  - [extract_variadic_args](../e/extract_variadic_args.md)
  - [CATALOG](../C/CATALOG.md) (pg_type.h)

## Notes and Other Information
- Uses ArrayMetaState caching to optimize repeated calls with the same element type
- Supports arrays up to MAXDIM dimensions
- Handles both NULL values and variable-length elements properly
- Performs extensive validation including size overflow checks
- The function is registered in the PostgreSQL type system as the input function for array types
- Memory layout follows PostgreSQL's ArrayType format with optional null bitmap