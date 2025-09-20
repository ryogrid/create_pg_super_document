# array_dims

## Location
[src/backend/utils/adt/arrayfuncs.c:1668-1705](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arrayfuncs.c#L1668-L1705)

## Overview
Returns a text representation of the dimensions and bounds of a PostgreSQL array in the format [lower:upper] for each dimension.

## Definition
```c
Datum array_dims(PG_FUNCTION_ARGS)
```

## Detailed Description
The `array_dims` function examines a PostgreSQL array and returns a text string describing its dimensional structure. For each dimension, it provides the lower and upper bounds in the format "[lower:upper]". For multi-dimensional arrays, multiple bound pairs are concatenated together. The function performs sanity checks on the input array and returns NULL for invalid arrays.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_ANY_ARRAY_P` - macro to extract array argument
  - `AARR_NDIM` - macro to get number of dimensions from array header
  - `AARR_DIMS` - macro to get dimension sizes from array header
  - `AARR_LBOUND` - macro to get lower bounds from array header
  - `AnyArrayType` - generic array type structure
  - `MAXDIM` - maximum allowed array dimensions constant
  - `cstring_to_text` - function to convert C string to PostgreSQL text type
  - `PG_RETURN_TEXT_P` - macro to return text result
  - `PG_RETURN_NULL` - macro to return NULL value
- Called from (representative examples):
  - SQL queries using `array_dims()` function
  - Array introspection routines

## Notes and Other Information
- Returns text in format "[lower:upper]" for single dimension, "[l1:u1][l2:u2]..." for multi-dimensional
- Uses a buffer of size MAXDIM * 33 + 1 to accommodate dimension strings
- Buffer sizing assumes 15 digits per number plus formatting characters
- Returns NULL for arrays with invalid dimension counts (≤ 0 or > MAXDIM)
- Part of PostgreSQL's array introspection function suite
- Defined in src/backend/utils/adt/arrayfuncs.c:1668-1705