# pg_column_size

## Location
src/backend/utils/adt/varlena.c: 5014 - 5060

## Overview
Returns the storage size in bytes of a datum of any PostgreSQL data type, including compressed and TOAST-ed values.

## Definition
```c
Datum pg_column_size(PG_FUNCTION_ARGS)
```

## Detailed Description
The `pg_column_size` function calculates and returns the actual storage size of a given datum. It handles different PostgreSQL data types appropriately:
- For variable-length types (typlen = -1), it uses `toast_datum_size` to get the size including TOAST compression
- For C-string types (typlen = -2), it calculates the string length plus null terminator
- For fixed-width types, it returns the type's defined length

The function caches the input type's length information in `fn_extra` for efficiency on subsequent calls with the same function context.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure
  - Argument 0: A datum of any PostgreSQL data type whose size is to be measured

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_DATUM`: Macro to extract a datum argument
  - `get_fn_expr_argtype`: Get the OID of the argument's data type
  - `get_typlen`: Get the storage length of a data type
  - `MemoryContextAlloc`: Allocate memory for caching type information
  - `toast_datum_size`: Calculate size of potentially TOAST-ed varlena data
  - `DatumGetCString`: Convert datum to C string
  - `PG_RETURN_INT32`: Macro to return a 32-bit integer result
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL function interface)

## Notes and Other Information
- Located in `src/backend/utils/adt/varlena.c:5014-5060`
- This function is exposed as the SQL function `pg_column_size()` for measuring storage size
- The function handles all PostgreSQL data types uniformly through the type system
- Results include compression effects for TOAST-ed data
- Uses function context caching to avoid repeated type lookups for the same function call context
- Essential for storage analysis and database administration tasks