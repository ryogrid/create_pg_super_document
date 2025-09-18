# pg_column_toast_chunk_id

## Location
src/backend/utils/adt/varlena.c: 5114 - 5161

## Overview
Returns the chunk ID (va_valueid) of an on-disk TOASTed value, or NULL if the value is not TOASTed or not stored on disk.

## Definition
```c
Datum pg_column_toast_chunk_id(PG_FUNCTION_ARGS)
```

## Detailed Description
The `pg_column_toast_chunk_id` function extracts the chunk ID from a TOASTed (The Oversized-Attribute Storage Technique) varlena value that is stored on disk. This function:
1. Validates that the input is a varlena type (typlen = -1)
2. Checks if the attribute is externally stored on disk using `VARATT_IS_EXTERNAL_ONDISK`
3. Extracts the TOAST pointer information using `VARATT_EXTERNAL_GET_POINTER`
4. Returns the `va_valueid` field, which serves as the chunk identifier for the TOASTed data

This function is useful for database administrators and developers who need to inspect TOAST storage internals.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure
  - Argument 0: A potentially TOASTed datum whose chunk ID is to be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - [get_fn_expr_argtype](../g/get_fn_expr_argtype.md): Get the OID of the argument's data type
  - [get_typlen](../g/get_typlen.md): Get the storage length of a data type
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md): Allocate memory for caching type information
  - [DatumGetPointer](../D/DatumGetPointer.md): Convert datum to pointer
  - `VARATT_IS_EXTERNAL_ONDISK`: Macro to check if attribute is stored on disk
  - `VARATT_EXTERNAL_GET_POINTER`: Macro to extract external pointer information
  - `PG_RETURN_OID`: Macro to return an OID result
  - `PG_RETURN_NULL`: Macro to return NULL
- Types referenced:
  - `struct varlena`: Variable-length data structure
  - `struct varatt_external`: External attribute pointer structure
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL function interface)

## Notes and Other Information
- Located in `src/backend/utils/adt/varlena.c:5114-5161`
- This function is exposed as the SQL function `pg_column_toast_chunk_id()` for TOAST inspection
- Only works with varlena types; returns NULL for fixed-length types
- Returns NULL for values that are not TOASTed or are stored inline/compressed but not externally
- The returned chunk ID corresponds to the `va_valueid` field in the TOAST pointer structure
- This ID can be used to locate the actual TOAST chunks in the associated TOAST table
- Useful for debugging TOAST storage issues and understanding PostgreSQL's large object storage mechanism
- Part of PostgreSQL's suite of functions for inspecting storage internals