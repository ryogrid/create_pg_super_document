# pg_column_compression

## Location
src/backend/utils/adt/varlena.c: 5061 - 5113

## Overview
Returns the name of the compression method used for a compressed varlena attribute, or NULL for uncompressed data or non-varlena types.

## Definition
```c
Datum pg_column_compression(PG_FUNCTION_ARGS)
```

## Detailed Description
The `pg_column_compression` function examines a varlena (variable-length) datum to determine what compression method was used to store it. The function:
1. Checks if the input type is varlena (typlen = -1)
2. Extracts the compression ID from the TOAST header using `toast_get_compression_id`
3. Maps the compression ID to its corresponding method name ("pglz" or "lz4")
4. Returns NULL for non-varlena types or uncompressed data

Like `pg_column_size`, it caches the type length information for efficiency.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure
  - Argument 0: A datum whose compression method is to be determined

## Dependencies
- Functions called/Symbols referenced:
  - [get_fn_expr_argtype](../g/get_fn_expr_argtype.md): Get the OID of the argument's data type
  - [get_typlen](../g/get_typlen.md): Get the storage length of a data type
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md): Allocate memory for caching type information
  - [toast_get_compression_id](../t/toast_get_compression_id.md): Extract compression ID from varlena header
  - [DatumGetPointer](../D/DatumGetPointer.md): Convert datum to pointer
  - `cstring_to_text`: Convert C string to PostgreSQL text type
  - `PG_RETURN_TEXT_P`: Macro to return a text result
  - `PG_RETURN_NULL`: Macro to return NULL
- Types/Constants referenced:
  - `ToastCompressionId`: Enumeration for compression method IDs
  - `TOAST_INVALID_COMPRESSION_ID`: Invalid compression ID constant
  - `TOAST_PGLZ_COMPRESSION_ID`: PGLZ compression method ID
  - `TOAST_LZ4_COMPRESSION_ID`: LZ4 compression method ID
  - `struct varlena`: Variable-length data structure
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL function interface)

## Notes and Other Information
- Located in `src/backend/utils/adt/varlena.c:5061-5113`
- This function is exposed as the SQL function `pg_column_compression()` for inspecting compression methods
- Only works with varlena (variable-length) data types; returns NULL for fixed-length types
- Supports PostgreSQL's two main compression methods: PGLZ (traditional) and LZ4 (newer, faster)
- Useful for database administrators to analyze storage efficiency and compression effectiveness
- Returns NULL for uncompressed varlena data or when compression information is not available