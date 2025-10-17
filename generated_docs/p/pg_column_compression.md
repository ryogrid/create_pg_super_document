# pg_column_compression

## Location
[src/backend/utils/adt/varlena.c:5061-5113](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L5061-L5113)

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
  - `[cstring_to_text](../c/cstring_to_text.md)`: Convert C string to PostgreSQL text type
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

## Simplified Source

```c
Datum
pg_column_compression(PG_FUNCTION_ARGS)
{
    int typlen;
    char *result;
    ToastCompressionId cmid;

    // Cache type length information for efficiency
    if (fcinfo->flinfo->fn_extra == NULL) {
        Oid argtypeid = get_fn_expr_argtype(fcinfo->flinfo, 0);
        typlen = get_typlen(argtypeid);

        if (typlen == 0)  // Sanity check
            elog(ERROR, "cache lookup failed for type %u", argtypeid);

        // Store typlen in function context for reuse
        fcinfo->flinfo->fn_extra = MemoryContextAlloc(fcinfo->flinfo->fn_mcxt, sizeof(int));
        *((int *) fcinfo->flinfo->fn_extra) = typlen;
    } else {
        typlen = *((int *) fcinfo->flinfo->fn_extra);
    }

    // Only varlena types can be compressed
    if (typlen != -1)
        PG_RETURN_NULL();

    // Get compression method ID from TOAST header
    cmid = toast_get_compression_id((struct varlena *) DatumGetPointer(PG_GETARG_DATUM(0)));
    if (cmid == TOAST_INVALID_COMPRESSION_ID)
        PG_RETURN_NULL();

    // Map compression ID to method name
    switch (cmid) {
        case TOAST_PGLZ_COMPRESSION_ID:
            result = "pglz";
            break;
        case TOAST_LZ4_COMPRESSION_ID:
            result = "lz4";
            break;
        default:
            elog(ERROR, "invalid compression method id %d", cmid);
    }

    PG_RETURN_TEXT_P(cstring_to_text(result));
}
```