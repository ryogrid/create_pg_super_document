# pg_column_size

## Location
[src/backend/utils/adt/varlena.c:5014-5060](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L5014-L5060)

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
  - [get_fn_expr_argtype](../g/get_fn_expr_argtype.md): Get the OID of the argument's data type
  - [get_typlen](../g/get_typlen.md): Get the storage length of a data type
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md): Allocate memory for caching type information
  - [toast_datum_size](../t/toast_datum_size.md): Calculate size of potentially TOAST-ed varlena data
  - [DatumGetCString](../D/DatumGetCString.md): Convert datum to C string
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

## Simplified Source

```c
Datum
pg_column_size(PG_FUNCTION_ARGS)
{
    Datum value = PG_GETARG_DATUM(0);
    int32 result;
    int typlen;

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

    // Calculate size based on type characteristics
    if (typlen == -1) {
        // Variable-length type, possibly TOASTed/compressed
        result = toast_datum_size(value);
    } else if (typlen == -2) {
        // C-string type: length + null terminator
        result = strlen(DatumGetCString(value)) + 1;
    } else {
        // Fixed-width type: use type's defined length
        result = typlen;
    }

    PG_RETURN_INT32(result);
}
```