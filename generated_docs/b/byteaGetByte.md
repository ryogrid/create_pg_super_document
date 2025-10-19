# byteaGetByte

## Location
[src/backend/utils/adt/varlena.c:3209-3237](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L3209-L3237)

## Overview
Extracts a specific byte from a bytea (binary string) data type and returns it as an integer value (0-255).

## Definition

```c
Datum
byteaGetByte(PG_FUNCTION_ARGS)
```
## Detailed Description
This function treats a PostgreSQL bytea value as an array of bytes and retrieves the Nth byte at the specified index position. The function performs bounds checking to ensure the index is within valid range and returns the byte value as a 32-bit integer. It's part of PostgreSQL's variable-length data type handling system for binary data manipulation.

## Parameters / Member Variables
- : The input bytea value from which to extract a byte
- : The zero-based index position of the byte to retrieve

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BYTEA_PP (macro to get bytea argument)
  - PG_GETARG_INT32 (macro to get int32 argument)
  - VARSIZE_ANY_EXHDR (macro to get variable-length data size excluding header)
  - VARDATA_ANY (macro to get pointer to variable-length data)
  - ereport (error reporting function)
  - PG_RETURN_INT32 (macro to return int32 value)
- Called from (representative examples):
  - No direct references found in the indexed codebase

## Notes and Other Information
- Performs strict bounds checking, throwing ERRCODE_ARRAY_SUBSCRIPT_ERROR if index is out of range
- Returns byte values as unsigned integers in the range 0-255
- Uses zero-based indexing (first byte is at index 0)
- Part of PostgreSQL's bytea data type manipulation functions in varlena.c
- Located in src/backend/utils/adt/varlena.c:3209-3237

## Simplified Source

```c
// Extract a specific byte from bytea at given index (0-based)
Datum byteaGetByte(PG_FUNCTION_ARGS) {
    // Extract arguments: bytea value and index position
    bytea *input_bytea = PG_GETARG_BYTEA_PP(0);
    int32 index = PG_GETARG_INT32(1);

    // Get the length of the bytea data (excluding header)
    int length = VARSIZE_ANY_EXHDR(input_bytea);

    // Validate index bounds (0-based indexing)
    if (index < 0 || index >= length) {
        ereport(ERROR,
                (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                 errmsg("index %d out of valid range, 0..%d", index, length - 1)));
    }

    // Extract the byte at the specified index and return as integer
    int byte_value = ((unsigned char *) VARDATA_ANY(input_bytea))[index];

    return PG_RETURN_INT32(byte_value);
}
```