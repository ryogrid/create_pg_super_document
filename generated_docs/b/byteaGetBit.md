# byteaGetBit

## Location
[src/backend/utils/adt/varlena.c:3238-3275](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L3238-L3275)

## Overview
Extracts a specific bit from a bytea (binary string) data type and returns it as an integer value (0 or 1).

## Definition
```c
Datum byteaGetBit(PG_FUNCTION_ARGS)
```

## Detailed Description
This function treats a PostgreSQL bytea value as an array of bits and retrieves the Nth bit at the specified index position. The function performs bounds checking to ensure the bit index is within valid range (0 to length*8-1) and returns the bit value as either 0 or 1. It calculates the byte position and bit position within that byte, then uses bit manipulation to extract the specific bit.

## Parameters / Member Variables
- `PG_GETARG_BYTEA_PP(0)`: The input bytea value from which to extract a bit
- `PG_GETARG_INT64(1)`: The zero-based bit index position to retrieve

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BYTEA_PP (macro to get bytea argument)
  - PG_GETARG_INT64 (macro to get int64 argument)
  - VARSIZE_ANY_EXHDR (macro to get variable-length data size excluding header)
  - VARDATA_ANY (macro to get pointer to variable-length data)
  - ereport (error reporting function)
  - PG_RETURN_INT32 (macro to return int32 value)
- Called from (representative examples):
  - No direct references found in the indexed codebase

## Notes and Other Information
- Performs strict bounds checking, throwing ERRCODE_ARRAY_SUBSCRIPT_ERROR if bit index is out of range
- Uses zero-based bit indexing where bit 0 is the least significant bit of the first byte
- Returns only 0 or 1 representing the bit state
- Calculates byte position as n/8 and bit position within byte as n%8
- Uses bit masking (1 << bitNo) to extract the specific bit
- Part of PostgreSQL's bytea data type manipulation functions in varlena.c
- Located in src/backend/utils/adt/varlena.c:3238-3275

## Simplified Source

```c
// Extract a specific bit from bytea at given bit index (0-based)
Datum byteaGetBit(PG_FUNCTION_ARGS) {
    // Extract arguments: bytea value and bit index position
    bytea *input_bytea = PG_GETARG_BYTEA_PP(0);
    int64 bit_index = PG_GETARG_INT64(1);

    // Get the length of the bytea data (excluding header)
    int length = VARSIZE_ANY_EXHDR(input_bytea);

    // Validate bit index bounds (total bits = length * 8)
    if (bit_index < 0 || bit_index >= (int64) length * 8) {
        ereport(ERROR,
                (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                 errmsg("index %lld out of valid range, 0..%lld",
                        (long long) bit_index, (long long) length * 8 - 1)));
    }

    // Calculate byte position and bit position within that byte
    int byte_pos = (int) (bit_index / 8);
    int bit_pos = (int) (bit_index % 8);

    // Extract the target byte and check the specific bit
    int target_byte = ((unsigned char *) VARDATA_ANY(input_bytea))[byte_pos];

    // Return 1 if bit is set, 0 otherwise
    if (target_byte & (1 << bit_pos))
        return PG_RETURN_INT32(1);
    else
        return PG_RETURN_INT32(0);
}
```