# byteaGetBit

## Location
src/backend/utils/adt/varlena.c: 3238 - 3275

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