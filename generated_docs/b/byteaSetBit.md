# byteaSetBit

## Location
src/backend/utils/adt/varlena.c: 3308 - 3358

## Overview
Creates a new bytea (binary string) instance with a specific bit at the given index position set to a new value (0 or 1).

## Definition
```c
Datum byteaSetBit(PG_FUNCTION_ARGS)
```

## Detailed Description
This function creates a copy of the input bytea value and modifies the Nth bit at the specified index position with a new bit value. The function performs bounds checking to ensure the bit index is within valid range (0 to length*8-1) and validates that the new bit value is either 0 or 1. It calculates the byte position and bit position within that byte, then uses bit manipulation operations to set or clear the specific bit while preserving other bits in the same byte.

## Parameters / Member Variables
- `PG_GETARG_BYTEA_P_COPY(0)`: The input bytea value to copy and modify
- `PG_GETARG_INT64(1)`: The zero-based bit index position to set
- `PG_GETARG_INT32(2)`: The new bit value (must be 0 or 1)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BYTEA_P_COPY (macro to get a copy of bytea argument)
  - PG_GETARG_INT64 (macro to get int64 argument)
  - PG_GETARG_INT32 (macro to get int32 argument)
  - VARSIZE (macro to get variable-length data total size)
  - VARHDRSZ (constant for variable-length header size)
  - VARDATA (macro to get pointer to variable-length data)
  - ereport (error reporting function)
  - PG_RETURN_BYTEA_P (macro to return bytea value)
- Called from (representative examples):
  - No direct references found in the indexed codebase

## Notes and Other Information
- Creates a copy of the input bytea to ensure immutability
- Performs strict bounds checking, throwing ERRCODE_ARRAY_SUBSCRIPT_ERROR if bit index is out of range
- Validates new bit value, throwing ERRCODE_INVALID_PARAMETER_VALUE if not 0 or 1
- Uses zero-based bit indexing where bit 0 is the least significant bit of the first byte
- Calculates byte position as n/8 and bit position within byte as n%8
- Uses bit masking operations: ~(1 << bitNo) to clear bit, (1 << bitNo) to set bit
- Preserves all other bits in the target byte when modifying a single bit
- Returns a new bytea instance with the modified bit value
- Part of PostgreSQL's bytea data type manipulation functions in varlena.c
- Located in src/backend/utils/adt/varlena.c:3308-3358