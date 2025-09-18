# byteaSetByte

## Location
[src/backend/utils/adt/varlena.c:3276-3307](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L3276-L3307)

## Overview
Creates a new bytea (binary string) instance with a specific byte at the given index position set to a new value.

## Definition
```c
Datum byteaSetByte(PG_FUNCTION_ARGS)
```

## Detailed Description
This function creates a copy of the input bytea value and modifies the Nth byte at the specified index position with a new byte value. The function performs bounds checking to ensure the index is within valid range and returns a new bytea instance rather than modifying the original. This follows PostgreSQL's immutable data approach where operations return new instances rather than modifying existing ones.

## Parameters / Member Variables
- `PG_GETARG_BYTEA_P_COPY(0)`: The input bytea value to copy and modify
- `PG_GETARG_INT32(1)`: The zero-based index position of the byte to set
- `PG_GETARG_INT32(2)`: The new byte value (0-255) to set at the specified position

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BYTEA_P_COPY (macro to get a copy of bytea argument)
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
- Performs strict bounds checking, throwing ERRCODE_ARRAY_SUBSCRIPT_ERROR if index is out of range
- Uses zero-based indexing (first byte is at index 0)
- The newByte parameter should be in range 0-255 but no explicit validation is performed
- Returns a new bytea instance with the modified byte value
- Part of PostgreSQL's bytea data type manipulation functions in varlena.c
- Located in src/backend/utils/adt/varlena.c:3276-3307