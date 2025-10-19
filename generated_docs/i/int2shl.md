# int2shl

## Location
[src/backend/utils/adt/int.c:1482-1490](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int.c#L1482-L1490)

## Overview
Performs bitwise left shift operation on a 16-bit signed integer (smallint type in PostgreSQL).

## Definition
```c
Datum int2shl(PG_FUNCTION_ARGS)
```

## Detailed Description
The `int2shl` function implements the bitwise left shift operation for PostgreSQL's `smallint` data type (16-bit signed integers). It takes a `smallint` value and a shift count (as an `int32`), performs a left shift operation using the C `<<` operator, and returns the result as a `smallint` value. This function is typically invoked through PostgreSQL's SQL operator `<<` when used with `smallint` operands. The left shift operation effectively multiplies the value by 2^n where n is the shift count.

## Parameters / Member Variables
- `arg1`: The 16-bit signed integer value to be shifted, retrieved via `PG_GETARG_INT16(0)`
- `arg2`: The number of bit positions to shift left (32-bit signed integer), retrieved via `PG_GETARG_INT32(1)`

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_INT16`: Macro to extract int16 argument from function call context
  - `PG_GETARG_INT32`: Macro to extract int32 argument from function call context  
  - `PG_RETURN_INT16`: Macro to return int16 result from PostgreSQL function
- Called from (representative examples):
  - No direct references found (likely called through PostgreSQL's operator dispatch system)

## Notes and Other Information
- Located in `src/backend/utils/adt/int.c:1482-1490`
- Part of PostgreSQL's arithmetic and bitwise operations for integer types
- The shift count is taken as a 32-bit integer to allow for larger shift values
- The function follows PostgreSQL's standard function interface using `PG_FUNCTION_ARGS` and return macros
- Typically accessed through the SQL left shift operator `<<` rather than direct function calls
- No bounds checking on shift count - behavior with negative or very large shift counts follows C language semantics

## Simplified Source
```c
Datum int2shl(PG_FUNCTION_ARGS) {
    // Extract 16-bit integer value to shift
    int16 arg1 = PG_GETARG_INT16(0);
    // Extract 32-bit shift count
    int32 arg2 = PG_GETARG_INT32(1);

    // Perform left shift operation and return result
    PG_RETURN_INT16(arg1 << arg2);
}
```