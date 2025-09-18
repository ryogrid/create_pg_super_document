# bit_bit_count

## Location
src/backend/utils/adt/varbit.c: 1211 - 1222

## Overview
Implements the SQL BIT_COUNT() function that returns the number of bits set to '1' in a bit string.

## Definition
```c
Datum bit_bit_count(PG_FUNCTION_ARGS)
```

## Detailed Description
The `bit_bit_count` function is a PostgreSQL built-in function that implements the SQL BIT_COUNT() operation for bit strings. It counts the number of bits set to '1' in the input bit string and returns this count as a 64-bit integer.

The function uses the highly optimized `pg_popcount` function to perform the actual bit counting operation. Population count (popcount) is a fundamental bit manipulation operation that counts the number of set bits in a binary representation. The function operates directly on the raw bit data of the VarBit structure for maximum efficiency.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS[0]`: Input bit string (VarBit*) - the bit string to count set bits in
- `arg`: Local variable - extracted VarBit pointer from function arguments

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_VARBIT_P (argument extraction macro)
  - PG_RETURN_INT64 (return value macro)
  - pg_popcount (optimized population count function)
  - VARBITS (macro to get raw bit data from VarBit)
  - VARBITBYTES (macro to get byte length of VarBit data)
- Called from (representative examples):
  - No direct callers found (called via PostgreSQL function dispatch system)

## Notes and Other Information
- Located in src/backend/utils/adt/varbit.c:1211-1222
- This is a PostgreSQL built-in function accessible via SQL as BIT_COUNT()
- Uses highly optimized hardware-accelerated bit counting when available
- Returns count as int64 to handle very large bit strings
- Operates on the entire bit string including any padding bits in the final byte
- The pg_popcount function provides optimal performance across different CPU architectures