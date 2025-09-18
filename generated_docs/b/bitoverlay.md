# bitoverlay

## Location
src/backend/utils/adt/varbit.c: 1153 - 1163

## Overview
Implements the SQL OVERLAY() function for bit strings, replacing a specified substring of the first bit string with a second bit string.

## Definition
```c
Datum bitoverlay(PG_FUNCTION_ARGS)
```

## Detailed Description
The `bitoverlay` function is a PostgreSQL built-in function that implements the SQL standard OVERLAY() operation for bit strings. It takes four arguments: two bit strings and two integers representing the start position and length of the substring to replace. The function replaces the specified substring of the first bit string with the entire second bit string.

This function serves as a wrapper around the internal `bit_overlay` function, handling argument extraction from PostgreSQL's function call interface and returning the result in the proper format. The SQL standard defines OVERLAY() in terms of substring and concatenation operations, which this implementation follows directly.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS[0]`: First bit string (VarBit*) - the target string to be modified
- `PG_FUNCTION_ARGS[1]`: Second bit string (VarBit*) - the replacement string  
- `PG_FUNCTION_ARGS[2]`: Start position (int32) - 1-based position where replacement begins
- `PG_FUNCTION_ARGS[3]`: Substring length (int32) - length of substring to replace

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_VARBIT_P (argument extraction macro)
  - PG_GETARG_INT32 (argument extraction macro)
  - [bit_overlay](bit_overlay.md) (internal implementation function)
  - PG_RETURN_VARBIT_P (return value macro)
- Called from (representative examples):
  - No direct callers found (called via PostgreSQL function dispatch system)

## Notes and Other Information
- Located in src/backend/utils/adt/varbit.c:1153-1163
- This is a PostgreSQL built-in function accessible via SQL
- Error handling and bounds checking are performed in the internal bit_overlay function
- The function follows PostgreSQL's standard pattern for built-in functions using PG_FUNCTION_ARGS