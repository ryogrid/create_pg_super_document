# bitoverlay_no_len

## Location
[src/backend/utils/adt/varbit.c:1164-1175](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varbit.c#L1164-L1175)

## Overview
Implements the SQL OVERLAY() function for bit strings without an explicit length parameter, using the length of the replacement string as the default length.

## Definition
```c
Datum bitoverlay_no_len(PG_FUNCTION_ARGS)
```

## Detailed Description
The `bitoverlay_no_len` function is a PostgreSQL built-in function that implements a variant of the SQL OVERLAY() operation for bit strings. Unlike the standard `bitoverlay` function, this version takes only three arguments: two bit strings and one integer representing the start position. The length of the substring to replace defaults to the length of the replacement bit string.

This function serves as a convenience wrapper around the internal `bit_overlay` function, automatically determining the appropriate length parameter by using `VARBITLEN(t2)` to get the length of the replacement string. This provides a more intuitive interface when you want to replace a substring with another string of known length.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS[0]`: First bit string (VarBit*) - the target string to be modified
- `PG_FUNCTION_ARGS[1]`: Second bit string (VarBit*) - the replacement string
- `PG_FUNCTION_ARGS[2]`: Start position (int32) - 1-based position where replacement begins
- `sl`: Local variable - calculated length of the replacement string using VARBITLEN

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_VARBIT_P (argument extraction macro)
  - PG_GETARG_INT32 (argument extraction macro)
  - VARBITLEN (macro to get bit string length)
  - [bit_overlay](bit_overlay.md) (internal implementation function)
  - PG_RETURN_VARBIT_P (return value macro)
- Called from (representative examples):
  - No direct callers found (called via PostgreSQL function dispatch system)

## Notes and Other Information
- Located in src/backend/utils/adt/varbit.c:1164-1175
- This is a PostgreSQL built-in function accessible via SQL
- Provides a more convenient interface than the full 4-parameter overlay function
- The replacement length is automatically determined from the replacement string
- Error handling and bounds checking are performed in the internal bit_overlay function