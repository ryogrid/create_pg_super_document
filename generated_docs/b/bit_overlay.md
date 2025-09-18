# bit_overlay

## Location
src/backend/utils/adt/varbit.c: 1176 - 1210

## Overview
Internal static function that implements the core logic for bit string overlay operations, replacing a specified substring with another bit string.

## Definition
```c
static VarBit *bit_overlay(VarBit *t1, VarBit *t2, int sp, int sl)
```

## Detailed Description
The `bit_overlay` function is the internal implementation of the SQL OVERLAY() operation for bit strings. It takes two bit strings and two integers representing the start position and length, then returns a new bit string where the specified substring of the first string has been replaced with the second string.

The function implements the SQL standard's definition of OVERLAY() using substring and concatenation operations. It creates the result by: 1) extracting the prefix before the replacement position, 2) extracting the suffix after the replacement region, and 3) concatenating the prefix, replacement string, and suffix together.

The function includes robust error checking for integer overflow conditions and validates that the start position is positive, as required by the SQL standard.

## Parameters / Member Variables
- `t1`: First bit string (VarBit*) - the target string to be modified
- `t2`: Second bit string (VarBit*) - the replacement string
- `sp`: Start position (int) - 1-based position where replacement begins
- `sl`: Substring length (int) - length of substring to replace
- `result`: Local VarBit* - the final result string
- `s1`: Local VarBit* - prefix substring before replacement position
- `s2`: Local VarBit* - suffix substring after replacement region
- `sp_pl_sl`: Local int - calculated end position with overflow checking

## Dependencies
- Functions called/Symbols referenced:
  - ereport (error reporting function)
  - [pg_add_s32_overflow](../p/pg_add_s32_overflow.md) (safe integer addition with overflow detection)
  - [bitsubstring](bitsubstring.md) (extracts substring from bit string)
  - [bit_catenate](bit_catenate.md) (concatenates two bit strings)
- Called from (representative examples):
  - [bitoverlay](bitoverlay.md) (public SQL function wrapper)
  - [bitoverlay_no_len](bitoverlay_no_len.md) (public SQL function wrapper with default length)

## Notes and Other Information
- Located in src/backend/utils/adt/varbit.c:1176-1210
- This is a static internal function, not directly callable from SQL
- Implements comprehensive error checking for invalid positions and integer overflow
- Uses bitsubstring to extract prefix (characters 1 to sp-1) and suffix (characters sp+sl onward)
- Follows the SQL standard's definition of OVERLAY() precisely
- Error messages follow PostgreSQL conventions for substring and numeric range errors