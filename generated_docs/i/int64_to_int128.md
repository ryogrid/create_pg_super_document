# int64_to_int128

## Location
src/include/common/int128.h: 84 - 93

## Overview
Converts (widens) a signed 64-bit integer to a 128-bit integer representation.

## Definition
```c
static inline INT128
int64_to_int128(int64 v)
```

## Detailed Description
This function performs a widening conversion from a signed 64-bit integer to a 128-bit integer. The conversion preserves the sign and value of the original integer by casting it to the INT128 type. For positive values, the high 64 bits of the result are zero; for negative values, the high 64 bits are filled with ones (sign extension). This is a fundamental conversion function used when 64-bit arithmetic needs to be extended to 128-bit precision.

## Parameters / Member Variables
- `v`: The signed 64-bit integer value to be converted to 128-bit representation.

## Dependencies
- Functions called/Symbols referenced:
  - INT128 (type definition)
- Called from (representative examples):
  - [interval_cmp_value](interval_cmp_value.md) (in src/backend/utils/adt/timestamp.c:2496)
  - [interval_sign](interval_sign.md) (in src/backend/utils/adt/timestamp.c:2517)

## Notes and Other Information
- This is a static inline function defined in the header file for optimal performance
- The function returns the converted value rather than modifying a parameter in-place
- Performs proper sign extension automatically for negative values
- Essential for mixed arithmetic operations between 64-bit and 128-bit integers
- Used primarily in PostgreSQL's timestamp and interval arithmetic where precision beyond 64 bits is required
- Companion function to int128_to_int64 which performs the reverse conversion
- Part of PostgreSQL's 128-bit integer arithmetic library for handling large numeric calculations