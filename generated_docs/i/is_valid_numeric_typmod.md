# is_valid_numeric_typmod

## Location
[src/backend/utils/adt/numeric.c:914-924](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L914-L924)

## Overview
A static inline function that validates whether a given typmod value is valid for PostgreSQL's NUMERIC data type.

## Definition
```c
static inline bool is_valid_numeric_typmod(int32 typmod)
```

## Detailed Description
This function checks if a typmod value is valid by ensuring it meets the minimum threshold. Due to the historical addition of VARHDRSZ in the make_numeric_typmod function, all valid numeric typmods must be at least VARHDRSZ in value. This simple validation helps distinguish between valid typmod values and special markers like -1 that indicate "no typmod specified".

## Parameters / Member Variables
- `typmod`: The typmod value to validate (int32)

## Dependencies
- Functions called/Symbols referenced:
  - VARHDRSZ (constant)
- Called from (representative examples):
  - [numeric_maximum_size](../n/numeric_maximum_size.md) (at src/backend/utils/adt/numeric.c:956)
  - [numeric_support](../n/numeric_support.md) (at src/backend/utils/adt/numeric.c:1226, 1227)
  - [numeric](../n/numeric.md) (at src/backend/utils/adt/numeric.c:1270)
  - [numerictypmodout](../n/numerictypmodout.md) (at src/backend/utils/adt/numeric.c:1372)
  - [apply_typmod](../a/apply_typmod.md) (at src/backend/utils/adt/numeric.c:7932)
  - [apply_typmod_special](../a/apply_typmod_special.md) (at src/backend/utils/adt/numeric.c:8025)

## Notes and Other Information
- This is a static inline function, meaning it's only visible within the numeric.c compilation unit
- The function returns true if the typmod is valid, false otherwise
- The VARHDRSZ offset requirement is a consequence of the encoding scheme used in make_numeric_typmod
- Invalid typmods (like -1) are commonly used to indicate that no type modifier was specified
- This validation is used throughout the numeric type system before attempting to extract precision and scale values