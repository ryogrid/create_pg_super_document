# numeric_typmod_scale

## Location
[src/backend/utils/adt/numeric.c:940-950](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L940-L950)

## Overview
A static inline function that extracts the scale value from a numeric typmod, handling sign extension for negative scale values.

## Definition
```c
static inline int numeric_typmod_scale(int32 typmod)
```

## Detailed Description
This function decodes the scale component from a typmod value by reversing the encoding scheme used in make_numeric_typmod. Since scale values can be negative (range [-1000, 1000]), the function must perform sign extension when unpacking the 11-bit two's complement representation. It uses a bit manipulation technique: (x^1024)-1024 to properly sign-extend the 11-bit value stored in the lower bits of the typmod.

## Parameters / Member Variables
- `typmod`: The encoded typmod value from which to extract scale

## Dependencies
- Functions called/Symbols referenced:
  - VARHDRSZ (constant)
- Called from (representative examples):
  - [numeric_support](numeric_support.md) (at src/backend/utils/adt/numeric.c:1214, 1215)
  - [numeric](numeric.md) (at src/backend/utils/adt/numeric.c:1277)
  - [numerictypmodout](numerictypmodout.md) (at src/backend/utils/adt/numeric.c:1375)
  - [apply_typmod](../a/apply_typmod.md) (at src/backend/utils/adt/numeric.c:7936)
  - [apply_typmod_special](../a/apply_typmod_special.md) (at src/backend/utils/adt/numeric.c:8029)

## Notes and Other Information
- This is a static inline function, meaning it's only visible within the numeric.c compilation unit
- The function performs the inverse operation of the scale encoding in make_numeric_typmod
- The 0x7ff mask extracts the lower 11 bits containing the scale value
- The bit hack (x^1024)-1024 correctly handles sign extension for negative scales in two's complement format
- Should only be called on typmods that have been validated with is_valid_numeric_typmod
- Scale values are constrained to the range [-1000, 1000] in PostgreSQL's NUMERIC type