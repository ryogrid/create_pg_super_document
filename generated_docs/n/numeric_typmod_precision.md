# numeric_typmod_precision

## Location
[src/backend/utils/adt/numeric.c:925-939](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L925-L939)

## Overview
A static inline function that extracts the precision value from a numeric typmod, reversing the encoding performed by make_numeric_typmod.

## Definition
```c
static inline int numeric_typmod_precision(int32 typmod)
```

## Detailed Description
This function decodes the precision component from a typmod value by reversing the encoding scheme used in make_numeric_typmod. It first subtracts VARHDRSZ to remove the historical offset, then right-shifts by 16 bits to move the precision from the upper 16 bits to the lower bits, and finally applies a 0xffff mask to ensure only the lower 16 bits are retained.

## Parameters / Member Variables
- `typmod`: The encoded typmod value from which to extract precision

## Dependencies
- Functions called/Symbols referenced:
  - VARHDRSZ (constant)
- Called from (representative examples):
  - [numeric_maximum_size](numeric_maximum_size.md) (at src/backend/utils/adt/numeric.c:960)
  - [numeric_support](numeric_support.md) (at src/backend/utils/adt/numeric.c:1216, 1217)
  - [numeric](numeric.md) (at src/backend/utils/adt/numeric.c:1276)
  - [numerictypmodout](numerictypmodout.md) (at src/backend/utils/adt/numeric.c:1374)
  - [apply_typmod](../a/apply_typmod.md) (at src/backend/utils/adt/numeric.c:7935)
  - [apply_typmod_special](../a/apply_typmod_special.md) (at src/backend/utils/adt/numeric.c:8028)

## Notes and Other Information
- This is a static inline function, meaning it's only visible within the numeric.c compilation unit
- The function performs the inverse operation of the precision encoding in make_numeric_typmod
- The 0xffff mask ensures the result is constrained to 16 bits (0-65535 range)
- Should only be called on typmods that have been validated with is_valid_numeric_typmod
- The maximum precision for NUMERIC type is 1000, so most of the 16-bit range is unused

## Simplified Source

```c
static inline int numeric_typmod_precision(int32 typmod) {
    // Extract precision from typmod: remove header offset,
    // shift to get upper 16 bits, mask to 16-bit value
    return ((typmod - VARHDRSZ) >> 16) & 0xffff;
}
```