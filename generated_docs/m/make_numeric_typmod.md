# make_numeric_typmod

## Location
src/backend/utils/adt/numeric.c: 905 - 913

## Overview
A static inline function that packs numeric precision and scale values into a single typmod value for PostgreSQL's NUMERIC data type.

## Definition


## Detailed Description
This function creates a typmod (type modifier) value by encoding both precision and scale into a single 32-bit integer. The encoding scheme uses:
- Upper 16 bits: precision value (though not all bits are needed since max precision is 1000)
- Lower 11 bits: scale value (constrained to range [-1000, 1000])
- Remaining 5 bits in the lower 16: unused, reserved for future use
- VARHDRSZ is added to the result for historical reasons

The function ensures the result doesn't overflow to a negative int32, as negative values are interpreted as invalid typmod values by other parts of the system.

## Parameters / Member Variables
- : The precision value to encode (maximum digits)
- : The scale value to encode (digits after decimal point, range [-1000, 1000])

## Dependencies
- Functions called/Symbols referenced:
  - VARHDRSZ (constant)
- Called from (representative examples):
  - [numerictypmodin](../n/numerictypmodin.md) (at src/backend/utils/adt/numeric.c:1343)
  - [numerictypmodin](../n/numerictypmodin.md) (at src/backend/utils/adt/numeric.c:1353)

## Notes and Other Information
- This is a static inline function, meaning it's only visible within the numeric.c compilation unit
- The encoding uses bitwise operations for efficiency: left shift for precision, bitwise AND with 0x7ff mask for scale
- The 0x7ff mask (2047 in decimal) allows for the scale range of [-1000, 1000] in the lower 11 bits
- VARHDRSZ addition is for historical compatibility reasons and affects the available space in upper bits