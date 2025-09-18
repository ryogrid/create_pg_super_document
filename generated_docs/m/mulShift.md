# mulShift

## Location
[src/common/f2s.c:120-161](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/f2s.c#L120-L161)

## Overview
Performs a high-precision multiplication followed by a right bit shift, using 128-bit intermediate arithmetic to maintain precision during floating-point conversion calculations.

## Definition


## Detailed Description
This function multiplies a 64-bit value  by a multi-precision number represented as an array  and then shifts the result right by  bits. The implementation uses 128-bit arithmetic to handle the intermediate calculations without precision loss. It performs two 128-bit multiplications: one with  and another with , then combines the results to form a 128-bit intermediate value before applying the right shift.

The function includes overflow detection and handling when summing the high part of the first multiplication with the low part of the second multiplication. This is critical for maintaining numerical accuracy in decimal-to-string conversion algorithms where precise arithmetic is essential.

## Parameters / Member Variables
- : The 64-bit multiplicand (maximum 55 bits as noted in comment)
- : Pointer to an array of 64-bit values representing a multi-precision multiplier
- : The shift amount (result is shifted right by j-64 bits)

## Dependencies
- Functions called/Symbols referenced:
  - : Performs 128-bit multiplication of two 64-bit values
  - : Right-shifts a 128-bit value represented as two 64-bit parts
- Called from (representative examples):
  -  (multiple calls in src/common/d2s.c:211, 212, 213)
  -  (in src/common/f2s.c:164)
  -  (in src/common/f2s.c:170)

## Notes and Other Information
- Function is marked as  for performance optimization
- Input  is constrained to maximum 55 bits according to the implementation comment
- Uses overflow detection with increment of  when sum overflows
- The comments (128, 64, 0) appear to indicate bit positions or significance levels
- Critical component in PostgreSQL's high-precision floating-point to decimal conversion
- Located in src/common/d2s.c:182-207