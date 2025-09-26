# floating_decimal_64

## Location
[src/common/d2s.c:339-343](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/d2s.c#L339-L343)

## Overview
A data structure representing a floating-point number in decimal form as mantissa * 10^exponent, used internally by the Ryu algorithm for efficient double-to-string conversion.

## Definition

```c
typedef struct floating_decimal_64
{
	uint64		mantissa;
	int32		exponent;
} floating_decimal_64;
```
## Detailed Description
The  struct is a core component of PostgreSQL's implementation of the Ryu floating-point output algorithm. It represents a floating-point number in decimal form where the actual value equals . This intermediate representation is used during the conversion of IEEE 754 double-precision floating-point numbers to their shortest decimal string representation.

The struct is part of the Ryu algorithm, which is a fast algorithm for converting floating-point numbers to decimal strings. The algorithm works by first converting the binary floating-point representation to this decimal floating-point representation, then formatting the result as a string.

This representation allows the algorithm to work with decimal arithmetic instead of binary arithmetic during the final stages of conversion, which simplifies the process of generating the correct decimal output with the minimum number of digits needed to uniquely represent the original floating-point value.

## Parameters / Member Variables
- `mantissa`: A 64-bit unsigned integer representing the significant digits of the decimal number
- `exponent`: A 32-bit signed integer representing the power of 10 by which the mantissa should be multiplied
## Dependencies
- Functions called/Symbols referenced: None (this is a data structure definition)
- Used by:
  -  (at src/common/d2s.c:623)
  -  (at src/common/d2s.c:631)  
  -  (at src/common/d2s.c:787)
  -  (at src/common/d2s.c:964)
  -  (at src/common/d2s.c:1034)

## Notes and Other Information
- This struct is defined in src/common/d2s.c, which implements the Ryu algorithm for double-precision floating-point to string conversion
- The Ryu algorithm is known for being both fast and producing the shortest possible decimal representation
- The implementation is based on code from github.com/ulfjack/ryu under the Boost license
- The struct serves as an intermediate representation between the binary IEEE 754 format and the final decimal string output
- The mantissa field holds the significant decimal digits, while the exponent field determines the decimal point placement
- This representation is particularly useful because it separates the significant digits from the scaling factor, making decimal formatting operations more straightforward