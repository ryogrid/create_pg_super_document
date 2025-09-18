# floating_decimal_32

## Location
src/common/f2s.c: 215 - 219

## Overview
A structure representing a floating-point number in decimal format as mantissa * 10^exponent, used in the Ryu algorithm for fast floating-point to string conversion.

## Definition


## Detailed Description
The `floating_decimal_32` structure is a key data type in PostgreSQL's implementation of the Ryu algorithm for converting 32-bit floating-point numbers to their shortest decimal string representation. This structure represents a decimal number in the form `mantissa * 10^exponent`, where the mantissa contains the significant digits and the exponent indicates the decimal point position.

This representation is particularly useful in the Ryu algorithm because it allows for efficient computation of the shortest decimal representation of a floating-point number without precision loss. The algorithm converts the IEEE 754 binary representation to this decimal format as an intermediate step before generating the final string output.

The structure is defined in `src/common/f2s.c`, which is part of PostgreSQL's adaptation of the Ryu floating-point conversion library originally developed by Ulf Adams.

## Parameters / Member Variables
- `mantissa`: A 32-bit unsigned integer containing the significant digits of the decimal representation
- `exponent`: A 32-bit signed integer indicating the power of 10 by which the mantissa should be multiplied

## Dependencies
- Functions called/Symbols referenced: None (this is a simple struct definition)
- Called from (representative examples):
  - `[f2d](f2d.md)`: The main function that converts IEEE 754 binary format to this decimal representation
  - `[to_chars_f](../t/to_chars_f.md)`: Uses this structure to format the decimal representation into a string
  - `[to_chars](../t/to_chars.md)`: Another formatting function that processes this structure
  - `[f2d_small_int](f2d_small_int.md)`: Optimized path for small integers
  - `[float_to_shortest_decimal_bufn](float_to_shortest_decimal_bufn.md)`: High-level entry point for float to string conversion

## Notes and Other Information
- This structure is part of PostgreSQL's implementation of the Ryu algorithm, which provides fast and accurate floating-point to string conversion
- The algorithm guarantees that the generated decimal representation is the shortest one that will round back to the original floating-point value
- The structure is used internally within the conversion process and is not typically exposed to higher-level PostgreSQL code
- The implementation is based on the work of Ulf Adams and is used under the Boost Software License
- The `f2s.c` file specifically handles single-precision (32-bit) floating-point numbers, while PostgreSQL also has a corresponding `d2s.c` for double-precision numbers