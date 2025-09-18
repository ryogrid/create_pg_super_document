# convert_to_base

## Location
src/backend/utils/adt/varlena.c: 4930 - 4955

## Overview
Internal utility function that converts unsigned 64-bit integers to their string representation in any base from 2 to 16, serving as the core implementation for PostgreSQL's to_bin, to_oct, and to_hex functions.

## Definition


## Detailed Description
This function implements efficient base conversion using a reverse-building algorithm that constructs the result string from right to left. It uses a fixed character array for digit representation, supporting bases from binary (base 2) through hexadecimal (base 16) with lowercase letters for digits 10-15. The function is optimized for performance by pre-allocating a buffer sized for the worst case (binary representation of a 64-bit integer) and building the result string backwards to avoid the need for string reversal. The algorithm repeatedly divides the input value by the target base, using the remainder as the next digit and continuing until the value becomes zero or the buffer is exhausted.

## Parameters / Member Variables
- : Unsigned 64-bit integer to be converted to the target base representation
- : Target numeric base for conversion (must be between 2 and 16 inclusive)

## Dependencies
- Functions called/Symbols referenced:
  - BITS_PER_BYTE (macro for calculating buffer size based on bit width)
  - cstring_to_text_with_len (converts the resulting C string to PostgreSQL text type)
  - Assert (debug assertions to validate base parameter constraints)
- Called from:
  - [to_bin32](../t/to_bin32.md), to_bin64 (binary conversion functions)
  - [to_oct32](../t/to_oct32.md), to_oct64 (octal conversion functions) 
  - [to_hex32](../t/to_hex32.md), to_hex64 (hexadecimal conversion functions)

## Notes and Other Information
The function is marked as static inline for performance, as it's called frequently by the various base conversion functions and the inlining optimization helps eliminate function call overhead. The buffer is sized to handle the longest possible output (binary representation of UINT64_MAX), which provides sufficient space for any supported base. The reverse-construction approach eliminates the need for string manipulation operations, making the conversion very efficient. The use of assertion checks ensures that the base parameter stays within the supported range, catching programming errors during development. The digit string "0123456789abcdef" provides a compact lookup table for all possible digit values in bases up to 16.