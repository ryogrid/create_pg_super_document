# double_to_bits

## Location
src/common/ryu_common.h: 125 - 133

## Overview
Extracts the IEEE 754 binary representation of a double-precision floating-point number as a 64-bit unsigned integer for bit-level manipulation in the Ryu floating-point output algorithm.

## Definition
static inline uint64 double_to_bits(const double d)

## Detailed Description
This function converts a double-precision floating-point value into its raw IEEE 754 binary representation as a 64-bit unsigned integer. It uses memcpy() to safely perform the type punning operation, avoiding undefined behavior that could arise from direct casting or union-based type punning. This is a critical utility function in PostgreSQL's implementation of the Ryu algorithm for fast and accurate floating-point to string conversion.

The function performs a safe bit-level copy of the double's memory representation into a uint64 variable, preserving the IEEE 754 format structure (sign bit, exponent, and mantissa) while allowing subsequent bit manipulation operations needed for the Ryu algorithm's decimal conversion process.

## Parameters / Member Variables
- : The double-precision floating-point number whose binary representation is to be extracted

## Dependencies
- Functions called/Symbols referenced:
  - memcpy (standard C library function)
- Called from (representative examples):
  - double_to_shortest_decimal_bufn

## Notes and Other Information
- This function is part of PostgreSQL's Ryu floating-point output implementation, adapted from Ulf Adams' Ryu algorithm
- The use of memcpy() ensures portability across different architectures and avoids potential strict aliasing violations
- This is a companion function to float_to_bits() which performs the same operation for single-precision floats
- The returned uint64 value preserves the exact IEEE 754 bit layout: 1 sign bit, 11 exponent bits, and 52 mantissa bits
- Located in src/common/ryu_common.h:125-130