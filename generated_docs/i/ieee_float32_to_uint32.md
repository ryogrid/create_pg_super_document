# ieee_float32_to_uint32

## Location
src/backend/access/gist/gistproc.c: 1603 - 1680

## Overview
A static utility function that converts IEEE 754 32-bit floating-point numbers to 32-bit unsigned integers while preserving numerical ordering for spatial indexing operations.

## Definition
```c
static uint32 ieee_float32_to_uint32(float f)
```

## Detailed Description
This function performs a specialized conversion from IEEE 754 single-precision floating-point values to unsigned 32-bit integers with a critical property: the relative ordering of the original floating-point values is preserved in the resulting integer representation. This is essential for Morton code generation in spatial indexing, where coordinate values need to maintain their relative ordering after conversion. The function maps negative values to the range 0-7FFFFFFF, zero to 80000000, and positive values to the range 80000001-FFFFFFFF. Special IEEE values like NaN are mapped to 0xFFFFFFFF. The conversion leverages the bit-level representation of IEEE 754 floats and applies specific transformations to ensure smooth ordering across the negative-to-positive transition.

## Parameters / Member Variables
- `f`: A 32-bit IEEE 754 floating-point number to be converted

## Dependencies
- Functions called/Symbols referenced:
  - isnan (checks for NaN values)
  - Assert (debugging assertion macro)
- Called from:
  - point_zorder_internal (called twice for X and Y coordinates)

## Notes and Other Information
- Preserves the natural ordering of floating-point numbers in the integer domain
- Uses union-based type punning to access the bit representation of floats
- Maps negative values by XORing with 0xFFFFFFFF (bitwise inversion)
- Maps positive values by ORing with 0x80000000 (setting the high bit)
- All NaN values are mapped to the same value (0xFFFFFFFF) regardless of their specific bit patterns
- Both positive and negative zero map to the same integer value (0x80000000)
- The mapping ensures there are no gaps in the ordering across the zero boundary
- Essential for spatial indexing algorithms that require order-preserving coordinate transformations
- Takes advantage of IEEE 754's property that bit patterns naturally preserve ordering within the same sign