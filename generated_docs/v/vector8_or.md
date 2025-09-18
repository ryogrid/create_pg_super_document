# vector8_or

## Location
src/include/port/simd.h: 338 - 350

## Overview
Performs bitwise OR operation on two 8-byte SIMD vectors using platform-optimized instructions.

## Definition
```c
static inline Vector8
vector8_or(const Vector8 v1, const Vector8 v2)
```

## Detailed Description
The `vector8_or` function performs a bitwise OR operation on two Vector8 inputs, returning a Vector8 result. The implementation is optimized for different SIMD instruction sets:
- On SSE2-capable processors, uses `_mm_or_si128()` intrinsic
- On ARM NEON processors, uses `vorrq_u8()` intrinsic  
- Falls back to standard bitwise OR operator for non-SIMD platforms

This function is part of PostgreSQL's portable SIMD abstraction layer that provides consistent vector operations across different architectures.

## Parameters / Member Variables
- `v1`: First Vector8 operand for the bitwise OR operation
- `v2`: Second Vector8 operand for the bitwise OR operation

## Dependencies
- Functions called/Symbols referenced:
  - `_mm_or_si128` (SSE2 intrinsic)
  - `vorrq_u8` (NEON intrinsic)
  - Vector8 type
- Called from (representative examples):
  - [is_valid_ascii](../i/is_valid_ascii.md) (ASCII validation utility)

## Notes and Other Information
- Defined as static inline for optimal performance
- Conditional compilation ensures the most efficient instruction set is used
- Part of the portable SIMD interface in src/include/port/simd.h
- Used primarily for ASCII validation and other byte-level operations requiring bitwise logic