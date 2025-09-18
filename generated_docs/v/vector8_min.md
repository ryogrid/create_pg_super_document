# vector8_min

## Location
src/include/port/simd.h: 412 - 422

## Overview
Computes the element-wise minimum of two 8-bit unsigned integer vectors using SIMD instructions, providing optimized vector operations for performance-critical code paths.

## Definition


## Detailed Description
The  function performs element-wise minimum comparison between two Vector8 objects (128-bit SIMD vectors containing 16 8-bit unsigned integers). It leverages platform-specific SIMD instruction sets to achieve optimal performance:

- On x86/x64 platforms with SSE2 support, it uses the  intrinsic
- On ARM platforms with NEON support, it uses the  intrinsic

This function is part of PostgreSQL's SIMD abstraction layer, which provides a unified interface for vector operations across different CPU architectures. It's commonly used in performance-critical algorithms like radix tree operations where efficient byte-wise comparisons are essential.

The function operates on 16 bytes simultaneously, comparing each corresponding byte position and returning the smaller value at each position. This parallelized approach significantly outperforms scalar implementations when processing large amounts of data.

## Parameters / Member Variables
- : First input vector containing 16 8-bit unsigned integers
- : Second input vector containing 16 8-bit unsigned integers

## Dependencies
- Functions called/Symbols referenced:
  -  (SSE2 intrinsic for x86/x64)
  -  (NEON intrinsic for ARM)
  -  (typedef for __m128i on SSE2 platforms)
  -  (preprocessor macro)
  -  (preprocessor macro)

- Called from (representative examples):
  -  (in src/include/lib/radixtree.h:1215, 1216)

## Notes and Other Information
- The function is declared as  for optimal performance and to avoid function call overhead
- Platform-specific implementations ensure maximum efficiency on different CPU architectures
- Primarily used in radix tree implementations for efficient key comparison and search operations
- The function assumes both input vectors contain unsigned 8-bit values (0-255 range)
- Return value is a Vector8 containing the element-wise minimum values
- Part of PostgreSQL's SIMD abstraction layer located in src/include/port/simd.h:412-422