# is_valid_ascii

## Location
src/include/utils/ascii.h: 25 - 84

## Overview
A high-performance inline function that validates whether a chunk of bytes contains only valid ASCII characters using SIMD optimization techniques.

## Definition
```c
static inline bool is_valid_ascii(const unsigned char *s, int len)
```

## Detailed Description
This function verifies that a given chunk of bytes contains only valid ASCII characters by checking for two conditions:
1. No zero bytes (null terminators)
2. No bytes with the high bit set (values > 127)

The function uses vectorized operations (SIMD) for efficient processing, with two different implementation strategies depending on whether SIMD instructions are available. It processes data in chunks of 8 or 16 bytes at a time, using vector operations to detect invalid characters in parallel.

The function employs two different algorithms:
- **SIMD version**: Uses vector equality and OR operations to detect zero bytes and high bits
- **Non-SIMD version**: Uses arithmetic operations (adding 0x7F) to detect zeros and accumulates high bits

The input length must be a multiple of the chunk size (8 or 16 bytes) as enforced by an assertion.

## Parameters / Member Variables
- `s`: Pointer to the unsigned char array to validate
- `len`: Length of the input array in bytes (must be multiple of chunk size)

## Dependencies
- Functions called/Symbols referenced:
  - Vector8 (SIMD vector type)
  - vector8_broadcast
  - vector8_load
  - vector8_or
  - vector8_eq
  - vector8_is_highbit_set
- Called from (representative examples):
  - Used in UTF-8 validation context in src/common/wchar.c:1913

## Notes and Other Information
- This is a static inline function for optimal performance
- Requires input length to be aligned to vector chunk boundaries
- Part of PostgreSQL's ASCII validation utilities in src/include/utils/ascii.h
- Uses conditional compilation (#ifdef USE_NO_SIMD) to provide fallback implementation
- Critical for UTF-8 processing performance where ASCII fast-path optimization is needed
- The function is designed for high-throughput text processing scenarios