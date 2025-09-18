# int128_add_int64

## Location
[src/include/common/int128.h:50-61](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/common/int128.h#L50-L61)

## Overview
Adds a signed 64-bit integer value to a 128-bit integer variable in-place.

## Definition
```c
static inline void
int128_add_int64(INT128 *i128, int64 v)
```

## Detailed Description
This function performs an in-place addition operation, adding a signed 64-bit integer value to an existing 128-bit integer. The function is implemented as a static inline function for optimal performance, utilizing the underlying 128-bit integer arithmetic capabilities. The addition is performed directly using the += operator on the dereferenced INT128 pointer, with proper sign extension handling for the signed input value.

## Parameters / Member Variables
- `i128`: Pointer to the INT128 variable to be modified. The result of the addition is stored back into this variable.
- `v`: The signed 64-bit integer value to be added to the INT128 variable.

## Dependencies
- Functions called/Symbols referenced:
  - INT128 (type definition)
- Called from (representative examples):
  - [main](../m/main.md) (in src/tools/testint128.c:113)

## Notes and Other Information
- This is a static inline function defined in the header file for maximum performance
- The function modifies the INT128 variable in-place rather than returning a new value
- Handles both positive and negative signed 64-bit integers correctly through automatic sign extension
- Part of PostgreSQL's 128-bit integer arithmetic library for handling large numeric calculations
- Companion function to int128_add_uint64 for signed integer addition operations