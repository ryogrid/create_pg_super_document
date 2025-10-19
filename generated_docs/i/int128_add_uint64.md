# int128_add_uint64

## Location
[src/include/common/int128.h:41-49](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/common/int128.h#L41-L49)

## Overview
Adds an unsigned 64-bit integer value to a 128-bit integer variable in-place.

## Definition

```c
static inline void
int128_add_uint64(INT128 *i128, uint64 v)
```
## Detailed Description
This function performs an in-place addition operation, adding an unsigned 64-bit integer value to an existing 128-bit integer. The function is implemented as a static inline function for optimal performance, utilizing the underlying 128-bit integer arithmetic capabilities. The addition is performed directly using the += operator on the dereferenced INT128 pointer.

## Parameters / Member Variables
- `*i128`: Pointer to the INT128 variable to be modified. The result of the addition is stored back into this variable.
- `v`: The unsigned 64-bit integer value to be added to the INT128 variable.
## Dependencies
- Functions called/Symbols referenced:
  - INT128 (type definition)
- Called from (representative examples):
  - [int128_add_int64_mul_int64](int128_add_int64_mul_int64.md) (in src/include/common/int128.h:219, 227, 230)
  - [main](../m/main.md) (in src/tools/testint128.c:98)

## Notes and Other Information
- This is a static inline function defined in the header file for maximum performance
- The function modifies the INT128 variable in-place rather than returning a new value
- Part of PostgreSQL's 128-bit integer arithmetic library for handling large numeric calculations
- Used internally by other int128 arithmetic functions for compound operations

## Simplified Source

```c
static inline void int128_add_uint64(INT128 *i128, uint64 v) {
    // Simple in-place addition of unsigned 64-bit value to 128-bit integer
    *i128 += v;
}
```