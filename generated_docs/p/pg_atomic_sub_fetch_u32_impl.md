# pg_atomic_sub_fetch_u32_impl

## Location
src/include/port/atomics/generic.h: 240 - 246

## Overview
This function performs an atomic subtraction operation on a 32-bit unsigned integer and returns the new value after the subtraction, providing a generic implementation for architectures without native atomic subtract-and-fetch support.

## Definition
```c
static inline uint32 pg_atomic_sub_fetch_u32_impl(volatile pg_atomic_uint32 *ptr, int32 sub_)
```

## Detailed Description
This is a generic implementation of atomic subtract-and-fetch operation that combines the functionality of fetch-subtract with post-subtraction to return the new value. It's implemented as a thin wrapper around `pg_atomic_fetch_sub_u32_impl`, which performs the atomic subtraction using compiler intrinsics (like GCC's __sync_fetch_and_sub), and then subtracts the decrement value from the returned old value to produce the new value. This function is used when the target architecture doesn't provide native support for atomic subtract-and-fetch operations.

## Parameters / Member Variables
- `ptr`: Pointer to the atomic 32-bit unsigned integer variable to be modified
- `sub_`: The signed 32-bit integer value to subtract from the atomic variable

## Dependencies
- Functions called/Symbols referenced:
  - pg_atomic_fetch_sub_u32_impl
  - pg_atomic_uint32 (type)
- Called from (representative examples):
  - pg_atomic_sub_fetch_u32

## Notes and Other Information
- This is a generic fallback implementation used when native atomic operations are not available
- The function is declared as static inline for performance optimization
- The implementation relies on compiler intrinsics in the underlying fetch_sub function
- This function is part of PostgreSQL's portable atomic operations abstraction layer
- Located in src/include/port/atomics/generic.h as part of the generic atomic operations implementation
- Complements the add_fetch operation by providing the subtract equivalent