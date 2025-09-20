# pg_atomic_add_fetch_u32_impl

## Location
[src/include/port/atomics/generic.h:231-237](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/port/atomics/generic.h#L231-L237)

## Overview
This function performs an atomic add operation on a 32-bit unsigned integer and returns the new value after the addition, providing a generic implementation for architectures without native atomic add-and-fetch support.

## Definition

```c
static inline uint32
pg_atomic_add_fetch_u32_impl(volatile pg_atomic_uint32 *ptr, int32 add_)
```
## Detailed Description
This is a generic implementation of atomic add-and-fetch operation that combines the functionality of fetch-add with post-addition to return the new value. It's implemented as a thin wrapper around `pg_atomic_fetch_add_u32_impl`, which performs the atomic addition using spinlock-based synchronization, and then adds the increment value to the returned old value to produce the new value. This function is used when the target architecture doesn't provide native support for atomic add-and-fetch operations.

## Parameters / Member Variables
- `ptr`: Pointer to the atomic 32-bit unsigned integer variable to be modified
- `add_`: The signed 32-bit integer value to add to the atomic variable

## Dependencies
- Functions called/Symbols referenced:
  - [pg_atomic_fetch_add_u32_impl](pg_atomic_fetch_add_u32_impl.md)
  - [pg_atomic_uint32](pg_atomic_uint32.md) (type)
- Called from (representative examples):
  - [pg_atomic_add_fetch_u32](pg_atomic_add_fetch_u32.md)

## Notes and Other Information
- This is a generic fallback implementation used when native atomic operations are not available
- The function is declared as static inline for performance optimization
- The implementation relies on spinlock-based synchronization in the underlying fetch_add function
- This function is part of PostgreSQL's portable atomic operations abstraction layer
- Located in src/include/port/atomics/generic.h as part of the generic atomic operations implementation