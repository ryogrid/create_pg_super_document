# pg_atomic_fetch_sub_u32_impl

## Location
[src/include/port/atomics/generic.h:196-202](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/port/atomics/generic.h#L196-L202)

## Overview
Provides an atomic fetch-and-subtract operation for 32-bit unsigned integers using GCC's legacy sync builtin functions, returning the original value before subtraction.

## Definition
```c
static inline uint32 pg_atomic_fetch_sub_u32_impl(volatile pg_atomic_uint32 *ptr, int32 sub_)
```

## Detailed Description
This function implements an atomic fetch-and-subtract operation using GCC's `__sync_fetch_and_sub` builtin function. It atomically subtracts the specified value from the memory location pointed to by `ptr` and returns the original value that was stored there before the operation. This is part of PostgreSQL's GCC-specific atomic operations implementation layer.

The function uses the older GCC sync builtin functions rather than the newer atomic builtins, which may indicate compatibility requirements with older GCC versions. The operation is atomic and thread-safe, ensuring that concurrent access to the same memory location will not result in race conditions.

The function is marked as `static inline` to encourage compiler optimization through inlining, which is important for atomic operations that are often used in performance-critical code paths.

## Parameters / Member Variables
- `ptr`: Pointer to the atomic 32-bit unsigned integer variable to be modified
- `sub_`: The signed 32-bit value to subtract from the current value

## Dependencies
- Functions called/Symbols referenced:
  - `__sync_fetch_and_sub` (GCC sync builtin)
  - [pg_atomic_uint32](pg_atomic_uint32.md) (PostgreSQL atomic type)
- Called from (representative examples):
  - [pg_atomic_fetch_sub_u32](pg_atomic_fetch_sub_u32.md)
  - [pg_atomic_sub_fetch_u32_impl](pg_atomic_sub_fetch_u32_impl.md)

## Notes and Other Information
- Uses GCC's legacy sync builtin functions instead of newer atomic builtins
- Part of PostgreSQL's GCC-specific atomic operations implementation
- The function is typically not called directly but through higher-level atomic operation wrappers
- Provides full memory barrier semantics as per GCC sync builtin behavior
- Requires underlying hardware support for atomic operations on 32-bit values

## Simplified Source

```c
// Simplified version of pg_atomic_fetch_sub_u32_impl
static inline uint32 pg_atomic_fetch_sub_u32_impl(volatile pg_atomic_uint32 *ptr, int32 sub_) {
    // Atomically subtract sub_ from the value at ptr and return the original value
    return __sync_fetch_and_sub(&ptr->value, sub_);
}
```

Key simplifications made:
- This function is already very simple - it's a thin wrapper around GCC's builtin
- Added explanatory comment describing the atomic operation
- The original implementation is minimal and doesn't require significant simplification