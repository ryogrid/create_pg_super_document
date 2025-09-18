# pg_atomic_fetch_and_u32_impl

## Location
src/include/port/atomics/generic.h: 205 - 215

## Overview
Provides an atomic fetch-and-bitwise-AND operation for 32-bit unsigned integers using GCC's legacy sync builtin functions, returning the original value before the operation.

## Definition
```c
static inline uint32 pg_atomic_fetch_and_u32_impl(volatile pg_atomic_uint32 *ptr, uint32 and_)
```

## Detailed Description
This function implements an atomic fetch-and-bitwise-AND operation using GCC's `__sync_fetch_and_and` builtin function. It atomically performs a bitwise AND operation between the current value at the memory location pointed to by `ptr` and the provided `and_` value, then returns the original value that was stored there before the operation. This is commonly used for atomic bit manipulation operations such as clearing specific bits in a flags register.

The function uses GCC's older sync builtin functions, which provide full memory barrier semantics and ensure the operation is atomic and thread-safe. This is part of PostgreSQL's GCC-specific atomic operations implementation layer that provides hardware-accelerated atomic operations when available.

The function is marked as `static inline` to encourage compiler optimization through inlining, which is crucial for atomic operations used in performance-critical sections.

## Parameters / Member Variables
- `ptr`: Pointer to the atomic 32-bit unsigned integer variable to be modified
- `and_`: The 32-bit unsigned value to bitwise AND with the current value

## Dependencies
- Functions called/Symbols referenced:
  - `__sync_fetch_and_and` (GCC sync builtin)
  - `pg_atomic_uint32` (PostgreSQL atomic type)
- Called from (representative examples):
  - `pg_atomic_fetch_and_u32`

## Notes and Other Information
- Uses GCC's legacy sync builtin functions for maximum compatibility
- Commonly used for atomic bit clearing operations (e.g., clearing flags)
- Part of PostgreSQL's GCC-specific atomic operations implementation
- The function is typically accessed through higher-level atomic operation wrappers
- Provides full memory barrier semantics as per GCC sync builtin behavior
- Requires underlying hardware support for atomic bitwise operations on 32-bit values