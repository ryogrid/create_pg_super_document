# pg_atomic_exchange_u32_impl

## Location
src/include/port/atomics/generic-gcc.h: 190 - 198

## Overview
Provides atomic exchange operation for 32-bit unsigned integers with multiple implementation strategies depending on available hardware features.

## Definition
```c
static inline uint32 pg_atomic_exchange_u32_impl(volatile pg_atomic_uint32 *ptr, uint32 newval)
```

## Detailed Description
This function implements the atomic exchange operation for PostgreSQL's 32-bit atomic integer type. It atomically replaces the value at the specified location with a new value and returns the previous value. The implementation varies based on available hardware features:

1. **GCC Implementation** (generic-gcc.h): Uses GCC's `__atomic_exchange_n` builtin with sequential consistency ordering
2. **Generic Fallback** (generic.h): Implements exchange using compare-and-swap in a retry loop when native exchange is unavailable

The generic fallback reads the current value and repeatedly attempts compare-and-swap until successful, handling potential race conditions through the retry mechanism.

## Parameters / Member Variables
- `ptr`: Pointer to the volatile atomic uint32 structure to operate on
- `newval`: The new value to store atomically

## Dependencies
- Functions called/Symbols referenced:
  - pg_atomic_uint32 (struct type)
  - `__atomic_exchange_n` (GCC builtin, in GCC implementation)
  - pg_atomic_compare_exchange_u32_impl (in generic fallback)
  - PG_HAVE_ATOMIC_FETCH_ADD_U32 (feature detection macro)
- Called from (representative examples):
  - pg_atomic_exchange_u32 (inline wrapper function)
  - pg_atomic_test_set_flag_impl (generic implementation)
  - pg_atomic_write_membarrier_u32_impl (generic implementation)

## Notes and Other Information
- Multiple implementations exist for different compiler/platform combinations
- GCC implementation located in src/include/port/atomics/generic-gcc.h:190-198
- Generic fallback located in src/include/port/atomics/generic.h:170-177
- The GCC version uses sequential consistency memory ordering (`__ATOMIC_SEQ_CST`)
- Generic fallback uses compare-and-swap retry loop, allowing for potential performance differences
- Part of PostgreSQL's portable atomic operations abstraction layer
- Returns the previous value that was stored at the location