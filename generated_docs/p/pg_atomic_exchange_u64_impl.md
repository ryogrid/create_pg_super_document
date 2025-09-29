# pg_atomic_exchange_u64_impl

## Location
[src/include/port/atomics/generic.h:267-277](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/port/atomics/generic.h#L267-L277)

## Overview
This function performs an atomic exchange operation on a 64-bit unsigned integer, replacing the current value with a new value and returning the previous value atomically using GCC compiler intrinsics.

## Definition
```c
static inline uint64 pg_atomic_exchange_u64_impl(volatile pg_atomic_uint64 *ptr, uint64 newval)
```

## Detailed Description
This is a GCC-specific implementation of atomic exchange operation for 64-bit unsigned integers that uses the GCC __atomic_exchange_n builtin function. The operation atomically replaces the value at the memory location pointed to by ptr with newval and returns the previous value that was stored at that location. The function uses __ATOMIC_SEQ_CST memory ordering, which provides sequential consistency - the strongest memory ordering guarantee that ensures all threads see the same order of operations. This implementation is used when the target platform supports GCC's atomic builtins for 64-bit operations.

## Parameters / Member Variables
- `ptr`: Pointer to the atomic 64-bit unsigned integer variable whose value will be exchanged
- `newval`: The new 64-bit unsigned integer value that will replace the current value

## Dependencies
- Functions called/Symbols referenced:
  - __atomic_exchange_n (GCC builtin)
  - [pg_atomic_uint64](pg_atomic_uint64.md) (type)
- Called from (representative examples):
  - [pg_atomic_exchange_u64](pg_atomic_exchange_u64.md)
  - [pg_atomic_write_membarrier_u64_impl](pg_atomic_write_membarrier_u64_impl.md)

## Notes and Other Information
- This is a GCC-specific implementation using compiler intrinsics for optimal performance
- The function is declared as static inline for performance optimization
- Uses __ATOMIC_SEQ_CST memory ordering for sequential consistency semantics
- Located in src/include/port/atomics/generic-gcc.h as part of the GCC-specific atomic operations
- This implementation is used when native 64-bit atomic operations are supported by the processor
- The exchange operation is fundamental for implementing other atomic operations like atomic writes with memory barriers
- Provides lock-free atomic operations which are essential for high-performance concurrent programming

## Simplified Source

```c
static inline uint64
pg_atomic_exchange_u64_impl(volatile pg_atomic_uint64 *ptr, uint64 newval)
{
    // Atomically exchange value using GCC builtin with sequential consistency
    return __atomic_exchange_n(&ptr->value, newval, __ATOMIC_SEQ_CST);
}
```