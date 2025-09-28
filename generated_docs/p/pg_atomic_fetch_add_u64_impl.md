# pg_atomic_fetch_add_u64_impl

## Location
[src/include/port/atomics/generic-gcc.h:288-294](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/port/atomics/generic-gcc.h#L288-L294)

## Overview
Implements atomic fetch-and-add operation for 64-bit unsigned integers, atomically adding a value and returning the previous value.

## Definition
```c
uint64 pg_atomic_fetch_add_u64_impl(volatile pg_atomic_uint64 *ptr, int64 add_)
```

## Detailed Description
This function provides platform-specific implementations of atomic fetch-and-add operations for 64-bit unsigned integers. There are multiple implementations depending on compiler and platform capabilities:

1. **Spinlock-based fallback implementation** (src/backend/port/atomics.c:228-239): Uses spinlocks to ensure atomicity when hardware atomic operations are not available
2. **GCC implementation** (src/include/port/atomics/generic-gcc.h:288-291): Uses GCC's `__sync_fetch_and_add` builtin when available
3. **Generic fallback implementation** (src/include/port/atomics/generic.h:358-365): Uses compare-and-swap loop when native fetch-add is not available

The function atomically adds the specified value to the memory location and returns the value that was previously stored at that location.

## Parameters / Member Variables
- `ptr`: Pointer to the atomic 64-bit unsigned integer variable to operate on
- `add_`: The signed 64-bit value to add (can be negative for subtraction)

## Dependencies
- Functions called/Symbols referenced:
  - SpinLockAcquire/SpinLockRelease (spinlock implementation)
  - __sync_fetch_and_add (GCC implementation)
  - [pg_atomic_compare_exchange_u64_impl](pg_atomic_compare_exchange_u64_impl.md) (generic fallback)
  - [pg_atomic_uint64](pg_atomic_uint64.md) (type)
  - [slock_t](../s/slock_t.md) (type)
- Called from (representative examples):
  - [pg_atomic_fetch_add_u64](pg_atomic_fetch_add_u64.md) (inline wrapper)
  - [pg_atomic_fetch_sub_u64_impl](pg_atomic_fetch_sub_u64_impl.md)
  - [pg_atomic_add_fetch_u64_impl](pg_atomic_add_fetch_u64_impl.md)
  - [pg_atomic_read_membarrier_u64_impl](pg_atomic_read_membarrier_u64_impl.md)

## Notes and Other Information
- Multiple implementations exist for different compilers and platforms
- Spinlock-based implementation is used as the ultimate fallback when no hardware atomic support is available
- GCC version uses hardware-optimized builtin functions when supported
- Generic implementation uses compare-and-swap loop for broader platform compatibility
- Used as building block for other atomic operations like subtract, add-fetch, and memory barrier reads
- Part of PostgreSQL's portable atomic operations infrastructure
- Located primarily in src/backend/port/atomics.c:228-239 for the fallback implementation

## Simplified Source

```c
// Simplified version of pg_atomic_fetch_add_u64_impl
uint64 pg_atomic_fetch_add_u64_impl(volatile pg_atomic_uint64 *ptr, int64 add_) {
    uint64 oldval;

    // Acquire spinlock to ensure atomic operation
    SpinLockAcquire((slock_t *) &ptr->sema);

    // Store old value before modification
    oldval = ptr->value;

    // Add the specified value to the atomic variable
    ptr->value += add_;

    // Release spinlock
    SpinLockRelease((slock_t *) &ptr->sema);

    // Return the previous value (before addition)
    return oldval;
}
```

Key simplifications made:
- Added explanatory comments for each logical step
- Maintained original variable names as they are already clear
- No complex error handling to remove - function is straightforward
- Preserved exact logic flow since it's already minimal and efficient
- Function implements classic atomic fetch-and-add pattern using spinlock protection