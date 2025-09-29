# pg_atomic_compare_exchange_u64_impl

## Location
[src/include/port/atomics/generic-gcc.h:252-274](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/port/atomics/generic-gcc.h#L252-L274)

## Overview
Implements atomic compare-and-swap operation for 64-bit unsigned integers using spinlock-based fallback implementation when hardware atomic operations are not available.

## Definition
```c
bool pg_atomic_compare_exchange_u64_impl(volatile pg_atomic_uint64 *ptr, uint64 *expected, uint64 newval)
```

## Detailed Description
This function provides a fallback implementation for atomic compare-and-swap operations on 64-bit unsigned integers. It uses spinlocks to ensure atomicity when native hardware atomic operations are unavailable. The implementation emulates a "strong" compare-and-swap operation that does not allow spurious failures, which is important for algorithms that rely on this behavior. The function compares the current value at the memory location with the expected value, and if they match, replaces it with the new value atomically.

## Parameters / Member Variables
- `ptr`: Pointer to the atomic 64-bit unsigned integer variable to operate on
- `expected`: Pointer to the expected value; will be updated with the actual current value
- `newval`: The new value to store if the comparison succeeds

## Dependencies
- Functions called/Symbols referenced:
  - SpinLockAcquire
  - SpinLockRelease
  - [pg_atomic_uint64](pg_atomic_uint64.md) (type)
  - [slock_t](../s/slock_t.md) (type)
- Called from (representative examples):
  - [pg_atomic_compare_exchange_u64](pg_atomic_compare_exchange_u64.md) (inline wrapper)
  - [pg_atomic_exchange_u64_impl](pg_atomic_exchange_u64_impl.md)
  - [pg_atomic_fetch_add_u64_impl](pg_atomic_fetch_add_u64_impl.md)
  - [pg_atomic_fetch_and_u64_impl](pg_atomic_fetch_and_u64_impl.md)

## Notes and Other Information
- This is a fallback implementation used when PG_HAVE_ATOMIC_COMPARE_EXCHANGE_U64 is not defined
- Uses spinlocks to provide thread-safe atomic behavior across all supported platforms
- Implements strong compare-and-swap semantics (no spurious failures)
- Located in src/backend/port/atomics.c:200-227
- Part of PostgreSQL's portable atomic operations infrastructure

## Simplified Source

```c
// Simplified version of pg_atomic_compare_exchange_u64_impl
bool pg_atomic_compare_exchange_u64_impl(volatile pg_atomic_uint64 *ptr,
                                         uint64 *expected, uint64 newval) {
    bool match_found;

    // Acquire spinlock for atomic operation
    SpinLockAcquire((slock_t *) &ptr->sema);

    // Compare current value with expected value
    match_found = (ptr->value == *expected);

    // Always update expected with actual current value
    *expected = ptr->value;

    // If values matched, store the new value
    if (match_found) {
        ptr->value = newval;
    }

    // Release spinlock
    SpinLockRelease((slock_t *) &ptr->sema);

    return match_found;
}
```

Key simplifications made:
- Removed detailed comments explaining implementation rationale
- Used more descriptive variable name (`match_found` instead of `ret`)
- Simplified the compare-and-swap logic flow with clearer variable naming
- Consolidated the core algorithm into clear, sequential steps
- Maintained the essential spinlock-based atomicity mechanism