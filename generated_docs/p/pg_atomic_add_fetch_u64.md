# pg_atomic_add_fetch_u64

## Location
[src/include/port/atomics.h:554-562](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/port/atomics.h#L554-L562)

## Overview
Atomically adds a value to a 64-bit unsigned integer and returns the result after the addition.

## Definition
static inline uint64
pg_atomic_add_fetch_u64(volatile pg_atomic_uint64 *ptr, int64 add_)

## Detailed Description
This function provides an atomic add-and-fetch operation for 64-bit unsigned integers. It atomically adds the specified value to the target variable and returns the new value after the addition has been performed. The operation is thread-safe and ensures that no other thread can interfere with the read-modify-write sequence.

The function acts as a wrapper around the platform-specific implementation pg_atomic_add_fetch_u64_impl, providing a consistent interface across different architectures. This operation is the complement to pg_atomic_fetch_add_u64 - while fetch_add returns the old value, add_fetch returns the new value after the operation.

## Parameters / Member Variables
- ptr: Pointer to the atomic 64-bit unsigned integer variable to be modified
- add_: The signed 64-bit value to add to the target variable (can be negative for subtraction)

## Dependencies
- Functions called/Symbols referenced:
  - AssertPointerAlignment (for pointer alignment verification when not using simulation)
  - [pg_atomic_add_fetch_u64_impl](pg_atomic_add_fetch_u64_impl.md) (platform-specific implementation which calls pg_atomic_fetch_add_u64_impl)
- Called from (representative examples):
  - [EmitProcSignalBarrier](../E/EmitProcSignalBarrier.md) (for incrementing process signal barrier generation counter)
  - [test_atomic_uint64](../t/test_atomic_uint64.md) (regression testing)

## Notes and Other Information
- Returns the new value after the addition, not the original value before addition
- The function includes pointer alignment assertions to ensure proper memory alignment for atomic operations
- The generic implementation simply calls pg_atomic_fetch_add_u64_impl and adds the increment value to the result
- Used in PostgreSQLs process signaling mechanism to atomically increment generation counters for barrier synchronization
- The add_ parameter is signed, allowing both addition (positive values) and subtraction (negative values)
- This is part of PostgreSQLs portable atomic operations interface, providing consistent behavior across different hardware architectures
- Particularly useful when you need to know the final result of the atomic addition rather than the original value

## Simplified Source

```c
static inline uint64 pg_atomic_add_fetch_u64(volatile pg_atomic_uint64 *ptr, int64 add_) {
#ifndef PG_HAVE_ATOMIC_U64_SIMULATION
    // Ensure proper 8-byte alignment for atomic operations
    AssertPointerAlignment(ptr, 8);
#endif
    // Delegate to platform-specific implementation
    return pg_atomic_add_fetch_u64_impl(ptr, add_);
}
```