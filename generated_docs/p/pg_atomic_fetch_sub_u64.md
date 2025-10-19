# pg_atomic_fetch_sub_u64

## Location
[src/include/port/atomics.h:526-535](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/port/atomics.h#L526-L535)

## Overview
Atomically subtracts a value from a 64-bit unsigned integer and returns the original value before the subtraction.

## Definition
static inline uint64
pg_atomic_fetch_sub_u64(volatile pg_atomic_uint64 *ptr, int64 sub_)

## Detailed Description
This function provides an atomic fetch-and-subtract operation for 64-bit unsigned integers. It atomically subtracts the specified value from the target variable and returns the original value that was present before the subtraction. The operation is thread-safe and ensures that no other thread can interfere with the read-modify-write sequence.

The function acts as a wrapper around the platform-specific implementation pg_atomic_fetch_sub_u64_impl, providing a consistent interface across different architectures. It includes additional safety checks to prevent integer overflow by asserting that the subtraction value is not PG_INT64_MIN (the most negative 64-bit integer).

## Parameters / Member Variables
- ptr: Pointer to the atomic 64-bit unsigned integer variable to be modified
- sub_: The signed 64-bit value to subtract from the target variable (must not be PG_INT64_MIN)

## Dependencies
- Functions called/Symbols referenced:
  - AssertPointerAlignment (for pointer alignment verification when not using simulation)
  - Assert (for runtime assertion checking)
  - PG_INT64_MIN (minimum 64-bit signed integer constant)
  - [pg_atomic_fetch_sub_u64_impl](pg_atomic_fetch_sub_u64_impl.md) (platform-specific implementation)
- Called from (representative examples):
  - [test_atomic_uint64](../t/test_atomic_uint64.md) (regression testing)

## Notes and Other Information
- Returns the original value before the subtraction, not the result after subtraction
- The function includes pointer alignment assertions to ensure proper memory alignment for atomic operations
- Contains a safety assertion that prevents passing PG_INT64_MIN as the subtraction value to avoid integer overflow issues
- On GCC-compatible platforms, the implementation uses __sync_fetch_and_sub builtin for optimal performance
- This is part of PostgreSQLs portable atomic operations interface, providing consistent behavior across different hardware architectures
- Less commonly used than pg_atomic_fetch_add_u64, as subtraction can often be achieved by adding a negative value

## Simplified Source

```c
static inline uint64
pg_atomic_fetch_sub_u64(volatile pg_atomic_uint64 *ptr, int64 sub_)
{
    // Safety check: ensure pointer is aligned and value is valid
    AssertPointerAlignment(ptr, 8);
    Assert(sub_ != PG_INT64_MIN);

    // Atomic fetch-and-subtract: return original value before subtraction
    return pg_atomic_fetch_sub_u64_impl(ptr, sub_);
}
```

**Key Points:**
- Atomically subtracts `sub_` from the value at `ptr`
- Returns the original value before subtraction (not the new value)
- Includes alignment checks and overflow protection
- Thread-safe atomic operation with memory ordering guarantees
- Prevents integer overflow by rejecting PG_INT64_MIN input