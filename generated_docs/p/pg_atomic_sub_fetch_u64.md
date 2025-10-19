# pg_atomic_sub_fetch_u64

## Location
[src/include/port/atomics.h:563-579](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/port/atomics.h#L563-L579)

## Overview
Atomically subtracts a value from a 64-bit unsigned integer and returns the resulting value after the subtraction.

## Definition

```c
static inline uint64
pg_atomic_sub_fetch_u64(volatile pg_atomic_uint64 *ptr, int64 sub_)
```
## Detailed Description
This function performs an atomic subtract-and-fetch operation on a 64-bit unsigned integer. It subtracts the specified value from the atomic variable and returns the new value after the subtraction has been performed. The operation is guaranteed to be atomic, meaning it cannot be interrupted by other threads or processes, ensuring thread-safe access to shared memory.

The function is implemented as a wrapper around `pg_atomic_sub_fetch_u64_impl`, which in turn uses `pg_atomic_fetch_sub_u64_impl` and performs the calculation `fetch_sub_result - sub_` to return the post-subtraction value.

The function includes safety assertions to ensure proper alignment (8-byte alignment for 64-bit operations) and validates that the subtraction value is not `PG_INT64_MIN` to prevent overflow issues.

## Parameters / Member Variables
- `ptr`: Pointer to the atomic 64-bit unsigned integer variable to be modified
- `sub_`: The signed 64-bit value to subtract from the atomic variable

## Dependencies
- Functions called/Symbols referenced:
  - [pg_atomic_sub_fetch_u64_impl](pg_atomic_sub_fetch_u64_impl.md)
  - AssertPointerAlignment
  - Assert
  - PG_HAVE_ATOMIC_U64_SIMULATION (conditional compilation)
  - PG_INT64_MIN (for validation)
- Called from (representative examples):
  - [test_atomic_uint64](../t/test_atomic_uint64.md)

## Notes and Other Information
- The function requires 8-byte alignment of the target pointer when not using simulation mode
- Input validation prevents subtraction of `PG_INT64_MIN` to avoid potential overflow scenarios
- This is part of PostgreSQL's atomic operations abstraction layer that provides consistent interfaces across different platforms
- The subtract operation is implemented atomically, making it safe for concurrent access in multi-threaded environments

## Simplified Source

```c
static inline uint64
pg_atomic_sub_fetch_u64(volatile pg_atomic_uint64 *ptr, int64 sub_)
{
    // Safety checks: alignment and overflow protection
    AssertPointerAlignment(ptr, 8);
    Assert(sub_ != PG_INT64_MIN);

    // Atomic subtract-and-fetch: return new value after subtraction
    return pg_atomic_sub_fetch_u64_impl(ptr, sub_);
}
```

**Key Points:**
- Atomically subtracts `sub_` from the value at `ptr`
- Returns the new value after subtraction (not the original value)
- Includes alignment checks and overflow protection
- Thread-safe atomic operation with memory ordering guarantees
- Complementary to `pg_atomic_fetch_sub_u64` (which returns original value)