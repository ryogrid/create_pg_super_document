# pg_atomic_fetch_or_u64

## Location
[src/include/port/atomics.h:545-553](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/port/atomics.h#L545-L553)

## Overview
Atomically performs a bitwise OR operation on a 64-bit unsigned integer and returns the original value before the operation.

## Definition
static inline uint64
pg_atomic_fetch_or_u64(volatile pg_atomic_uint64 *ptr, uint64 or_)

## Detailed Description
This function provides an atomic fetch-and-OR operation for 64-bit unsigned integers. It atomically performs a bitwise OR between the target variable and the specified mask, then returns the original value that was present before the operation. The operation is thread-safe and ensures that no other thread can interfere with the read-modify-write sequence.

The function acts as a wrapper around the platform-specific implementation pg_atomic_fetch_or_u64_impl, providing a consistent interface across different architectures. This operation is commonly used for setting specific bits in a bitmask or implementing atomic flag operations.

## Parameters / Member Variables
- ptr: Pointer to the atomic 64-bit unsigned integer variable to be modified
- or_: The 64-bit unsigned integer mask to OR with the target variable

## Dependencies
- Functions called/Symbols referenced:
  - AssertPointerAlignment (for pointer alignment verification when not using simulation)
  - [pg_atomic_fetch_or_u64_impl](pg_atomic_fetch_or_u64_impl.md) (platform-specific implementation)
- Called from (representative examples):
  - [test_atomic_uint64](../t/test_atomic_uint64.md) (regression testing - multiple test cases)

## Notes and Other Information
- Returns the original value before the OR operation, not the result after the operation
- The function includes pointer alignment assertions to ensure proper memory alignment for atomic operations
- On GCC-compatible platforms, the implementation uses __sync_fetch_and_or builtin for optimal performance
- Commonly used for atomic bit setting operations where specific bits need to be turned on
- The or_ parameter is unsigned, as bitwise operations typically work with unsigned values
- This is part of PostgreSQLs portable atomic operations interface, providing consistent behavior across different hardware architectures
- Primarily used in testing scenarios within the current PostgreSQL codebase, indicating its specialized nature for low-level bit manipulation
- Complementary to pg_atomic_fetch_and_u64, where OR sets bits while AND clears them