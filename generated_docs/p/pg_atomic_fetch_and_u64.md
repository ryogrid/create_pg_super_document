# pg_atomic_fetch_and_u64

## Location
src/include/port/atomics.h: 536 - 544

## Overview
Atomically performs a bitwise AND operation on a 64-bit unsigned integer and returns the original value before the operation.

## Definition
static inline uint64
pg_atomic_fetch_and_u64(volatile pg_atomic_uint64 *ptr, uint64 and_)

## Detailed Description
This function provides an atomic fetch-and-AND operation for 64-bit unsigned integers. It atomically performs a bitwise AND between the target variable and the specified mask, then returns the original value that was present before the operation. The operation is thread-safe and ensures that no other thread can interfere with the read-modify-write sequence.

The function acts as a wrapper around the platform-specific implementation pg_atomic_fetch_and_u64_impl, providing a consistent interface across different architectures. This operation is commonly used for clearing specific bits in a bitmask or implementing atomic flag operations.

## Parameters / Member Variables
- ptr: Pointer to the atomic 64-bit unsigned integer variable to be modified
- and_: The 64-bit unsigned integer mask to AND with the target variable

## Dependencies
- Functions called/Symbols referenced:
  - AssertPointerAlignment (for pointer alignment verification when not using simulation)
  - pg_atomic_fetch_and_u64_impl (platform-specific implementation)
- Called from (representative examples):
  - test_atomic_uint64 (regression testing - multiple test cases)

## Notes and Other Information
- Returns the original value before the AND operation, not the result after the operation
- The function includes pointer alignment assertions to ensure proper memory alignment for atomic operations
- On GCC-compatible platforms, the implementation uses __sync_fetch_and_and builtin for optimal performance
- Commonly used for atomic bit clearing operations where specific bits need to be turned off
- The and_ parameter is unsigned, as bitwise operations typically work with unsigned values
- This is part of PostgreSQLs portable atomic operations interface, providing consistent behavior across different hardware architectures
- Primarily used in testing scenarios within the current PostgreSQL codebase, indicating its specialized nature for low-level bit manipulation