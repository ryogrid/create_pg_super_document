# pg_atomic_fetch_add_u64

## Location
[src/include/port/atomics.h:517-525](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/port/atomics.h#L517-L525)

## Overview
Atomically adds a value to a 64-bit unsigned integer and returns the original value before the addition.

## Definition

```c
static inline uint64
pg_atomic_fetch_add_u64(volatile pg_atomic_uint64 *ptr, int64 add_)
```
## Detailed Description
This function provides an atomic fetch-and-add operation for 64-bit unsigned integers. It atomically adds the specified value to the target variable and returns the original value that was present before the addition. The operation is thread-safe and ensures that no other thread can interfere with the read-modify-write sequence.

The function acts as a wrapper around the platform-specific implementation , providing a consistent interface across different architectures. On platforms without native 64-bit atomic support, it falls back to a spinlock-based implementation to ensure atomicity.

## Parameters / Member Variables
- : Pointer to the atomic 64-bit unsigned integer variable to be modified
- : The signed 64-bit value to add to the target variable (can be negative for subtraction)

## Dependencies
- Functions called/Symbols referenced:
  - AssertPointerAlignment (for pointer alignment verification when not using simulation)
  - [pg_atomic_fetch_add_u64_impl](pg_atomic_fetch_add_u64_impl.md) (platform-specific implementation)
- Called from (representative examples):
  - [table_block_parallelscan_nextpage](../t/table_block_parallelscan_nextpage.md) (parallel table scanning)
  - [GetFakeLSNForUnloggedRel](../G/GetFakeLSNForUnloggedRel.md) (transaction log sequence number generation)
  - pgstat_request_entry_refs_gc (statistics entry reference counting)
  - dsa_pointer_atomic_fetch_add (dynamic shared area pointer arithmetic)
  - [test_atomic_uint64](../t/test_atomic_uint64.md) (regression testing)

## Notes and Other Information
- Returns the original value before the addition, not the result after addition
- The function includes pointer alignment assertions to ensure proper memory alignment for atomic operations
- On platforms without native 64-bit atomic support (PG_HAVE_ATOMIC_U64_SIMULATION), the implementation uses spinlocks to provide atomicity
- The add_ parameter is signed, allowing both addition (positive values) and subtraction (negative values)
- This is part of PostgreSQL's portable atomic operations interface, providing consistent behavior across different hardware architectures