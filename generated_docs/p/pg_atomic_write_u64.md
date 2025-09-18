# pg_atomic_write_u64

## Location
src/include/port/atomics.h: 480 - 488

## Overview
Performs an atomic write of a 64-bit unsigned integer value to a memory location, ensuring the operation is indivisible and thread-safe.

## Definition
```c
static inline void
pg_atomic_write_u64(volatile pg_atomic_uint64 *ptr, uint64 val)
```

## Detailed Description
This function provides a thread-safe way to atomically write a 64-bit unsigned integer value to a specified memory location. The atomic write operation guarantees that the entire 64-bit value is written as a single, indivisible operation, preventing other threads from observing partially written values.

The function is implemented as an inline wrapper around the platform-specific implementation `pg_atomic_write_u64_impl()`. When atomic 64-bit operations are not simulated (PG_HAVE_ATOMIC_U64_SIMULATION is not defined), it includes pointer alignment assertions to ensure the atomic variable is properly aligned on an 8-byte boundary, which is required for efficient atomic operations on most architectures.

## Parameters / Member Variables
- `ptr`: A pointer to the volatile atomic 64-bit unsigned integer location where the value will be written. Must be 8-byte aligned when not using simulation.
- `val`: The 64-bit unsigned integer value to be written atomically to the specified location.

## Dependencies
- Functions called/Symbols referenced:
  - `[pg_atomic_write_u64_impl](pg_atomic_write_u64_impl.md)`
  - `AssertPointerAlignment` (when PG_HAVE_ATOMIC_U64_SIMULATION is not defined)
  - `[pg_atomic_uint64](pg_atomic_uint64.md)` (type)
  - `PG_HAVE_ATOMIC_U64_SIMULATION` (macro)
- Called from (representative examples):
  - `[table_block_parallelscan_reinitialize](../t/table_block_parallelscan_reinitialize.md)` (src/backend/access/table/tableam.c:411)
  - `[StartupCLOG](../S/StartupCLOG.md)` (src/backend/access/transam/clog.c:885)
  - `[AdvanceXLInsertBuffer](../A/AdvanceXLInsertBuffer.md)` (src/backend/access/transam/xlog.c:2087)
  - `[XLogWrite](../X/XLogWrite.md)` (src/backend/access/transam/xlog.c:2583)
  - `InitProcess` (src/backend/storage/lmgr/proc.c:407)

## Notes and Other Information
- This function provides basic atomic write semantics without additional memory barrier guarantees
- The pointer alignment requirement (8-byte boundary) is enforced through assertions in debug builds
- Widely used throughout PostgreSQL for updating atomic counters, state variables, and synchronization primitives
- The volatile qualifier on the pointer parameter prevents compiler optimizations that might eliminate or cache the memory access
- This function is part of PostgreSQL's portable atomic operations interface, providing consistent behavior across different platforms and architectures