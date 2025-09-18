# pg_atomic_fetch_add_u32

## Location
src/include/port/atomics.h: 361 - 375

## Overview
Atomically adds a signed 32-bit integer value to a 32-bit unsigned atomic variable and returns the original value before the addition.

## Definition
```c
static inline uint32
pg_atomic_fetch_add_u32(volatile pg_atomic_uint32 *ptr, int32 add_)
```

## Detailed Description
This function performs an atomic fetch-and-add operation on a 32-bit unsigned integer. It atomically adds the signed value `add_` to the value pointed to by `ptr` and returns the original value that was stored at that location before the addition. The addition is performed using modular arithmetic, so overflow wraps around according to standard C semantics.

The operation provides full memory barrier semantics, ensuring that all memory operations before this call are completed before the addition, and all memory operations after this call happen after the addition. This function is particularly useful for implementing atomic counters, reference counting, and other scenarios where you need to atomically increment or decrement a value and also need to know the previous value.

The function is implemented as a wrapper around `pg_atomic_fetch_add_u32_impl`, which contains the platform-specific implementation.

## Parameters / Member Variables
- `ptr`: Pointer to the atomic 32-bit unsigned integer variable to be modified
- `add_`: The signed 32-bit integer value to add to the atomic variable (can be negative for subtraction)

## Dependencies
- Functions called/Symbols referenced:
  - pg_atomic_uint32 (type definition)
  - AssertPointerAlignment (alignment check)
  - pg_atomic_fetch_add_u32_impl (platform-specific implementation)
- Called from (representative examples):
  - parallel_vacuum_process_safe_indexes (src/backend/commands/vacuumparallel.c:787)
  - ExecParallelHashJoinNewBatch (src/backend/executor/nodeHashjoin.c:1195)
  - ClockSweepTick (src/backend/storage/buffer/freelist.c:118)
  - StrategyGetBuffer (src/backend/storage/buffer/freelist.c:250)
  - LWLockQueueSelf (src/backend/storage/lmgr/lwlock.c:1069)

## Notes and Other Information
- Returns the value that was stored before the addition operation
- The `add_` parameter is signed, allowing both addition (positive values) and subtraction (negative values)
- Provides full barrier semantics, making it suitable for synchronization and coordination
- The pointer must be 4-byte aligned as enforced by AssertPointerAlignment
- Commonly used for implementing atomic counters, reference counts, and statistics collection
- Overflow behavior follows standard C arithmetic (wraps around for unsigned integers)
- This is a fundamental building block for lock-free algorithms that need to track counts or perform atomic arithmetic operations