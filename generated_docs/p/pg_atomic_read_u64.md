# pg_atomic_read_u64

## Location
src/include/port/atomics.h: 462 - 470

## Overview
Atomically reads the current value of a 64-bit unsigned integer variable, providing thread-safe access without modifying the value.

## Definition
```c
static inline uint64
pg_atomic_read_u64(volatile pg_atomic_uint64 *ptr)
```

## Detailed Description
This function provides an atomic read operation for a 64-bit unsigned integer variable. It safely retrieves the current value stored at the memory location pointed to by `ptr` without the risk of reading a partially updated value during concurrent modifications by other threads. The function serves as a high-level wrapper around the platform-specific implementation `pg_atomic_read_u64_impl`, providing a consistent interface across different architectures.

Similar to the initialization function, it includes conditional pointer alignment enforcement based on whether the platform uses hardware atomic operations or a spinlock-based simulation. When hardware atomics are available, it enforces 8-byte alignment for optimal performance.

## Parameters / Member Variables
- `ptr`: Pointer to the atomic 64-bit unsigned integer variable to be read

## Dependencies
- Functions called/Symbols referenced:
  - AssertPointerAlignment (conditionally)
  - pg_atomic_read_u64_impl
  - pg_atomic_uint64 (type)
  - PG_HAVE_ATOMIC_U64_SIMULATION (preprocessor macro)
- Called from (representative examples):
  - SlruSelectLRUPage
  - RefreshXLogWriteResult
  - GetXLogBuffer
  - WALReadFromBuffers
  - AdvanceXLInsertBuffer
  - XLogWrite
  - XLogPrefetchIncrement
  - GetWalRcvWriteRecPtr
  - WaitForProcSignalBarrier
  - shm_mq_send_bytes
  - shm_mq_receive_bytes

## Notes and Other Information
- Conditionally enforces 8-byte alignment only when hardware atomic operations are available (not using spinlock simulation)
- Used extensively throughout PostgreSQL's core systems including transaction logging, shared memory queues, and signal handling
- Critical for safe reading of counters, pointers, and state variables in multi-process environments
- The inline nature of the function ensures minimal overhead during read operations
- Platform-specific behavior: alignment requirements depend on the underlying atomic implementation
- Unlike simple memory reads, this function guarantees that the returned value represents a consistent snapshot at a specific point in time
- Commonly used for reading WAL positions, buffer states, and inter-process communication variables
- Essential for implementing lock-free algorithms and wait-free data structures in PostgreSQL