# pg_atomic_init_u64

## Location
[src/include/port/atomics.h:448-461](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/port/atomics.h#L448-L461)

## Overview
Initializes a 64-bit atomic unsigned integer variable with a specified value, providing thread-safe initialization.

## Definition
```c
static inline void
pg_atomic_init_u64(volatile pg_atomic_uint64 *ptr, uint64 val)
```

## Detailed Description
This function provides safe initialization of a 64-bit atomic unsigned integer variable. It sets the value of the atomic variable pointed to by `ptr` to the initial value `val`. The function serves as a high-level wrapper around the platform-specific implementation `pg_atomic_init_u64_impl`, providing a consistent interface across different architectures.

The function includes conditional pointer alignment enforcement based on whether the platform uses hardware atomic operations or a spinlock-based simulation. When hardware atomics are available, it enforces 8-byte alignment for optimal performance. When using the spinlock fallback implementation, alignment is not enforced as it's not necessary for the simulation approach.

## Parameters / Member Variables
- `ptr`: Pointer to the atomic 64-bit unsigned integer variable to be initialized
- `val`: The initial 64-bit unsigned integer value to set

## Dependencies
- Functions called/Symbols referenced:
  - AssertPointerAlignment (conditionally)
  - [pg_atomic_init_u64_impl](pg_atomic_init_u64_impl.md)
  - [pg_atomic_uint64](pg_atomic_uint64.md) (type)
  - PG_HAVE_ATOMIC_U64_SIMULATION (preprocessor macro)
- Called from (representative examples):
  - [table_block_parallelscan_initialize](../t/table_block_parallelscan_initialize.md)
  - [SimpleLruInit](../S/SimpleLruInit.md)
  - [MarkAsPreparingGuts](../M/MarkAsPreparingGuts.md)
  - [XLOGShmemInit](../X/XLOGShmemInit.md)
  - [XLogPrefetchShmemInit](../X/XLogPrefetchShmemInit.md)
  - [WalRcvShmemInit](../W/WalRcvShmemInit.md)
  - [ProcSignalShmemInit](../P/ProcSignalShmemInit.md)
  - [shm_mq_create](../s/shm_mq_create.md)
  - [InitProcGlobal](../I/InitProcGlobal.md)
  - [StatsShmemInit](../S/StatsShmemInit.md)

## Notes and Other Information
- Conditionally enforces 8-byte alignment only when hardware atomic operations are available (not using spinlock simulation)
- Used extensively in PostgreSQL's shared memory initialization routines
- Common in transaction log management, parallel scanning, and inter-process communication
- The inline nature of the function ensures minimal overhead during initialization
- Platform-specific behavior: alignment requirements depend on the underlying atomic implementation
- Unlike the 32-bit counterpart, this function may use spinlock-based simulation on platforms without native 64-bit atomic support
- Critical for initializing shared memory structures that require atomic access across multiple processes

## Simplified Source

```c
// Simplified version of pg_atomic_init_u64
static inline void pg_atomic_init_u64(volatile pg_atomic_uint64 *ptr, uint64 val) {
    // Core logic step 1: Enforce alignment when using hardware atomics
#ifndef PG_HAVE_ATOMIC_U64_SIMULATION
    AssertPointerAlignment(ptr, 8);
#endif

    // Core logic step 2: Initialize the atomic variable using platform implementation
    pg_atomic_init_u64_impl(ptr, val);
}
```

Key simplifications made:
- Removed detailed comments about alignment requirements in different implementations
- Focused on the two core steps: conditional alignment check and actual initialization
- Maintained the essential conditional compilation for alignment enforcement
- Simplified to show the wrapper nature around the platform-specific implementation