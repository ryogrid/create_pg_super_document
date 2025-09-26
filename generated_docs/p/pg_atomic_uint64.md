# pg_atomic_uint64

## Location
[src/include/port/atomics/arch-x86.h:74-78](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/port/atomics/arch-x86.h#L74-L78)

## Overview
An atomic 64-bit unsigned integer structure that provides lock-free operations for concurrent access to large integer values, particularly used for LSNs, counters, and pointers in PostgreSQL's high-performance subsystems.

## Definition

```c
typedef struct pg_atomic_uint64
{
	/* alignment guaranteed due to being on a 64bit platform */
	volatile uint64 value;
} pg_atomic_uint64;
```
## Detailed Description
The `pg_atomic_uint64` struct is PostgreSQL's atomic type for 64-bit unsigned integers, designed for high-performance lock-free operations on large values such as Log Sequence Numbers (LSNs), transaction IDs, and memory pointers. This structure is critical for PostgreSQL's write-ahead logging (WAL) system, replication, and other subsystems that require atomic operations on 64-bit values.

The struct includes an 8-byte alignment attribute to ensure proper atomic access on all supported platforms. This alignment is crucial because many architectures require 64-bit values to be aligned on 8-byte boundaries for atomic operations to work correctly. The implementation provides comprehensive atomic operations including read, write, compare-and-exchange, and arithmetic operations, all with appropriate memory barrier semantics.

Unlike the 32-bit variant, 64-bit atomic operations may be simulated using locks on some platforms where native 64-bit atomics are not available, making this type conditionally available based on PG_HAVE_ATOMIC_U64_SIMULATION compilation flags.

## Parameters / Member Variables
- `value`: A volatile 64-bit unsigned integer field that stores the atomic value, aligned to 8-byte boundaries using pg_attribute_aligned(8). The volatile qualifier ensures the compiler doesn't optimize away memory accesses, while the alignment attribute ensures proper atomic operation support.

## Dependencies
- Functions called/Symbols referenced:
  - uint64 (base type)
  - [pg_attribute_aligned](pg_attribute_aligned.md) (alignment directive)
- Called from (representative examples):
  - [pg_atomic_init_u64](pg_atomic_init_u64.md)
  - [pg_atomic_read_u64](pg_atomic_read_u64.md)
  - [pg_atomic_write_u64](pg_atomic_write_u64.md)
  - [pg_atomic_compare_exchange_u64](pg_atomic_compare_exchange_u64.md)
  - [pg_atomic_fetch_add_u64](pg_atomic_fetch_add_u64.md)
  - [pg_atomic_monotonic_advance_u64](pg_atomic_monotonic_advance_u64.md)
  - [XLogCtlData](../X/XLogCtlData.md) (WAL control structure for LSN tracking)
  - [LWLockWaitForVar](../L/LWLockWaitForVar.md) (lightweight lock condition variables)
  - [shm_mq](../s/shm_mq.md) (shared memory message queues)
  - [ProcSignalSlot](../P/ProcSignalSlot.md) (process signaling)

## Notes and Other Information
- Extensively used in PostgreSQL's WAL system for tracking LSNs (Log Sequence Numbers) and write positions
- Critical for replication and recovery systems where 64-bit LSN values must be atomically updated
- Employed in lightweight lock condition variable implementation for efficient waiting mechanisms
- Used in shared memory queue implementations for atomic head/tail pointer management
- Requires 8-byte alignment on most platforms, enforced through pg_attribute_aligned(8)
- May fall back to lock-based simulation on platforms without native 64-bit atomic support
- Supports specialized operations like pg_atomic_monotonic_advance_u64 for LSN advancement
- The AssertPointerAlignment(ptr, 8) checks ensure proper alignment at runtime
- Particularly important for maintaining consistency in high-throughput transaction logging scenarios
- Part of PostgreSQL's strategy to minimize lock contention in performance-critical paths