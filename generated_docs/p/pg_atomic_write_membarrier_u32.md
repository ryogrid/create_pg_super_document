# pg_atomic_write_membarrier_u32

## Location
[src/include/port/atomics.h:310-324](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/port/atomics.h#L310-L324)

## Overview
An atomic write function with full memory barrier semantics that guarantees complete writes and proper interaction with other barrier-semantic operations.

## Definition
static inline void pg_atomic_write_membarrier_u32(volatile pg_atomic_uint32 *ptr, uint32 val)

## Detailed Description
pg_atomic_write_membarrier_u32 is a static inline function that performs an atomic write to a 32-bit unsigned integer atomic variable with full memory barrier semantics. The function guarantees that the write will succeed as a whole, meaning no partial writes can be observed by any reader. This function correctly interacts with both pg_atomic_compare_exchange_u32 and pg_atomic_read_membarrier_u32, providing consistent barrier semantics across operations. While this function may be less performant than pg_atomic_write_u32 due to the barrier overhead, it provides stronger memory ordering guarantees and may be easier to reason about in terms of correctness, making it suitable for less performance-sensitive code where memory ordering is critical.

The function includes pointer alignment verification and provides full barrier semantics, ensuring proper memory ordering with respect to other memory operations.

## Parameters / Member Variables
- ptr: A pointer to a volatile pg_atomic_uint32 atomic variable to write to
- val: The uint32 value to write to the atomic variable

## Dependencies
- Functions called/Symbols referenced:
  - AssertPointerAlignment (for 4-byte alignment verification)
  - [pg_atomic_write_membarrier_u32_impl](pg_atomic_write_membarrier_u32_impl.md) (platform-specific implementation with barrier semantics)
- Called from (representative examples):
  - [PgArchForceDirScan](../P/PgArchForceDirScan.md) (PostgreSQL archiver directory scanning)

## Notes and Other Information
- Provides full memory barrier semantics, ensuring proper memory ordering
- Guarantees atomicity - no partial writes can be observed
- Compatible with pg_atomic_compare_exchange_u32 and pg_atomic_read_membarrier_u32
- More expensive than pg_atomic_write_u32 due to barrier overhead
- Suitable for correctness-critical code where memory ordering matters
- Proper pointer alignment (4 bytes) is verified through AssertPointerAlignment
- Limited usage in codebase, primarily in archiver components
- Part of PostgreSQL's portable atomic operations abstraction layer
- Should be used in conjunction with other barrier-semantic atomic operations for consistency