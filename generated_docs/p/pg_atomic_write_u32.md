# pg_atomic_write_u32

## Location
src/include/port/atomics.h: 271 - 289

## Overview
An atomic write function that guarantees a complete write to a 32-bit atomic variable, ensuring no partial writes can be observed by any reader.

## Definition
static inline void pg_atomic_write_u32(volatile pg_atomic_uint32 *ptr, uint32 val)

## Detailed Description
pg_atomic_write_u32 is a static inline function that performs an atomic write to a 32-bit unsigned integer atomic variable. The function guarantees that the write will succeed as a whole, meaning it's not possible for any reader to observe a partial write. This function correctly interacts with pg_atomic_compare_exchange_u32, unlike pg_atomic_unlocked_write_u32 which may have race conditions. The function provides no memory barrier semantics, making it suitable for cases where atomicity is required but memory ordering is not critical.

The function includes pointer alignment verification to ensure the atomic variable is properly aligned for 32-bit operations.

## Parameters / Member Variables
- ptr: A pointer to a volatile pg_atomic_uint32 atomic variable to write to
- val: The uint32 value to write to the atomic variable

## Dependencies
- Functions called/Symbols referenced:
  - AssertPointerAlignment (for 4-byte alignment verification)
  - [pg_atomic_write_u32_impl](pg_atomic_write_u32_impl.md) (platform-specific implementation)
- Called from (representative examples):
  - TransactionGroupUpdateXidStatus (transaction processing)
  - [parallel_vacuum_process_all_indexes](parallel_vacuum_process_all_indexes.md) (parallel vacuum operations)
  - [ProcArrayGroupClearXid](../P/ProcArrayGroupClearXid.md) (process array management)
  - [UnlockBufHdr](../U/UnlockBufHdr.md) (buffer header unlocking)
  - [InjectionPointAttach](../I/InjectionPointAttach.md)/Detach (injection point management)

## Notes and Other Information
- No barrier semantics are provided, meaning this operation does not enforce memory ordering
- Guarantees atomicity - no partial writes can be observed
- Compatible with pg_atomic_compare_exchange_u32 operations
- Proper pointer alignment (4 bytes) is verified through AssertPointerAlignment
- Widely used throughout PostgreSQL for updating atomic counters, flags, and status values
- Part of PostgreSQL's portable atomic operations abstraction layer
- Safer than pg_atomic_unlocked_write_u32 for concurrent access scenarios