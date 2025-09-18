# pg_atomic_unlocked_write_u32

## Location
src/include/port/atomics.h: 290 - 309

## Overview
An unlocked atomic write function that provides high-performance writes with atomicity guarantees but without proper interaction with read-modify-write operations.

## Definition
static inline void pg_atomic_unlocked_write_u32(volatile pg_atomic_uint32 *ptr, uint32 val)

## Detailed Description
pg_atomic_unlocked_write_u32 is a static inline function that performs an unlocked atomic write to a 32-bit unsigned integer atomic variable. The function guarantees that the write will succeed as a whole, meaning no partial writes can be observed by any reader. However, this function is not guaranteed to correctly interact with read-modify-write operations like pg_atomic_compare_exchange_u32, which can lead to race conditions. This function should only be used in performance-critical scenarios where minor performance regressions due to atomic emulation are unacceptable and where no concurrent read-modify-write operations will occur.

The function provides no memory barrier semantics and includes pointer alignment verification.

## Parameters / Member Variables
- ptr: A pointer to a volatile pg_atomic_uint32 atomic variable to write to
- val: The uint32 value to write to the atomic variable

## Dependencies
- Functions called/Symbols referenced:
  - AssertPointerAlignment (for 4-byte alignment verification)
  - [pg_atomic_unlocked_write_u32_impl](pg_atomic_unlocked_write_u32_impl.md) (platform-specific implementation)
- Called from (representative examples):
  - [ZeroAndLockBuffer](../Z/ZeroAndLockBuffer.md) (buffer initialization)
  - [WaitReadBuffers](../W/WaitReadBuffers.md) (buffer read operations)
  - LocalBufferAlloc (local buffer allocation)
  - ExtendBufferedRelLocal (local buffer extension)
  - [FlushRelationBuffers](../F/FlushRelationBuffers.md) (buffer flushing operations)

## Notes and Other Information
- No barrier semantics are provided, meaning this operation does not enforce memory ordering
- Does NOT correctly interact with read-modify-write operations like pg_atomic_compare_exchange_u32
- Higher performance than pg_atomic_write_u32 due to relaxed synchronization
- Should only be used when performance is critical and no RMW operations are expected
- Proper pointer alignment (4 bytes) is verified through AssertPointerAlignment
- Primarily used in buffer management code where performance is crucial
- Part of PostgreSQL's portable atomic operations abstraction layer
- Use with caution - prefer pg_atomic_write_u32 unless performance is absolutely critical