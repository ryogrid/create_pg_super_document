# pg_atomic_fetch_or_u32

## Location
[src/include/port/atomics.h:405-418](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/port/atomics.h#L405-L418)

## Overview
Atomically performs a bitwise OR operation on a 32-bit unsigned integer variable and returns the original value before the operation.

## Definition

```c
static inline uint32
pg_atomic_fetch_or_u32(volatile pg_atomic_uint32 *ptr, uint32 or_)
```
## Detailed Description
This function provides an atomic fetch-and-OR operation on a 32-bit unsigned integer. It atomically performs a bitwise OR operation between the value stored at the memory location pointed to by  and the value , then returns the original value that was stored at  before the operation. The function guarantees full memory barrier semantics, ensuring proper ordering of memory operations across threads.

The function serves as a high-level wrapper around the platform-specific implementation , providing a consistent interface across different architectures while ensuring proper pointer alignment.

## Parameters / Member Variables
- `*ptr`: Pointer to the atomic 32-bit unsigned integer variable to be modified
- `or_`: The value to be bitwise ORed with the variable
## Dependencies
- Functions called/Symbols referenced:
  - AssertPointerAlignment
  - [pg_atomic_fetch_or_u32_impl](pg_atomic_fetch_or_u32_impl.md)
  - [pg_atomic_uint32](pg_atomic_uint32.md) (type)
- Called from (representative examples):
  - [LockBufHdr](../L/LockBufHdr.md)
  - [EmitProcSignalBarrier](../E/EmitProcSignalBarrier.md)
  - [ResetProcSignalBarrierBits](../R/ResetProcSignalBarrierBits.md)
  - [LWLockWaitListLock](../L/LWLockWaitListLock.md)
  - [LWLockQueueSelf](../L/LWLockQueueSelf.md)
  - [LWLockDequeueSelf](../L/LWLockDequeueSelf.md)
  - [LWLockAcquire](../L/LWLockAcquire.md)
  - [LWLockWaitForVar](../L/LWLockWaitForVar.md)

## Notes and Other Information
- The function enforces 4-byte alignment for the pointer parameter through AssertPointerAlignment
- Provides full barrier semantics, making it suitable for synchronization primitives
- Commonly used in PostgreSQL's locking mechanisms and signal handling
- The inline nature of the function ensures minimal overhead while maintaining atomicity guarantees
- Used extensively in buffer management and lightweight lock implementations

## Simplified Source

```c
// Simplified version of pg_atomic_fetch_or_u32
static inline uint32 pg_atomic_fetch_or_u32(volatile pg_atomic_uint32 *ptr, uint32 or_) {
    // Verify proper alignment for atomic operations
    AssertPointerAlignment(ptr, 4);

    // Perform atomic fetch-and-OR operation
    return pg_atomic_fetch_or_u32_impl(ptr, or_);
}
```

Key simplifications made:
- Added comments explaining the alignment check and atomic operation
- Maintained the complete function as it's already very simple and focused
- No significant simplification needed due to the function's inherent simplicity
- Preserved all essential safety checks and atomic operation semantics