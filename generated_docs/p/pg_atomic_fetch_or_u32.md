# pg_atomic_fetch_or_u32

## Location
[src/include/port/atomics.h:405-418](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/port/atomics.h#L405-L418)

## Overview
Atomically performs a bitwise OR operation on a 32-bit unsigned integer variable and returns the original value before the operation.

## Definition


## Detailed Description
This function provides an atomic fetch-and-OR operation on a 32-bit unsigned integer. It atomically performs a bitwise OR operation between the value stored at the memory location pointed to by  and the value , then returns the original value that was stored at  before the operation. The function guarantees full memory barrier semantics, ensuring proper ordering of memory operations across threads.

The function serves as a high-level wrapper around the platform-specific implementation , providing a consistent interface across different architectures while ensuring proper pointer alignment.

## Parameters / Member Variables
- : Pointer to the atomic 32-bit unsigned integer variable to be modified
- : The value to be bitwise ORed with the variable

## Dependencies
- Functions called/Symbols referenced:
  - AssertPointerAlignment
  - [pg_atomic_fetch_or_u32_impl](pg_atomic_fetch_or_u32_impl.md)
  - [pg_atomic_uint32](pg_atomic_uint32.md) (type)
- Called from (representative examples):
  - LockBufHdr
  - [EmitProcSignalBarrier](../E/EmitProcSignalBarrier.md)
  - [ResetProcSignalBarrierBits](../R/ResetProcSignalBarrierBits.md)
  - LWLockWaitListLock
  - LWLockQueueSelf
  - LWLockDequeueSelf
  - LWLockAcquire
  - LWLockWaitForVar

## Notes and Other Information
- The function enforces 4-byte alignment for the pointer parameter through AssertPointerAlignment
- Provides full barrier semantics, making it suitable for synchronization primitives
- Commonly used in PostgreSQL's locking mechanisms and signal handling
- The inline nature of the function ensures minimal overhead while maintaining atomicity guarantees
- Used extensively in buffer management and lightweight lock implementations