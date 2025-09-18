# pg_atomic_exchange_u32

## Location
src/include/port/atomics.h: 325 - 343

## Overview
Atomically exchanges a 32-bit unsigned integer value with a new value and returns the previous value, providing full memory barrier semantics.

## Definition
```c
static inline uint32
pg_atomic_exchange_u32(volatile pg_atomic_uint32 *ptr, uint32 newval)
```

## Detailed Description
This function performs an atomic exchange operation on a 32-bit unsigned integer. It atomically replaces the value pointed to by `ptr` with `newval` and returns the original value that was stored at that location. The operation is guaranteed to be atomic and provides full memory barrier semantics, ensuring that all memory operations before this call are completed before the exchange, and all memory operations after this call happen after the exchange.

The function is implemented as a thin wrapper around `pg_atomic_exchange_u32_impl`, which contains the platform-specific implementation. It includes pointer alignment assertions to ensure the atomic variable is properly aligned for efficient atomic operations.

## Parameters / Member Variables
- `ptr`: Pointer to the atomic 32-bit unsigned integer variable to be exchanged
- `newval`: The new 32-bit unsigned integer value to store in the atomic variable

## Dependencies
- Functions called/Symbols referenced:
  - pg_atomic_uint32 (type definition)
  - AssertPointerAlignment (alignment check)
  - pg_atomic_exchange_u32_impl (platform-specific implementation)
- Called from (representative examples):
  - TransactionGroupUpdateXidStatus (src/backend/access/transam/clog.c:570)
  - pgarch_readyXlog (src/backend/postmaster/pgarch.c:653)
  - StrategySyncStart (src/backend/storage/buffer/freelist.c:416)
  - ProcArrayGroupClearXid (src/backend/storage/ipc/procarray.c:854)
  - ProcessProcSignalBarrier (src/backend/storage/ipc/procsignal.c:508)

## Notes and Other Information
- Provides full barrier semantics, making it suitable for synchronization primitives
- The pointer must be 4-byte aligned as enforced by AssertPointerAlignment
- This is a platform-independent interface that delegates to platform-specific implementations
- Commonly used in PostgreSQL for implementing lock-free data structures and coordination between processes
- The atomic exchange is fundamental for implementing compare-and-swap loops and other synchronization patterns