# ShmemAllocUnlocked

## Location
[src/backend/storage/ipc/shmem.c:238-273](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/shmem.c#L238-L273)

## Overview
ShmemAllocUnlocked allocates a max-aligned chunk from shared memory without acquiring the ShmemLock, designed specifically for allocations that must occur before ShmemLock is ready.

## Definition

```c
void *
ShmemAllocUnlocked(Size size)
```
## Detailed Description
This function provides a mechanism for allocating shared memory during the early stages of PostgreSQL initialization when the shared memory lock (ShmemLock) is not yet available. It operates directly on the shared memory segment header to track and allocate space. The function ensures proper alignment using MAXALIGN rather than CACHEALIGN, which is considered sufficient for early initialization purposes.

The allocation process involves:
1. Aligning the requested size to maximum alignment boundaries
2. Calculating the new memory offset based on current free offset
3. Verifying sufficient space is available in the shared memory segment
4. Updating the shared memory header's free offset
5. Returning a pointer to the allocated space

If insufficient shared memory is available, the function raises an ERROR with an out-of-memory message.

## Parameters / Member Variables
- : The number of bytes to allocate from shared memory

## Dependencies
- Functions called/Symbols referenced:
  - MAXALIGN (macro for memory alignment)
  - Assert (assertion macro)
  - ereport (error reporting function)
  - [errcode](../e/errcode.md)/errmsg (error reporting macros)
- Called from (representative examples):
  - [PGReserveSemaphores](../P/PGReserveSemaphores.md) (in posix_sema.c and sysv_sema.c)
  - [InitShmemAllocation](../I/InitShmemAllocation.md)
  - [SpinlockSemaInit](SpinlockSemaInit.md)

## Notes and Other Information
- This function should ONLY be used for allocations that must happen before ShmemLock is ready
- Uses MAXALIGN instead of CACHEALIGN for alignment, which is sufficient for early initialization
- Direct manipulation of ShmemSegHdr without locking makes this function unsafe for general use
- The function assumes ShmemSegHdr is already initialized and not NULL
- Memory allocated is permanent and cannot be freed back to the shared memory pool