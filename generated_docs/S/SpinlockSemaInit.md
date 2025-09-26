# SpinlockSemaInit

## Location
[src/backend/storage/lmgr/spin.c:77-113](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/spin.c#L77-L113)

## Overview
Initializes spinlock emulation by allocating and creating the required semaphores in shared memory.

## Definition
```c
void SpinlockSemaInit(void)
```

## Detailed Description
This function performs the initialization of spinlock emulation infrastructure during system startup. It allocates shared memory space for the semaphore array and creates individual semaphores that will be used to emulate spinlock functionality on systems where hardware spinlocks are not available. The function must be called after PGReserveSemaphores() to ensure proper semaphore resource allocation.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [PGSemaphore](../P/PGSemaphore.md) (semaphore structure type)
  - [SpinlockSemas](SpinlockSemas.md)() (gets number of semaphores needed)
  - [ShmemAllocUnlocked](ShmemAllocUnlocked.md)() (allocates shared memory without spinlock protection)
  - [SpinlockSemaSize](SpinlockSemaSize.md)() (gets total memory size needed)
  - [PGSemaphoreCreate](../P/PGSemaphoreCreate.md)() (creates individual semaphores)
  - SpinlockSemaArray (global array to store semaphores)
- Called from:
  - [CreateSharedMemoryAndSemaphores](../C/CreateSharedMemoryAndSemaphores.md) (in src/backend/storage/ipc/ipci.c:236)
  - SpinLockFree (in src/include/storage/spin.h:73)

## Notes and Other Information
- Must be called after PGReserveSemaphores() to ensure semaphore resources are properly reserved
- Uses ShmemAllocUnlocked() because normal ShmemAlloc() requires spinlock protection which is not yet available
- Creates a global SpinlockSemaArray that contains all semaphores used for spinlock emulation
- Part of the bootstrap process for systems requiring spinlock emulation
- Each semaphore in the array will be used to provide mutual exclusion equivalent to hardware spinlocks