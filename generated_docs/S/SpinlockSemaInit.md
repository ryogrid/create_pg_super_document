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

## Simplified Source

```c
// Simplified version of SpinlockSemaInit
void SpinlockSemaInit(void) {
    // Step 1: Determine how many semaphores we need
    int nsemas = SpinlockSemas();

    // Step 2: Allocate shared memory for the semaphore array
    // Note: Uses unlocked allocation since spinlocks aren't ready yet
    PGSemaphore *spinsemas = (PGSemaphore *) ShmemAllocUnlocked(SpinlockSemaSize());

    // Step 3: Create each individual semaphore
    for (int i = 0; i < nsemas; i++) {
        spinsemas[i] = PGSemaphoreCreate();
    }

    // Step 4: Store the array globally for use by spinlock operations
    SpinlockSemaArray = spinsemas;
}
```

Key simplifications made:
- Combined variable declarations with their usage for clarity
- Added step-by-step comments explaining the logical flow
- Simplified the loop variable declaration to modern C style
- Removed detailed implementation comments in favor of high-level explanations
- Focused on the core algorithm: calculate size, allocate memory, create semaphores, store globally