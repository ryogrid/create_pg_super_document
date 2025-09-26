# SpinlockSemas

## Location
src/backend/storage/lmgr/spin.c: 64 - 76

## Overview
Reports the number of semaphores needed to support spinlock emulation when hardware spinlocks are not available.

## Definition
```c
int SpinlockSemas(void)
```

## Detailed Description
This function returns the constant value NUM_EMULATION_SEMAPHORES, which defines how many semaphores are required for spinlock emulation. This count is used by various parts of the system to allocate the appropriate number of semaphores during shared memory initialization and to manage spinlock resources.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - NUM_EMULATION_SEMAPHORES (constant defining number of emulation semaphores)
- Called from:
  - CalculateShmemSize (in src/backend/storage/ipc/ipci.c:97)
  - SpinlockSemaInit (in src/backend/storage/lmgr/spin.c:80)
  - SpinLockFree (in src/include/storage/spin.h:69)

## Notes and Other Information
- This function works in conjunction with SpinlockSemaSize() to determine both the count and total memory requirements for spinlock emulation
- The returned value is used during system initialization to allocate the correct number of semaphores
- Only relevant when hardware spinlocks are not available and semaphore-based emulation is needed