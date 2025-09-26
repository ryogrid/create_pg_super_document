# s_check_valid

## Location
src/backend/storage/lmgr/spin.c: 114 - 120

## Overview
Validates that a spinlock index is within the valid range for spinlock emulation semaphores.

## Definition
```c
static inline void s_check_valid(int lockndx)
```

## Detailed Description
This inline function performs bounds checking on spinlock indices used in the semaphore-based spinlock emulation system. It ensures that the provided lock index is within the valid range of 1 to NUM_EMULATION_SEMAPHORES. The function uses an error log to report invalid indices, which helps catch programming errors where spinlocks have not been properly initialized or where invalid indices are being used.

## Parameters / Member Variables
- `lockndx`: The spinlock index to validate (must be between 1 and NUM_EMULATION_SEMAPHORES inclusive)

## Dependencies
- Functions called/Symbols referenced:
  - NUM_EMULATION_SEMAPHORES (constant defining the maximum number of emulation semaphores)
  - unlikely() (compiler optimization hint macro)
  - elog() (PostgreSQL logging function)
- Called from:
  - s_init_lock_sema (in src/backend/storage/lmgr/spin.c:146)
  - s_unlock_sema (in src/backend/storage/lmgr/spin.c:156)
  - tas_sema (in src/backend/storage/lmgr/spin.c:174)

## Notes and Other Information
- The function intentionally excludes 0 as a valid lock index to help detect uninitialized spinlocks
- Uses the unlikely() macro to optimize for the common case where the index is valid
- Part of the defensive programming approach in the spinlock emulation system
- The static inline declaration makes this a very low-overhead validation function
- Called by various spinlock operations to ensure they operate on valid lock indices