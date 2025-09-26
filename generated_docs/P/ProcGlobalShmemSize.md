# ProcGlobalShmemSize

## Location
src/backend/storage/lmgr/proc.c: 100 - 121

## Overview
Reports the amount of shared memory space needed by InitProcGlobal for process management data structures.

## Definition
```c
Size ProcGlobalShmemSize(void)
```

## Detailed Description
ProcGlobalShmemSize calculates the total shared memory requirements for PostgreSQL's process management subsystem. The function computes memory needed for:

1. **Process Header (PROC_HDR)**: Core process management metadata
2. **Process Array**: Array of PGPROC structures for all processes (backends + auxiliary + prepared transactions)  
3. **Process Lock**: Spinlock for protecting process structures
4. **Transaction Arrays**: Arrays for transaction IDs, subtransaction states, and status flags

The calculation uses safe arithmetic functions (add_size, mul_size) to prevent integer overflow when computing large memory requirements.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - add_size (safe addition to prevent overflow)
  - mul_size (safe multiplication to prevent overflow)
  - MaxBackends (GUC parameter for max backend processes)
  - NUM_AUXILIARY_PROCS (constant for auxiliary processes)
  - max_prepared_xacts (GUC parameter for prepared transactions)
- Referenced types:
  - PROC_HDR (process management header structure)
  - PGPROC (individual process structure)
  - slock_t (spinlock type)
- Called from:
  - CalculateShmemSize (in shared memory initialization)

## Notes and Other Information
- Critical for shared memory sizing during PostgreSQL startup
- Must account for all process types: regular backends, auxiliary processes, and prepared transactions
- Uses overflow-safe arithmetic to handle large installations with many connections
- The calculated size is used by the shared memory allocator to reserve adequate space