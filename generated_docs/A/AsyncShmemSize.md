# AsyncShmemSize

## Location
src/backend/commands/async.c: 485 - 501

## Overview
Calculates and returns the total shared memory size required for PostgreSQL's asynchronous notification system.

## Definition
```c
Size AsyncShmemSize(void)
```

## Detailed Description
The `AsyncShmemSize` function computes the amount of shared memory space needed for the asynchronous notification (LISTEN/NOTIFY) system. It performs careful size calculations using PostgreSQL's safe arithmetic functions to avoid overflow. The function calculates space for backend status tracking structures and the notification buffer management system. The implementation includes a comment noting that the calculations must match those in `AsyncShmemInit` to ensure consistency between memory allocation and initialization.

## Parameters / Member Variables
- Returns: `Size` - The total number of bytes required for async notification shared memory

## Dependencies
- Functions called/Symbols referenced:
  - mul_size (safe multiplication for memory calculations)
  - add_size (safe addition for memory calculations)  
  - SimpleLruShmemSize (calculates LRU buffer space)
  - QueueBackendStatus (structure for backend status tracking)
  - AsyncQueueControl (main control structure)
- Called from (representative examples):
  - CalculateShmemSize (during shared memory setup)
  - Referenced in ASYNC_H header file

## Notes and Other Information
- Uses PostgreSQL's safe arithmetic functions (mul_size, add_size) to prevent integer overflow during size calculations
- Calculates space for MaxBackends number of QueueBackendStatus structures
- Includes space for the AsyncQueueControl structure using offsetof for proper alignment
- Allocates space for notification buffers using SimpleLruShmemSize with notify_buffers parameter
- Critical that calculations match AsyncShmemInit implementation to prevent memory allocation mismatches
- Part of the shared memory subsystem initialization process during PostgreSQL startup