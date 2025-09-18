# AsyncShmemInit

## Location
src/backend/commands/async.c: 502 - 556

## Overview
Initializes the shared memory structures and SLRU (Simple Least Recently Used) management for PostgreSQL's asynchronous notification system.

## Definition
```c
void AsyncShmemInit(void)
```

## Detailed Description
The `AsyncShmemInit` function performs the crucial initialization of shared memory components for the LISTEN/NOTIFY asynchronous messaging system. It creates or attaches to the AsyncQueueControl structure, which manages the notification queue and backend status information. When initializing for the first time (indicated by `!found`), it sets up initial queue positions, clears backend tracking arrays, and initializes the SLRU buffer management system. The function also configures the pg_notify SLRU system with appropriate page precedence logic and cleans up any existing notification files during startup.

## Parameters / Member Variables
- No parameters - void function

## Dependencies
- Functions called/Symbols referenced:
  - mul_size, add_size (safe arithmetic for memory calculations)
  - ShmemInitStruct (shared memory structure initialization)
  - SET_QUEUE_POS (macro for setting queue positions)
  - asyncQueuePagePrecedes (page ordering function)
  - SimpleLruInit (SLRU system initialization)
  - SlruScanDirectory, SlruScanDirCbDeleteAll (SLRU directory management)
  - AsyncQueueControl, QueueBackendStatus (data structures)
  - Various queue management macros (QUEUE_HEAD, QUEUE_TAIL, etc.)
- Called from (representative examples):
  - CreateOrAttachShmemStructs (during shared memory setup)
  - Referenced in ASYNC_H header file

## Notes and Other Information
- Must be called during PostgreSQL startup to establish the notification system
- Uses the same size calculations as AsyncShmemSize to ensure consistency
- Initializes queue head and tail positions to (0,0) for a fresh start
- Sets up backend tracking arrays for MaxBackends processes
- Configures SLRU with long segment names to avoid wraparound issues
- Cleans out the pg_notify directory during fresh initialization to ensure a clean state
- Critical component of the shared memory initialization sequence
- The `found` parameter from ShmemInitStruct indicates whether this is the first process to initialize the structure