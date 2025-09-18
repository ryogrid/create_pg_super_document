# WalRcvShmemInit

## Location
src/backend/replication/walreceiverfuncs.c: 54 - 74

## Overview
Allocates and initializes the shared memory structures required for WAL receiver operations.

## Definition


## Detailed Description
This function is responsible for setting up the shared memory area used by the WAL receiver subsystem. It allocates a shared memory segment of the size determined by WalRcvShmemSize() and initializes the WalRcvData structure if this is the first process to access it. The initialization includes setting up synchronization primitives like condition variables and spin locks, as well as initializing atomic variables and setting the initial WAL receiver state to WALRCV_STOPPED.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [WalRcvData](WalRcvData.md)
  - [ShmemInitStruct](../S/ShmemInitStruct.md)
  - [WalRcvShmemSize](WalRcvShmemSize.md)
  - MemSet
  - WALRCV_STOPPED
  - [ConditionVariableInit](../C/ConditionVariableInit.md)
  - SpinLockInit
  - [pg_atomic_init_u64](../p/pg_atomic_init_u64.md)
- Called from (representative examples):
  - [CreateOrAttachShmemStructs](../C/CreateOrAttachShmemStructs.md)

## Notes and Other Information
- Located in src/backend/replication/walreceiverfuncs.c:54-74
- Uses the standard PostgreSQL shared memory initialization pattern with ShmemInitStruct
- Only initializes the structure on first access (when found is false)
- Sets up critical synchronization primitives needed for multi-process coordination
- The global WalRcv pointer is assigned during this initialization
- Essential for proper operation of streaming replication functionality