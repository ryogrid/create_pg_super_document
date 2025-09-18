# RequestCheckpoint

## Location
src/backend/postmaster/checkpointer.c: 947 - 996

## Overview
Initiates a checkpoint request from backend processes, either executing it immediately in standalone mode or signaling the checkpointer process in normal operation.

## Definition
```c
void RequestCheckpoint(int flags)
```

## Detailed Description
This function provides the main interface for backend processes to request database checkpoints. It handles two distinct execution paths:

1. **Standalone mode**: When not in a postmaster environment, it directly calls CreateCheckPoint() with CHECKPOINT_IMMEDIATE flag and performs cleanup.

2. **Normal operation**: In a multi-process environment, it:
   - Atomically sets checkpoint flags in shared memory using bitwise OR to preserve stronger requests
   - Sends SIGINT signal to the checkpointer process (with retry logic for up to 60 seconds)
   - Optionally waits for checkpoint completion using condition variables

The function implements a sophisticated waiting mechanism when CHECKPOINT_WAIT is specified, using condition variables to detect when a new checkpoint starts and completes. It tracks checkpoint completion through counters that increment in a modulo fashion.

## Parameters / Member Variables
- `flags`: Bitwise OR of checkpoint control flags:
  - `CHECKPOINT_IS_SHUTDOWN`: Checkpoint for database shutdown
  - `CHECKPOINT_END_OF_RECOVERY`: Checkpoint for end of WAL recovery  
  - `CHECKPOINT_IMMEDIATE`: Finish ASAP, ignore checkpoint_completion_target
  - `CHECKPOINT_FORCE`: Force checkpoint even without XLOG activity
  - `CHECKPOINT_WAIT`: Wait for completion before returning
  - `CHECKPOINT_CAUSE_XLOG`: Checkpoint due to XLOG filling (affects logging)

## Dependencies
- Functions called/Symbols referenced:
  - IsPostmasterEnvironment
  - [CreateCheckPoint](../C/CreateCheckPoint.md)
  - [smgrdestroyall](../s/smgrdestroyall.md)
  - SpinLockAcquire/SpinLockRelease
  - kill (system call)
  - [ConditionVariablePrepareToSleep](../C/ConditionVariablePrepareToSleep.md)
  - [ConditionVariableSleep](../C/ConditionVariableSleep.md)
  - [ConditionVariableCancelSleep](../C/ConditionVariableCancelSleep.md)
  - CHECK_FOR_INTERRUPTS
  - [pg_usleep](../p/pg_usleep.md)
  - elog/ereport
- Called from (representative examples):
  - [XLogWrite](../X/XLogWrite.md)
  - [StartupXLOG](../S/StartupXLOG.md)
  - [CreateDatabaseUsingFileCopy](../C/CreateDatabaseUsingFileCopy.md)
  - [dropdb](../d/dropdb.md)
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md)

## Notes and Other Information
- Uses MAX_SIGNAL_TRIES (600) with 0.1 second intervals for signaling retry
- In standalone mode, always adds CHECKPOINT_IMMEDIATE flag for efficiency
- Flag values must be designed to work with bitwise OR operations for multiple requests
- Error handling differs based on CHECKPOINT_WAIT flag (ERROR vs LOG)
- Implements modulo arithmetic for checkpoint completion detection
- The function can handle checkpointer process restarts during signaling
- Checkpoint failure is detected through the ckpt_failed counter comparison