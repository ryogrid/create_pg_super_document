# CheckpointerMain

## Location
src/backend/postmaster/checkpointer.c: 176 - 560

## Overview
Main entry point for the checkpointer process that performs database checkpoints and manages WAL archiving in PostgreSQL.

## Definition


## Detailed Description
CheckpointerMain is the core function of the checkpointer background process, responsible for:
- Performing periodic checkpoints to ensure database consistency
- Managing WAL (Write-Ahead Log) archiving timeouts  
- Handling checkpoint requests from other processes
- Managing memory contexts and error recovery for the checkpointer process
- Coordinating with other backend processes through shared memory

The function runs in an infinite loop, sleeping between checkpoint operations and responding to signals for checkpoint requests. It handles both time-driven checkpoints (based on CheckPointTimeout) and request-driven checkpoints from other processes. During recovery, it performs restartpoints instead of full checkpoints.

The process sets up signal handlers for configuration reloads, checkpoint requests, and shutdown signals. It uses a dedicated memory context for error recovery and implements comprehensive cleanup procedures when errors occur.

## Parameters / Member Variables
- : Startup data passed from the postmaster (expected to be NULL/empty)
- : Length of startup data (expected to be 0)

## Dependencies
- Functions called/Symbols referenced:
  - AuxiliaryProcessMainCommon
  - HandleCheckpointerInterrupts  
  - CheckArchiveTimeout
  - CreateCheckPoint
  - CreateRestartPoint
  - AbsorbSyncRequests
  - UpdateSharedMemoryConfig
  - ResetLatch/WaitLatch
  - pgstat_report_checkpointer
  - RecoveryInProgress
  - GetInsertRecPtr/GetXLogReplayRecPtr
- Called from (representative examples):
  - child_process_kind (launch_backend.c:204)

## Notes and Other Information
- Ignores SIGTERM to avoid premature shutdown during system-wide shutdowns
- Uses sigsetjmp/longjmp for error recovery instead of PG_TRY/PG_CATCH
- Maintains separate statistics for checkpoints vs restartpoints
- Implements checkpoint frequency warnings when checkpoints occur too frequently
- Coordinates with waiting backends through condition variables in shared memory
- Performs cleanup of storage manager objects after each checkpoint to handle dropped relations