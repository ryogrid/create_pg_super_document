# ShutdownXLOG

## Location
src/backend/access/transam/xlog.c: 6581 - 6627

## Overview
ShutdownXLOG performs the final WAL operations during PostgreSQL server shutdown, coordinating WAL senders and creating final checkpoints or restart points to ensure data consistency.

## Definition
```c
void ShutdownXLOG(int code, Datum arg)
```

## Detailed Description
This function handles the orderly shutdown of the WAL (Write-Ahead Log) subsystem and must be called exactly once during postmaster or standalone backend shutdown. It follows a specific sequence: first signaling WAL senders to stop and waiting for them to reach stopping state, then creating either a restart point (during recovery) or a checkpoint (during normal operation). If archiving is active during normal operation, it requests a WAL switch to ensure all remaining records are archived before the final checkpoint.

## Parameters / Member Variables
- `code`: Exit code parameter (standard for shutdown callbacks)
- `arg`: Additional argument parameter (standard for shutdown callbacks, typically unused)

## Dependencies
- Functions called/Symbols referenced:
  - [WalSndInitStopping](../W/WalSndInitStopping.md)
  - [WalSndWaitStopping](../W/WalSndWaitStopping.md)
  - [RecoveryInProgress](../R/RecoveryInProgress.md)
  - [CreateRestartPoint](../C/CreateRestartPoint.md)
  - XLogArchivingActive
  - [RequestXLogSwitch](../R/RequestXLogSwitch.md)
  - [CreateCheckPoint](../C/CreateCheckPoint.md)
  - CHECKPOINT_IS_SHUTDOWN, CHECKPOINT_IMMEDIATE (flags)
- Called from (representative examples):
  - [HandleCheckpointerInterrupts](../H/HandleCheckpointerInterrupts.md) (in checkpointer.c:600)
  - [InitPostgres](../I/InitPostgres.md) (in postinit.c:810)

## Notes and Other Information
- Must be called exactly once during shutdown
- Uses AuxProcessResourceOwner for resource management
- Handles both recovery and normal operation modes differently
- Ensures WAL senders are stopped before proceeding with final operations
- In normal mode with archiving, rotates WAL file to archive remaining records
- Creates shutdown checkpoint with IMMEDIATE flag for fast completion