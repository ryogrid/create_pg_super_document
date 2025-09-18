# WalRcvDie

## Location
src/backend/replication/walreceiver.c: 801 - 838

## Overview
WalRcvDie is an exit handler function that performs cleanup operations when the WAL receiver process terminates.

## Definition
static void WalRcvDie(int code, Datum arg)

## Detailed Description
This function serves as a cleanup handler registered with on_shmem_exit() to ensure proper shutdown procedures when the WAL receiver process exits, either normally or due to an error. It performs essential cleanup tasks to maintain data consistency and notify other processes of the termination.

The function executes several critical cleanup steps:
1. **Data persistence**: Forces all received WAL records to disk using XLogWalRcvFlush() to prevent data loss
2. **State management**: Updates shared memory state to WALRCV_STOPPED and clears process identification
3. **Resource cleanup**: Resets display readiness flag and latch pointer to prevent dangling references
4. **Process notification**: Broadcasts to condition variable waiters that WAL receiver has stopped
5. **Connection cleanup**: Gracefully disconnects from the primary server if a connection exists
6. **Recovery notification**: Wakes up the startup process to handle the WAL receiver termination promptly

The function includes assertions to verify that the WAL receiver is in a valid state before termination and that the correct process is performing the cleanup. The timeline ID passed as an argument is used for the final WAL flush operation.

## Parameters / Member Variables
- : Exit code indicating the reason for termination (standard exit handler parameter)
- : Datum containing a pointer to the TimeLineID for the final WAL flush operation

## Dependencies
- Functions called/Symbols referenced:
  - [XLogWalRcvFlush](../X/XLogWalRcvFlush.md)
  - SpinLockAcquire, SpinLockRelease
  - ConditionVariableBroadcast
  - walrcv_disconnect
  - [WakeupRecovery](WakeupRecovery.md)
  - [DatumGetPointer](../D/DatumGetPointer.md)
  - WALRCV_STREAMING, WALRCV_RESTARTING, WALRCV_STARTING, WALRCV_WAITING, WALRCV_STOPPING, WALRCV_STOPPED
  - [WalRcvData](WalRcvData.md) structure and its fields

- Called from (representative examples):
  - [WalReceiverMain](WalReceiverMain.md) (via on_shmem_exit registration)
  - WalRcvWakeupReason

## Notes and Other Information
- Registered as an exit handler to ensure cleanup occurs regardless of how the process terminates
- Critical for maintaining data consistency during WAL receiver shutdown
- Ensures proper notification of other processes about WAL receiver termination
- Part of PostgreSQL's streaming replication infrastructure cleanup mechanisms
- The forced WAL flush prevents potential data loss during abnormal terminations
- Condition variable broadcast allows waiting processes to detect termination promptly