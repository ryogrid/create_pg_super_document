# XLogWalRcvFlush

## Location
src/backend/replication/walreceiver.c: 993 - 1047

## Overview
Forces WAL data written to disk to be synchronized (flushed) to persistent storage and notifies other processes of the flushed data availability during streaming replication.

## Definition
```c
static void XLogWalRcvFlush(bool dying, TimeLineID tli)
```

## Detailed Description
This function ensures that WAL data previously written to disk is actually synchronized to persistent storage through fsync operations. It's a critical component for durability guarantees in streaming replication. The function performs several important tasks:

1. Issues an fsync operation to ensure data persistence
2. Updates shared memory structures to reflect the new flush position
3. Wakes up the startup (recovery) process to process newly available WAL
4. Notifies walsender processes if cascade replication is enabled
5. Updates the process title to show streaming progress
6. Sends progress replies to the primary server (unless the process is shutting down)

The function includes safeguards for shutdown scenarios where sending replies might be unsafe.

## Parameters / Member Variables
- `dying`: Boolean flag indicating whether the process is shutting down (prevents sending replies if true)
- `tli`: Timeline ID associated with the WAL data being flushed

## Dependencies
- Functions called/Symbols referenced:
  - issue_xlog_fsync
  - WakeupRecovery
  - AllowCascadeReplication
  - WalSndWakeup
  - set_ps_display
  - XLogWalRcvSendReply
  - XLogWalRcvSendHSFeedback
- Called from (representative examples):
  - WalReceiverMain
  - WalRcvDie
  - XLogWalRcvClose

## Notes and Other Information
- This is a static function internal to the walreceiver.c module
- Only flushes if there's unsynced written data (Flush < Write)
- Uses spinlocks for atomic updates to shared memory state
- Critical for ensuring WAL durability in streaming replication
- Includes process title updates for monitoring and debugging
- Located in src/backend/replication/walreceiver.c:993-1047