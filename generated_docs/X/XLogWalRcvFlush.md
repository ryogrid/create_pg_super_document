# XLogWalRcvFlush

## Location
[src/backend/replication/walreceiver.c:993-1047](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/walreceiver.c#L993-L1047)

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
  - [issue_xlog_fsync](../i/issue_xlog_fsync.md)
  - [WakeupRecovery](../W/WakeupRecovery.md)
  - AllowCascadeReplication
  - [WalSndWakeup](../W/WalSndWakeup.md)
  - [set_ps_display](../s/set_ps_display.md)
  - [XLogWalRcvSendReply](XLogWalRcvSendReply.md)
  - [XLogWalRcvSendHSFeedback](XLogWalRcvSendHSFeedback.md)
- Called from (representative examples):
  - [WalReceiverMain](../W/WalReceiverMain.md)
  - [WalRcvDie](../W/WalRcvDie.md)
  - [XLogWalRcvClose](XLogWalRcvClose.md)

## Notes and Other Information
- This is a static function internal to the walreceiver.c module
- Only flushes if there's unsynced written data (Flush < Write)
- Uses spinlocks for atomic updates to shared memory state
- Critical for ensuring WAL durability in streaming replication
- Includes process title updates for monitoring and debugging
- Located in src/backend/replication/walreceiver.c:993-1047

## Simplified Source

```c
// Simplified version of XLogWalRcvFlush
static void XLogWalRcvFlush(bool dying, TimeLineID tli) {
    Assert(tli != 0);

    // Core logic step 1: Check if there's data to flush
    if (LogstreamResult.Flush < LogstreamResult.Write) {
        WalRcvData *walrcv = WalRcv;

        // Core logic step 2: Force WAL data to disk
        issue_xlog_fsync(recvFile, recvSegNo, tli);
        LogstreamResult.Flush = LogstreamResult.Write;

        // Core logic step 3: Update shared memory with flush position
        SpinLockAcquire(&walrcv->mutex);
        if (walrcv->flushedUpto < LogstreamResult.Flush) {
            walrcv->latestChunkStart = walrcv->flushedUpto;
            walrcv->flushedUpto = LogstreamResult.Flush;
            walrcv->receivedTLI = tli;
        }
        SpinLockRelease(&walrcv->mutex);

        // Core logic step 4: Notify other processes of new WAL data
        WakeupRecovery();
        if (AllowCascadeReplication()) {
            WalSndWakeup(true, false);
        }

        // Core logic step 5: Update process status display
        if (update_process_title) {
            char activitymsg[50];
            snprintf(activitymsg, sizeof(activitymsg), "streaming %X/%X",
                     LSN_FORMAT_ARGS(LogstreamResult.Write));
            set_ps_display(activitymsg);
        }

        // Core logic step 6: Send progress updates to primary (if not shutting down)
        if (!dying) {
            XLogWalRcvSendReply(false, false);
            XLogWalRcvSendHSFeedback(false);
        }
    }
}
```

Key simplifications made:
- Preserved the essential flush-only-when-needed check
- Maintained the core fsync operation for durability
- Kept shared memory update logic with proper locking
- Retained process notification and progress reporting
- Focused on the main execution path without removing critical functionality
- Added descriptive comments for each major step