# WalSndWakeup

## Location
src/backend/replication/walsender.c: 3708 - 3728

## Overview
WalSndWakeup wakes up WAL sender processes waiting for new WAL data, with separate control for physical and logical replication types.

## Definition
```c
void WalSndWakeup(bool physical, bool logical)
```

## Detailed Description
This function wakes up WAL sender processes that are waiting for WAL data to become available. It distinguishes between two types of replication:

1. **Physical Replication**: WAL senders that stream raw WAL records. These can only send data after it has been flushed to disk.
2. **Logical Replication**: WAL senders that decode and stream logical changes. On standby servers, these can only send data after WAL has been applied (replayed).

The function uses condition variable broadcasting to wake up waiting processes efficiently. It's designed to be called from critical sections, so it avoids operations that could throw errors.

## Parameters / Member Variables
- `physical`: If true, wake up physical WAL senders waiting for WAL flush
- `logical`: If true, wake up logical WAL senders waiting for WAL replay

## Dependencies
- Functions called/Symbols referenced:
  - ConditionVariableBroadcast (broadcasts signal to all waiters on condition variables)
- Called from (representative examples):
  - [StartupXLOG](../S/StartupXLOG.md) (during WAL replay in recovery)
  - [KeepFileRestoredFromArchive](../K/KeepFileRestoredFromArchive.md) (after restoring archived WAL)
  - [ApplyWalRecord](../A/ApplyWalRecord.md) (during WAL record application)
  - [XLogWalRcvFlush](../X/XLogWalRcvFlush.md) (after flushing received WAL data)
  - [WalSndWakeupProcessRequests](WalSndWakeupProcessRequests.md) (general wakeup processing)

## Notes and Other Information
- This function is critical for the performance of streaming replication
- Can be called with either or both parameters set to true
- Designed to be safe to call from within critical sections
- The distinction between physical and logical is important for cascading replication scenarios
- WAL senders must have previously called WalSndWait() to be added to the condition variable wait lists
- Uses the shared memory condition variables wal_flush_cv and wal_replay_cv initialized by WalSndShmemInit
- Enables efficient coordination between WAL generation/application and replication streaming