# WaitLatch

## Location
src/backend/storage/ipc/latch.c: 517 - 564

## Overview
Waits for a latch to be set, postmaster death, or timeout expiration, providing the core blocking mechanism for inter-process synchronization.

## Definition
```c
int WaitLatch(Latch *latch, int wakeEvents, long timeout, uint32 wait_event_info)
```

## Detailed Description
WaitLatch is the primary function for waiting on latch events in PostgreSQL. It blocks the calling process until one of the specified wake events occurs: the latch being set, postmaster death, or timeout expiration. The function uses a WaitEventSet internally to efficiently handle multiple event types simultaneously. It returns immediately if the latch is already set and WL_LATCH_SET is specified. The function requires that the latch is owned by the current process, either through InitLatch for process-local latches or OwnLatch for shared latches.

## Parameters / Member Variables
- `latch`: Pointer to the Latch structure to wait on (must be owned by current process)
- `wakeEvents`: Bitmask specifying which events to wait for (WL_LATCH_SET, WL_TIMEOUT, WL_POSTMASTER_DEATH, etc.)
- `timeout`: Timeout in milliseconds (must be >= 0 if WL_TIMEOUT is specified, max INT_MAX)
- `wait_event_info`: Event information for wait event reporting and monitoring

## Dependencies
- Functions called/Symbols referenced:
  - [Latch](../L/Latch.md) (structure type)
  - [WaitEvent](WaitEvent.md) (structure type)
  - WL_EXIT_ON_PM_DEATH, WL_POSTMASTER_DEATH, WL_LATCH_SET, WL_TIMEOUT (wait event flags)
  - [ModifyWaitEvent](../M/ModifyWaitEvent.md)
  - WaitEventSetWait
  - LatchWaitSetLatchPos
- Called from (representative examples):
  - [BackgroundWriterMain](../B/BackgroundWriterMain.md)
  - [CheckpointerMain](../C/CheckpointerMain.md)  
  - [WalWriterMain](WalWriterMain.md)
  - [ApplyLauncherMain](../A/ApplyLauncherMain.md)
  - [SyncRepWaitForLSN](../S/SyncRepWaitForLSN.md)
  - [shm_mq_wait_internal](../s/shm_mq_wait_internal.md)

## Notes and Other Information
- Processes under postmaster must handle postmaster death via WL_EXIT_ON_PM_DEATH or WL_POSTMASTER_DEATH
- Returns bitmask indicating which condition(s) caused wake-up, but may not return all conditions in one call
- Timeout parameter adds overhead, so should be avoided when not needed
- The latch can be NULL if WL_LATCH_SET is not specified in wakeEvents
- Used extensively throughout PostgreSQL for background processes and inter-process coordination
- Maximum supported timeout is INT_MAX milliseconds despite long parameter type