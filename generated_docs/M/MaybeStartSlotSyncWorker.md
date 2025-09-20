# MaybeStartSlotSyncWorker

## Location
[src/backend/postmaster/postmaster.c:4091-4102](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/postmaster.c#L4091-L4102)

## Overview
MaybeStartSlotSyncWorker conditionally starts a slot synchronization worker process specifically for hot standby servers to synchronize replication slots with the primary server.

## Definition

```c
static void
MaybeStartSlotSyncWorker(void)
```
## Detailed Description
MaybeStartSlotSyncWorker implements conditional startup logic for slot sync worker processes, which are responsible for synchronizing replication slots between primary and standby servers. This function is highly specialized - it only operates on hot standby servers (pmState == PM_HOT_STANDBY) and includes several validation checks to ensure proper configuration.

The worker starts only when multiple conditions are satisfied: no worker is currently running (SlotSyncWorkerPID == 0), the server is in hot standby mode, no fast/immediate shutdown is in progress, the sync_replication_slots parameter is enabled, slot sync parameters are configured correctly (validated by ValidateSlotSyncParams), and enough time has passed since the last worker launch (checked by SlotSyncWorkerCanRestart).

The restart timing mechanism prevents rapid restart loops and allows for controlled periodic synchronization attempts, making the slot sync process resilient to temporary network issues or primary server unavailability.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [ValidateSlotSyncParams](../V/ValidateSlotSyncParams.md) (validates slot sync configuration parameters)
  - [SlotSyncWorkerCanRestart](../S/SlotSyncWorkerCanRestart.md) (checks if enough time has passed since last launch)
  - [StartChildProcess](../S/StartChildProcess.md) (creates the slot sync worker process with B_SLOTSYNC_WORKER type)
- [Variables](../V/Variables.md) referenced:
  - SlotSyncWorkerPID (tracks current worker process ID)
  - pmState (postmaster state - must be PM_HOT_STANDBY)
  - Shutdown (shutdown state - compared with SmartShutdown)
  - sync_replication_slots (configuration parameter enabling slot synchronization)
- Called from (representative examples):
  - [ServerLoop](../S/ServerLoop.md) (main postmaster loop for regular checks)
  - [process_pm_child_exit](../p/process_pm_child_exit.md) (restart after child process termination)

## Notes and Other Information
- Only functions on hot standby servers - primary servers don't need slot sync workers
- Includes comprehensive configuration validation before attempting to start workers
- Implements restart throttling to prevent rapid restart loops and resource exhaustion
- Critical for maintaining replication slot consistency in streaming replication setups
- Controlled by sync_replication_slots GUC parameter for enabling/disabling functionality
- Worker processes handle the complex protocol for synchronizing slot states with primary servers
- Essential for high availability setups where standby promotion requires accurate replication slot state