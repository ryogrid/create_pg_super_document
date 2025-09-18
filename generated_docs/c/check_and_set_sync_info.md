# check_and_set_sync_info

## Location
src/backend/replication/logical/slotsync.c: 1271 - 1314

## Overview
check_and_set_sync_info validates synchronization preconditions and atomically sets the sync-in-progress state to prevent concurrent operations and handle promotion scenarios.

## Definition
```c
static void check_and_set_sync_info(pid_t worker_pid)
```

## Detailed Description
This function serves as a critical synchronization point that ensures safe coordination between slot synchronization operations and database promotion processes. It performs several essential checks and state updates under spinlock protection:

1. **Promotion Check**: Verifies that no standby promotion is currently in progress by checking the stopSignaled flag
2. **Concurrency Check**: Ensures no other slot synchronization operation is already running
3. **State Setting**: Atomically sets the syncing flag to true and records the worker PID

The function prevents race conditions between multiple sync attempts and ensures the startup process can properly coordinate with slot sync workers during promotion scenarios. If any precondition fails, it raises appropriate errors with specific error codes.

## Parameters / Member Variables
- `worker_pid`: Process ID of the slot synchronization worker, or InvalidPid for non-worker contexts

## Dependencies
- Functions called/Symbols referenced:
  - SpinLockAcquire
  - SpinLockRelease
  - Assert (macro)
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
  - InvalidPid (constant)
  - ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE
- Global variables accessed:
  - SlotSyncCtx (shared memory structure)
  - syncing_slots (module-level flag)
- Called from (representative examples):
  - [ReplSlotSyncWorkerMain](../R/ReplSlotSyncWorkerMain.md) (in src/backend/replication/logical/slotsync.c:1401)
  - SyncReplicationSlots (in src/backend/replication/logical/slotsync.c:1729)

## Notes and Other Information
- This is a static function, meaning it's only visible within the slotsync.c compilation unit
- Uses spinlock protection to ensure atomic access to shared memory state
- Critical for preventing race conditions in multi-process slot synchronization
- The worker PID is recorded to enable clean termination during promotion
- Raises specific PostgreSQL error codes for different failure scenarios
- Essential component of the slot synchronization coordination mechanism
- Must be called before beginning any slot synchronization operations