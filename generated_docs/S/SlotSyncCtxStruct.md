# SlotSyncCtxStruct

## Location
src/backend/replication/logical/slotsync.c: 97 - 104

## Overview
SlotSyncCtxStruct is a shared memory structure that controls slot synchronization between the slot sync worker process and other PostgreSQL processes, particularly during standby server promotion scenarios.

## Definition


## Detailed Description
SlotSyncCtxStruct serves as a coordination mechanism for slot synchronization operations in PostgreSQL logical replication. This structure is stored in shared memory and provides process coordination, state management, and race condition prevention during slot sync operations. It is particularly critical during standby server promotion when the slot sync worker needs to be cleanly shut down.

The structure implements a locking mechanism to prevent concurrent slot synchronization operations that could lead to slot overwrites. It also tracks timing information to ensure the postmaster starts the slot sync worker at appropriate intervals defined by SLOTSYNC_RESTART_INTERVAL_SEC.

## Parameters / Member Variables
- : Process ID of the currently running slot sync worker, used by the startup process to shut down the worker during promotion
- : Boolean flag set during promotion to prevent the slot sync worker from restarting and to make pg_sync_replication_slots() error out
- : Boolean flag that prevents concurrent slot synchronization operations to avoid slot overwrites
- : Timestamp used by the postmaster to control slot sync worker restart intervals, can be reset by the worker to trigger immediate restart
- : Spinlock (slock_t) that provides thread-safe access to the structure members

## Dependencies
- Functions called/Symbols referenced:
  - pid_t (system type for process IDs)
  - slock_t (PostgreSQL spinlock type)
- Called from (representative examples):
  - SlotSyncShmemSize (calculates shared memory size requirements)
  - SlotSyncShmemInit (initializes the shared memory structure)

## Notes and Other Information
- This structure is designed to handle race conditions during standby promotion when the postmaster may not immediately notice the promotion
- The stopSignaled flag remains set after promotion since PostgreSQL doesn't support demoting a primary without server restart
- The structure supports the MaybeStartSlotSyncWorker logic that determines when slot sync workers should be started based on pmState changes
- Access to this structure should always be protected by the mutex member to ensure thread safety
- The last_start_time can be reset by workers to bypass the normal restart interval when immediate restart is needed (e.g., when slot sync GUCs change)