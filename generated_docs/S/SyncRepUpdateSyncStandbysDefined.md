# SyncRepUpdateSyncStandbysDefined

## Location
src/backend/replication/syncrep.c: 964 - 1023

## Overview
Updates the shared memory flag indicating whether synchronous standbys are currently defined, and wakes up waiting backends when synchronous replication is disabled to prevent them from waiting indefinitely.

## Definition
```c
void SyncRepUpdateSyncStandbysDefined(void)
```

## Detailed Description
This function is called by the checkpointer process to maintain the shared sync_standbys_status flag in response to configuration changes. It performs several critical tasks:

1. **Status Synchronization**: Compares the current synchronous standby configuration with the shared memory status flag and updates it if they differ.

2. **Backend Wake-up**: When synchronous_standby_names is unset (synchronous replication disabled), it wakes up all backends currently waiting in the synchronous replication queues to prevent them from waiting indefinitely.

3. **Race Prevention**: Uses an interlock mechanism (SYNC_STANDBY_INIT flag) to prevent race conditions where backends might join the wait queue after standbys are disabled but before they reload their configuration.

4. **Initialization Tracking**: Ensures the sync_standbys_status flag is properly initialized even when no sync standbys are defined.

The function is designed to be safe for concurrent access, as only the checkpointer process updates the status flag.

## Parameters / Member Variables
- No input parameters (operates on global shared memory state)

## Dependencies
- Functions called/Symbols referenced:
  - SyncStandbysDefined (checks current sync standby configuration)
  - LWLockAcquire/LWLockRelease (exclusive lock management)
  - [SyncRepWakeQueue](SyncRepWakeQueue.md) (wakes waiting backends)
  - SYNC_STANDBY_DEFINED, SYNC_STANDBY_INIT (status flag constants)
  - NUM_SYNC_REP_WAIT_MODE (number of wait modes)
- Called from (representative examples):
  - [UpdateSharedMemoryConfig](../U/UpdateSharedMemoryConfig.md) (src/backend/postmaster/checkpointer.c:1320)

## Notes and Other Information
- Called exclusively by the checkpointer process
- Prevents backends from waiting indefinitely when sync replication is disabled
- Uses lock-free reading but exclusive locking for updates
- Critical for maintaining system responsiveness during configuration changes
- Handles both enabling and disabling of synchronous replication
- The race prevention mechanism ensures no backends get permanently stuck in wait queues