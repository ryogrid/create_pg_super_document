# UpdateFullPageWrites

## Location
src/backend/access/transam/xlog.c: 8182 - 8250

## Overview
UpdateFullPageWrites synchronizes the full_page_writes configuration parameter between the GUC system and shared memory, writing a WAL record when necessary for standby consistency.

## Definition
```c
void UpdateFullPageWrites(void)
```

## Detailed Description
UpdateFullPageWrites ensures that changes to the full_page_writes configuration parameter are properly propagated to shared memory and logged to WAL when required. The full_page_writes parameter controls whether PostgreSQL writes complete page images to WAL after checkpoints, which is crucial for crash recovery consistency.

The function carefully manages the order of operations when toggling full_page_writes: when enabling, it sets the shared memory flag first, then writes the WAL record; when disabling, it writes the WAL record first, then updates the flag. This ordering ensures safety - it's always safe to take full page images when not strictly required, but dangerous to skip them when they are needed.

The function operates within a critical section and uses WAL insert locks to ensure atomicity. It only writes WAL records when standby information is active and recovery is not in progress, as these records are primarily needed for hot standby servers.

## Parameters / Member Variables
This function takes no parameters but works with:
- `fullPageWrites`: Current GUC value for full_page_writes
- `Insert->fullPageWrites`: Shared memory copy of the setting
- `XLogCtl->Insert`: Shared memory structure containing WAL insertion state

## Dependencies
- Functions called/Symbols referenced:
  - [RecoveryInProgress](../R/RecoveryInProgress.md)
  - START_CRIT_SECTION
  - END_CRIT_SECTION
  - [WALInsertLockAcquireExclusive](../W/WALInsertLockAcquireExclusive.md)
  - [WALInsertLockRelease](../W/WALInsertLockRelease.md)
  - XLogStandbyInfoActive
  - [XLogBeginInsert](../X/XLogBeginInsert.md)
  - [XLogRegisterData](../X/XLogRegisterData.md)
  - [XLogInsert](../X/XLogInsert.md)
  - [XLogCtlInsert](../X/XLogCtlInsert.md) (struct type)
  - XLOG_FPW_CHANGE (record type)
- Called from (representative examples):
  - [StartupXLOG](../S/StartupXLOG.md) (during startup processing)
  - [UpdateSharedMemoryConfig](UpdateSharedMemoryConfig.md) (when configuration changes)

## Notes and Other Information
- This function assumes no concurrent processes are updating full_page_writes, allowing safe lock-free reads
- The ordering of operations (flag first when enabling, WAL record first when disabling) is critical for correctness
- WAL records are only written when XLogStandbyInfoActive() returns true and recovery is not in progress
- The function uses critical sections to prevent interruption during the update process
- The XLOG_FPW_CHANGE record contains a boolean value indicating the new state of full_page_writes
- This mechanism allows standby servers to track full_page_writes changes during archive recovery
- The shared memory update is protected by WALInsertLock to ensure atomicity with concurrent WAL insertions