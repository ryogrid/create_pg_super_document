# GetRecoveryState

## Location
[src/backend/access/transam/xlog.c:6349-6367](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L6349-L6367)

## Overview
GetRecoveryState returns the current recovery state from shared memory, providing a consistent view of the database system's recovery status.

## Definition


## Detailed Description
GetRecoveryState is a thread-safe function that retrieves the current recovery state from PostgreSQL's shared memory control structure (XLogCtl). The function uses spinlock protection to ensure atomic access to the SharedRecoveryState field, guaranteeing consistency with the control file contents. This state information is crucial for determining whether the database is in recovery mode, what phase of recovery it's in, or if it has completed recovery and is in normal operation.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - SpinLockAcquire (on XLogCtl->info_lck)
  - SpinLockRelease (on XLogCtl->info_lck)
  - [RecoveryState](../R/RecoveryState.md) (return type)
  - XLogCtl (global shared memory structure)
- Called from (representative examples):
  - [XLogArchiveCheckDone](../X/XLogArchiveCheckDone.md)
  - [WALAvailability](../W/WALAvailability.md)

## Notes and Other Information
- The function is thread-safe due to spinlock protection around the shared memory access
- The returned state is kept consistent with the contents of the control file
- See xlog.h for details about possible RecoveryState values
- Located in src/backend/access/transam/xlog.c:6349-6367
- Critical for WAL and recovery management subsystems