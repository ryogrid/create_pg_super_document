# restoreTwoPhaseData

## Location
[src/backend/access/transam/twophase.c:1889-1952](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/twophase.c#L1889-L1952)

## Overview
restoreTwoPhaseData scans the pg_twophase directory and populates the TwoPhaseState data structure with prepared transaction information found in on-disk state files during PostgreSQL startup recovery.

## Definition
```c
void restoreTwoPhaseData(void)
```

## Detailed Description
restoreTwoPhaseData is called once at the beginning of recovery to initialize the in-memory two-phase commit state from persistent storage. The function scans the pg_twophase directory for valid two-phase state files and reconstructs the TwoPhaseState structure to reflect prepared transactions that were persisted to disk.

The function operates by:
1. Acquiring exclusive lock on TwoPhaseStateLock to ensure atomic initialization
2. Opening and reading the TWOPHASE_DIR directory
3. Examining each directory entry for valid two-phase file names (16 hexadecimal characters)
4. Converting filenames to transaction IDs and processing the corresponding state files
5. Adding valid prepared transactions back to the in-memory state via PrepareRedoAdd

Files that represent transactions newer than the minimum XID horizon are automatically discarded during this process, ensuring only relevant prepared transactions are restored.

## Parameters / Member Variables
This function takes no parameters and operates on global TwoPhaseState.

## Dependencies
- Functions called/Symbols referenced:
  - [AllocateDir](../A/AllocateDir.md)
  - [ReadDir](../R/ReadDir.md)
  - [FreeDir](../F/FreeDir.md)
  - [FullTransactionIdFromU64](../F/FullTransactionIdFromU64.md)
  - XidFromFullTransactionId
  - [ProcessTwoPhaseBuffer](../P/ProcessTwoPhaseBuffer.md)
  - [PrepareRedoAdd](../P/PrepareRedoAdd.md)
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease
  - strtou64
- Called from:
  - [StartupXLOG](../S/StartupXLOG.md)

## Notes and Other Information
- Called only once during startup recovery to avoid repeated directory scans
- Validates file names by checking for exactly 16 hexadecimal characters
- Holds TwoPhaseStateLock exclusively during the entire restoration process
- Automatically filters out transactions that are no longer relevant based on XID horizon
- Uses ProcessTwoPhaseBuffer with specific flags (true, false, false) for recovery context
- Integrates with the WAL recovery system through PrepareRedoAdd calls