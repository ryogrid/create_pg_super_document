# PrepareRedoAdd

## Location
src/backend/access/transam/twophase.c: 2470 - 2571

## Overview
PrepareRedoAdd creates and registers a global transaction entry in shared memory during WAL recovery, tracking prepared transaction state from either WAL records or existing disk files.

## Definition


## Detailed Description
PrepareRedoAdd is a critical function in PostgreSQL's two-phase commit recovery process that creates global transaction entries in the TwoPhaseState shared memory structure during WAL replay. The function handles both scenarios where two-phase data is available in WAL records (start_lsn is valid) and where data has already been restored from disk files (start_lsn is invalid). It includes sophisticated duplicate detection logic to handle crash scenarios where some two-phase data may exist both in WAL records and on disk. The function also manages replication origin advancement for logical replication scenarios and maintains proper state tracking with the inredo flag.

## Parameters / Member Variables
- : Buffer containing the two-phase transaction data including header and global transaction identifier
- : WAL log sequence number where the prepare record starts (InvalidXLogRecPtr if reading from disk)
- : WAL log sequence number where the prepare record ends
- : Replication origin identifier for logical replication tracking (InvalidRepOriginId if not applicable)

## Dependencies
- Functions called/Symbols referenced:
  - LWLockHeldByMeInMode
  - RecoveryInProgress
  - XLogRecPtrIsInvalid
  - TwoPhaseFilePath
  - access
  - replorigin_advance
- Called from (representative examples):
  - restoreTwoPhaseData
  - xact_redo

## Notes and Other Information
The function requires exclusive access to TwoPhaseStateLock and can only be called during recovery. It performs careful duplicate detection by checking for existing files when processing WAL records, preventing corruption during crash recovery scenarios. The function allocates global transaction structures from the free list and properly initializes all necessary fields including prepare timestamps, LSN positions, and state flags. Error handling distinguishes between consistency-reached and pre-consistency states. Location: src/backend/access/transam/twophase.c:2470-2571