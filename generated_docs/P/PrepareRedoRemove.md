# PrepareRedoRemove

## Location
src/backend/access/transam/twophase.c: 2572 - 2623

## Overview
PrepareRedoRemove removes a global transaction entry from shared memory during WAL recovery and cleans up any associated two-phase commit disk files.

## Definition


## Detailed Description
PrepareRedoRemove is the cleanup counterpart to PrepareRedoAdd, responsible for removing global transaction entries from the TwoPhaseState shared memory structure during WAL replay. The function searches through the active prepared transactions array to find the entry matching the specified transaction ID, then removes both the in-memory state and any corresponding disk files if they exist. This function is typically called during recovery when processing COMMIT PREPARED or ROLLBACK PREPARED WAL records, or when cleaning up stale transaction state that was already committed or aborted. It handles both scenarios where the transaction state exists only in memory and where it was previously checkpointed to disk.

## Parameters / Member Variables
- : The transaction ID of the prepared transaction to remove from the recovery state
- : Boolean flag indicating whether to issue warnings when removing disk files (used for error reporting control)

## Dependencies
- Functions called/Symbols referenced:
  - LWLockHeldByMeInMode
  - RecoveryInProgress
  - RemoveTwoPhaseFile
  - RemoveGXact
- Called from (representative examples):
  - ProcessTwoPhaseBuffer
  - xact_redo

## Notes and Other Information
The function requires exclusive access to TwoPhaseStateLock and can only be called during recovery (RecoveryInProgress must be true). It gracefully handles cases where the transaction entry doesn't exist, which is expected during normal WAL replay scenarios. When a matching entry is found, it verifies the inredo flag is set (confirming it was added during recovery) before proceeding with cleanup. The function performs both memory cleanup via RemoveGXact and optional disk cleanup via RemoveTwoPhaseFile depending on the ondisk flag. Location: src/backend/access/transam/twophase.c:2572-2623