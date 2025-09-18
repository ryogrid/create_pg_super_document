# FinishPreparedTransaction

## Location
src/backend/access/transam/twophase.c: 1487 - 1679

## Overview
FinishPreparedTransaction executes the final phase of a two-phase commit, handling both COMMIT PREPARED and ROLLBACK PREPARED operations to complete prepared transactions.

## Definition
void FinishPreparedTransaction(const char *gid, bool isCommit)

## Detailed Description
This function performs the complete finalization of a prepared transaction identified by its Global Identifier (GID). It orchestrates the complex sequence of operations required to either commit or rollback a prepared transaction, including WAL logging, shared memory cleanup, file operations, cache invalidation, and callback execution. The function maintains strict ordering of operations to ensure data consistency: first logging the transaction outcome, then updating transaction status, removing the transaction from the process array, and finally executing post-commit or post-abort callbacks. It handles both on-disk and in-WAL stored transaction state data, manages relation file drops, executes statistics updates, and processes cache invalidation messages.

## Parameters / Member Variables
- `gid`: The Global Identifier string that uniquely identifies the prepared transaction to finish
- `isCommit`: Boolean flag indicating whether to commit (true) or rollback (false) the transaction

## Dependencies
- Functions called/Symbols referenced:
  - LockGXact
  - ReadTwoPhaseFile
  - XlogReadTwoPhaseData
  - TransactionIdLatest
  - RecordTransactionCommitPrepared
  - RecordTransactionAbortPrepared
  - ProcArrayRemove
  - ProcessRecords
  - RemoveTwoPhaseFile
  - DropRelationFiles
  - SendSharedInvalidMessages
- Called from (representative examples):
  - standard_ProcessUtility (for COMMIT/ROLLBACK PREPARED statements)
  - apply_handle_commit_prepared
  - apply_handle_rollback_prepared

## Notes and Other Information
- Uses critical sections with HOLD_INTERRUPTS/RESUME_INTERRUPTS to prevent interruption during cleanup
- Maintains strict operation ordering for consistency: WAL logging → transaction status update → process array removal → callbacks
- Handles both on-disk stored state (via ReadTwoPhaseFile) and WAL-stored state (via XlogReadTwoPhaseData)
- Processes relation cache invalidation messages only for commits, with pre/post invalidation phases
- Manages automatic cleanup of relation files that should be dropped as part of the transaction
- Acquires TwoPhaseStateLock during callback processing to prevent conflicts with other transactions