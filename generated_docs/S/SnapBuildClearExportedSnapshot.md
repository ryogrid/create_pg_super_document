# SnapBuildClearExportedSnapshot

## Location
src/backend/replication/logical/snapbuild.c: 739 - 765

## Overview
Clears and resets a previously exported snapshot created by SnapBuildExportSnapshot, aborting the associated transaction and restoring the original resource owner state.

## Definition


## Detailed Description
This function provides cleanup functionality for exported snapshots, ensuring proper resource management and transaction state reset. It is the counterpart to SnapBuildExportSnapshot and handles the cleanup process when an exported snapshot is no longer needed:

1. **Export State Check**: Verifies that a snapshot export is currently in progress using the ExportInProgress flag. If no export is active, the function returns early.

2. **Transaction State Validation**: Ensures the current backend is in a proper transaction state before attempting to abort the transaction.

3. **Resource Owner Management**: Preserves the original resource owner (SavedResourceOwnerDuringExport) before transaction abort, since AbortCurrentTransaction() will reset various transaction-related state.

4. **Transaction Cleanup**: Calls AbortCurrentTransaction() to clean up the transaction that was started during snapshot export, which automatically handles snapshot state reset.

5. **State Restoration**: Restores the CurrentResourceOwner to its original value, completing the cleanup process.

This function is essential for maintaining proper transaction boundaries and preventing resource leaks when exported snapshots are no longer needed.

## Parameters / Member Variables
This function takes no parameters and operates on global state variables related to snapshot export.

## Dependencies
- Functions called/Symbols referenced:
  - IsTransactionState
  - AbortCurrentTransaction
  - ResourceOwner (type)
- Called from (representative examples):
  - exec_replication_command

## Notes and Other Information
- Only performs cleanup if ExportInProgress flag indicates an active export
- Must be called when the backend is in a valid transaction state
- AbortCurrentTransaction() automatically handles snapshot state cleanup
- Restores the original resource owner to maintain proper resource management
- This function ensures that exported snapshot transactions don't remain open indefinitely
- Typically called when replication commands complete or encounter errors