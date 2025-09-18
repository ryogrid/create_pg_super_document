# SnapBuildResetExportedSnapshotState

## Location
src/backend/replication/logical/snapbuild.c: 766 - 777

## Overview
Resets the global state variables related to exported snapshots during transaction abort, ensuring clean state recovery after transaction failures.

## Definition


## Detailed Description
This function provides essential cleanup functionality for the snapshot export mechanism during transaction abort scenarios. It resets the global state variables that track exported snapshot status to their initial values:

1. **Resource Owner Reset**: Sets SavedResourceOwnerDuringExport to NULL, clearing the reference to the saved resource owner from before the export began.

2. **Export Flag Reset**: Sets ExportInProgress to false, indicating that no snapshot export is currently active.

This function is specifically designed to be called during transaction abort processing to ensure that the snapshot export state is properly cleaned up even when transactions fail unexpectedly. Unlike SnapBuildClearExportedSnapshot, which performs active cleanup including transaction abort, this function only resets the state variables and is meant to be called as part of the abort process itself.

## Parameters / Member Variables
This function takes no parameters and operates on global state variables:
- : Reset to NULL
- : Reset to false

## Dependencies
- Functions called/Symbols referenced:
  - None (only modifies global variables)
- Called from (representative examples):
  - AbortTransaction

## Notes and Other Information
- Called automatically during transaction abort processing
- Does not perform transaction cleanup itself - only resets state variables
- Essential for preventing inconsistent state after transaction failures
- Simpler than SnapBuildClearExportedSnapshot as it assumes transaction abort is already in progress
- Ensures that subsequent operations don't think an export is still active after an abort
- Part of PostgreSQL's transaction abort cleanup chain