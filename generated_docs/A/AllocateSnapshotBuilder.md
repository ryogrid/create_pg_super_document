# AllocateSnapshotBuilder

## Location
src/backend/replication/logical/snapbuild.c: 324 - 371

## Overview
AllocateSnapshotBuilder allocates and initializes a new snapshot builder for logical replication, setting up the necessary context and initial state for building consistent snapshots during WAL decoding.

## Definition


## Detailed Description
This function creates a new SnapBuild structure which is the core component for building consistent snapshots during logical replication. It allocates the structure in its own memory context for better memory management and accountability. The function initializes the snapshot builder with starting parameters that define the point from which catalog consistency can be guaranteed and transaction replay should begin. The builder starts in the SNAPBUILD_START state and sets up initial arrays for tracking committed transactions and catalog changes.

## Parameters / Member Variables
- : ReorderBuffer instance that will be associated with this snapshot builder
- : Transaction ID >= which we can be sure no catalog rows have been removed
- : LSN >= from which we want to replay commits
- : Boolean indicating whether a full snapshot is required
- : Boolean indicating if this is being called during slot creation
- : LSN for two-phase commit handling

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate
  - MemoryContextSwitchTo
  - palloc0
  - ALLOCSET_DEFAULT_SIZES
  - SNAPBUILD_START
- Called from (representative examples):
  - StartupDecodingContext

## Notes and Other Information
The function creates a dedicated memory context named "snapshot builder context" to ensure proper memory management. It initializes the committed transactions array with an arbitrary starting size of 128 entries and marks it as initially including all transactions. The catchange array for catalog changes is initially set to NULL. The builder is initialized in the START state, which is the beginning of the snapshot building state machine used in logical replication.