# AllocateSnapshotBuilder

## Location
[src/backend/replication/logical/snapbuild.c:324-371](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/snapbuild.c#L324-L371)

## Overview
AllocateSnapshotBuilder allocates and initializes a new snapshot builder for logical replication, setting up the necessary context and initial state for building consistent snapshots during WAL decoding.

## Definition

```c
struct members initialized by zeroing via palloc0 above */

	builder->committed.xcnt = 0;
```
## Detailed Description
This function creates a new SnapBuild structure which is the core component for building consistent snapshots during logical replication. It allocates the structure in its own memory context for better memory management and accountability. The function initializes the snapshot builder with starting parameters that define the point from which catalog consistency can be guaranteed and transaction replay should begin. The builder starts in the SNAPBUILD_START state and sets up initial arrays for tracking committed transactions and catalog changes.

## Parameters / Member Variables
- : ReorderBuffer instance that will be associated with this snapshot builder

## Simplified Source

```c
// Simplified version of AllocateSnapshotBuilder
SnapBuild *AllocateSnapshotBuilder(ReorderBuffer *reorder,
                                  TransactionId xmin_horizon,
                                  XLogRecPtr start_lsn,
                                  bool need_full_snapshot,
                                  bool in_slot_creation,
                                  XLogRecPtr two_phase_at) {
    // Create dedicated memory context for the snapshot builder
    MemoryContext context = AllocSetContextCreate(CurrentMemoryContext,
                                                  "snapshot builder context",
                                                  ALLOCSET_DEFAULT_SIZES);
    MemoryContext oldcontext = MemoryContextSwitchTo(context);

    // Allocate and initialize the snapshot builder
    SnapBuild *builder = palloc0(sizeof(SnapBuild));
    builder->state = SNAPBUILD_START;
    builder->context = context;
    builder->reorder = reorder;

    // Initialize committed transaction tracking
    builder->committed.xcnt = 0;
    builder->committed.xcnt_space = 128;
    builder->committed.xip = palloc0(builder->committed.xcnt_space * sizeof(TransactionId));
    builder->committed.includes_all_transactions = true;

    // Initialize catalog change tracking
    builder->catchange.xcnt = 0;
    builder->catchange.xip = NULL;

    // Set configuration parameters
    builder->initial_xmin_horizon = xmin_horizon;
    builder->start_decoding_at = start_lsn;
    builder->in_slot_creation = in_slot_creation;
    builder->building_full_snapshot = need_full_snapshot;
    builder->two_phase_at = two_phase_at;

    MemoryContextSwitchTo(oldcontext);
    return builder;
}
```

Key simplifications made:
- Grouped related initialization code together
- Added clear comments for each major section
- Simplified memory context management
- Combined related field assignments
- Focused on core snapshot builder allocation functionality
- : Transaction ID >= which we can be sure no catalog rows have been removed
- : LSN >= from which we want to replay commits
- : Boolean indicating whether a full snapshot is required
- : Boolean indicating if this is being called during slot creation
- : LSN for two-phase commit handling

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [palloc0](../p/palloc0.md)
  - ALLOCSET_DEFAULT_SIZES
  - SNAPBUILD_START
- Called from (representative examples):
  - [StartupDecodingContext](../S/StartupDecodingContext.md)

## Notes and Other Information
The function creates a dedicated memory context named "snapshot builder context" to ensure proper memory management. It initializes the committed transactions array with an arbitrary starting size of 128 entries and marks it as initially including all transactions. The catchange array for catalog changes is initially set to NULL. The builder is initialized in the START state, which is the beginning of the snapshot building state machine used in logical replication.