# SnapshotSetCommandId

## Location
src/backend/utils/time/snapmgr.c: 456 - 476

## Overview
Propagates command counter increments to active static snapshots to maintain proper intra-transaction visibility semantics.

## Definition
```c
void SnapshotSetCommandId(CommandId curcid)
```

## Detailed Description
SnapshotSetCommandId is responsible for updating the current command ID (curcid) in static snapshots when the command counter is incremented within a transaction. The function checks if snapshots have been initialized (FirstSnapshotSet) and then updates the curcid field in both CurrentSnapshot and SecondarySnapshot if they exist. This ensures that these snapshots reflect the correct command-level visibility semantics, allowing them to see the effects of commands executed earlier in the same transaction while remaining invisible to concurrent transactions.

The function contains a comment questioning whether CatalogSnapshot should also be updated, indicating this is an area of potential future enhancement.

## Parameters / Member Variables
- `curcid`: The new command ID to propagate to static snapshots

## Dependencies
- Functions called/Symbols referenced:
  - CommandId (type)
- Called from (representative examples):
  - CommandCounterIncrement

## Notes and Other Information
- Only operates if FirstSnapshotSet is true
- Updates CurrentSnapshot and SecondarySnapshot curcid fields
- Essential for proper intra-transaction command visibility
- Potential future enhancement: updating CatalogSnapshot curcid
- Located in src/backend/utils/time/snapmgr.c:456-476
- Part of PostgreSQL's command counter mechanism for transaction isolation