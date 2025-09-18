# AtSubCommit_Snapshot

## Location
src/backend/utils/time/snapmgr.c: 938 - 958

## Overview
Transfers ownership of active snapshots from a subtransaction to its parent transaction during subtransaction commit, ensuring proper snapshot lifecycle management across transaction boundaries.

## Definition


## Detailed Description
This function is called during subtransaction commit to handle the transfer of snapshot ownership from the committing subtransaction to its parent. It iterates through the active snapshot stack and relabels all snapshots that belong to the current subtransaction level, changing their ownership to the parent subtransaction level.

The function ensures that snapshots created within a subtransaction continue to be available to the parent transaction after the subtransaction commits successfully. This is crucial for maintaining consistent visibility semantics across nested transaction boundaries.

The iteration stops when it encounters a snapshot with a level lower than the current subtransaction, as those snapshots already belong to outer transaction levels and should not be modified.

## Parameters / Member Variables
- : The subtransaction level that is being committed (snapshots at this level will be transferred to level-1)

## Dependencies
- Functions called/Symbols referenced:
  - ActiveSnapshotElt
- Called from (representative examples):
  - CommitSubTransaction
  - IsMVCCSnapshot (via header inclusion)

## Notes and Other Information
- Part of PostgreSQL's nested transaction (savepoint) implementation
- Only affects snapshots at the exact subtransaction level being committed
- Decrements the as_level field by 1 to transfer ownership to parent
- Works with the ActiveSnapshot linked list structure
- Critical for proper cleanup and ownership tracking in nested transactions
- Ensures that useful snapshots persist beyond subtransaction boundaries