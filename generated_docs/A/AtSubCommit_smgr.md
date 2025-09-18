# AtSubCommit_smgr

## Location
src/backend/catalog/storage.c: 939 - 958

## Overview
AtSubCommit_smgr handles subtransaction commit by reassigning all pending relation deletions from the committing subtransaction to its parent transaction, ensuring proper cleanup ownership transfer.

## Definition
```c
void AtSubCommit_smgr(void)
```

## Detailed Description
This function is called during subtransaction commit processing to handle the transfer of pending relation deletions from the committing subtransaction to its parent transaction. When a subtransaction commits, any relation deletions that were scheduled within that subtransaction need to remain pending until the top-level transaction commits or aborts.

The function works by iterating through the global pendingDeletes linked list and adjusting the nesting level of all entries that belong to the current subtransaction or deeper nested levels. It decreases the nestLevel by 1, effectively transferring ownership to the parent transaction level.

This ensures that when the parent transaction eventually commits or aborts, it will properly handle all the relation deletions that were originally scheduled in its committed subtransactions.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - GetCurrentTransactionNestLevel (returns current transaction nesting level)
  - PendingRelDelete (struct type for pending deletion tracking)
  - pendingDeletes (global variable - linked list of pending deletions)

- Called from (representative examples):
  - CommitSubTransaction (src/backend/access/transam/xact.c:5108)

## Notes and Other Information
- This function is part of PostgreSQL's nested transaction (savepoint) implementation
- It ensures that relation deletions scheduled in subtransactions are not lost when the subtransaction commits
- The nesting level adjustment is crucial for proper cleanup during parent transaction commit/abort
- Only affects pending deletions at the current nesting level or deeper
- Works in conjunction with AtSubAbort_smgr for complete subtransaction cleanup handling
- The function preserves the deletion schedule across subtransaction boundaries