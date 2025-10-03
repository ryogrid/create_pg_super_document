# AtSubAbort_smgr

## Location
[src/backend/catalog/storage.c:959-964](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/storage.c#L959-L964)

## Overview
AtSubAbort_smgr handles subtransaction abort by immediately executing pending relation deletions for the aborting subtransaction, since it will not commit.

## Definition
```c
void AtSubAbort_smgr(void)
```

## Detailed Description
This function is called during subtransaction abort processing to handle immediate cleanup of relations that were scheduled for deletion within the aborting subtransaction. Unlike subtransaction commit where pending deletions are transferred to the parent transaction, subtransaction abort allows for immediate execution of these deletions since the subtransaction will definitively not commit.

The function delegates the actual deletion work to smgrDoPendingDeletes(false), which processes all pending deletions at the current transaction nesting level. The 'false' parameter indicates this is an abort scenario, allowing the function to immediately delete relations that were created in the subtransaction and forget about relations that were marked for deletion.

This immediate processing is safe and efficient because there's no possibility of the subtransaction committing, eliminating the need to defer deletions until the top-level transaction completes.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [smgrDoPendingDeletes](../s/smgrDoPendingDeletes.md) (executes pending relation deletions, called with 'false' for abort scenario)

- Called from (representative examples):
  - [AbortSubTransaction](AbortSubTransaction.md) (src/backend/access/transam/xact.c:5289)

## Notes and Other Information
- This function is part of PostgreSQL's nested transaction (savepoint) rollback implementation
- It provides immediate cleanup of relations during subtransaction abort, optimizing resource usage
- Works in conjunction with AtSubCommit_smgr to provide complete subtransaction cleanup handling
- The immediate execution approach is more efficient than deferring deletions to top-level transaction abort
- Handles both relations created within the subtransaction (which should be deleted) and relations marked for deletion (which should be preserved)
- The function ensures that aborted subtransactions don't leave behind temporary relations or incorrect deletion schedules

## Simplified Source

```c
// Simplified version of AtSubAbort_smgr
void AtSubAbort_smgr(void) {
    // Immediately execute pending deletions for aborting subtransaction
    // Safe to delete immediately since subtransaction will not commit
    smgrDoPendingDeletes(false);
}
```

Key simplifications made:
- Function is already very simple - only one line of core logic
- Added clarifying comments explaining the immediate deletion safety
- The 'false' parameter indicates abort scenario processing
- Core logic: delegate to smgrDoPendingDeletes for actual deletion work