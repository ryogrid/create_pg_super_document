# PostPrepare_smgr

## Location
src/backend/catalog/storage.c: 918 - 938

## Overview
PostPrepare_smgr cleans up the in-memory state of pending relation deletes after a successful PREPARE statement in two-phase commit (2PC) protocol, transferring responsibility from the storage manager to the 2PC state file.

## Definition
```c
void PostPrepare_smgr(void)
```

## Detailed Description
This function is called during the post-prepare phase of two-phase commit transactions. When a transaction is prepared (PREPARE TRANSACTION), all pending relation deletions that were scheduled during the transaction must be preserved so they can be executed later during COMMIT PREPARED or discarded during ROLLBACK PREPARED.

The function iterates through the global pendingDeletes linked list and frees all PendingRelDelete entries from memory. This cleanup is safe because all the information about pending deletions has already been recorded in the 2PC state file, making it no longer the storage manager's responsibility to track these deletions in memory.

The function ensures that the storage manager's in-memory state is clean after a successful prepare, preventing memory leaks and avoiding confusion about which component is responsible for tracking pending deletions.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [PendingRelDelete](PendingRelDelete.md) (struct type for pending deletion tracking)
  - [pfree](../p/pfree.md) (memory deallocation function)
  - pendingDeletes (global variable - linked list of pending deletions)

- Called from (representative examples):
  - [PrepareTransaction](PrepareTransaction.md) (src/backend/access/transam/xact.c:2679)

## Notes and Other Information
- This function is part of PostgreSQL's two-phase commit implementation
- It specifically handles the storage manager's cleanup responsibilities during transaction preparation
- The function assumes that all pending deletion information has already been serialized to the 2PC state file
- After this function executes, the pendingDeletes list is empty and ready for new transactions
- This is a critical step in ensuring proper resource management during distributed transactions