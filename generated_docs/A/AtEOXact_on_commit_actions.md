# AtEOXact_on_commit_actions

## Location
src/backend/commands/tablecmds.c: 17688 - 17719

## Overview
AtEOXact_on_commit_actions performs post-commit or post-abort cleanup for ON COMMIT management by removing no-longer-needed OnCommitItem entries from the global on_commits list.

## Definition
void AtEOXact_on_commit_actions(bool isCommit)

## Detailed Description
This function serves as the cleanup mechanism for the ON COMMIT actions system, managing the lifecycle of OnCommitItem entries in the global on_commits list. Its behavior depends on whether the transaction is committing or aborting. During commit, it removes entries that were marked for deletion during the transaction (deleting_subid != InvalidSubTransactionId). During abort, it removes entries that were created during the transaction (creating_subid != InvalidSubTransactionId).

For entries that survive the cleanup, the function resets both creating_subid and deleting_subid to InvalidSubTransactionId, effectively clearing their transaction-specific state and making them ready for future transactions.

## Parameters / Member Variables
- : Boolean flag indicating whether this is called during transaction commit (true) or abort (false)

## Dependencies
- Functions called/Symbols referenced:
  - [OnCommitItem](../O/OnCommitItem.md) (struct representing on-commit entries)
  - InvalidSubTransactionId (constant representing invalid subtransaction ID)
  - foreach_delete_current (macro to safely delete current list element during iteration)
  - [pfree](../p/pfree.md) (memory deallocation function)
- Called from:
  - [CommitTransaction](../C/CommitTransaction.md) (during transaction commit cleanup)
  - [PrepareTransaction](../P/PrepareTransaction.md) (during two-phase commit cleanup) 
  - [AbortTransaction](AbortTransaction.md) (during transaction abort cleanup)

## Notes and Other Information
- This function is part of the cleanup phase that occurs after PreCommit_on_commit_actions
- Uses foreach_delete_current to safely modify the list while iterating over it
- The function handles both successful commits and transaction aborts with different cleanup logic
- Preserved entries have their subtransaction IDs reset to prepare them for future use
- Memory management is handled by freeing removed OnCommitItem structures with pfree