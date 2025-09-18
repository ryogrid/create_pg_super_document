# AtEOSubXact_on_commit_actions

## Location
[src/backend/commands/tablecmds.c:17720-17754](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L17720-L17754)

## Overview
AtEOSubXact_on_commit_actions performs post-subcommit or post-subabort cleanup for ON COMMIT management by handling OnCommitItem entries associated with the ending subtransaction.

## Definition
void AtEOSubXact_on_commit_actions(bool isCommit, SubTransactionId mySubid, SubTransactionId parentSubid)

## Detailed Description
This function manages the lifecycle of OnCommitItem entries during subtransaction completion. When a subtransaction aborts, it immediately removes entries that were created during that subtransaction, as they are no longer valid. When a subtransaction commits, it transfers ownership of entries to the parent subtransaction by updating their subtransaction IDs.

The function handles two types of subtransaction IDs for each OnCommitItem: creating_subid (tracks which subtransaction created the entry) and deleting_subid (tracks which subtransaction marked it for deletion). During subcommit, both IDs are transferred to the parent subtransaction. During subabort, entries created in the aborting subtransaction are deleted, while entries marked for deletion have their deleting_subid reset to InvalidSubTransactionId.

## Parameters / Member Variables
- : Boolean flag indicating whether this is called during subtransaction commit (true) or abort (false)
- : SubTransactionId of the subtransaction that is ending
- : SubTransactionId of the parent subtransaction that will inherit responsibilities

## Dependencies
- Functions called/Symbols referenced:
  - SubTransactionId (type representing subtransaction identifiers)
  - [OnCommitItem](../O/OnCommitItem.md) (struct representing on-commit entries)
  - foreach_delete_current (macro to safely delete current list element during iteration)
  - [pfree](../p/pfree.md) (memory deallocation function)
  - InvalidSubTransactionId (constant representing invalid subtransaction ID)
- Called from:
  - [CommitSubTransaction](../C/CommitSubTransaction.md) (during subtransaction commit cleanup)
  - [AbortSubTransaction](AbortSubTransaction.md) (during subtransaction abort cleanup)

## Notes and Other Information
- This function is the subtransaction counterpart to AtEOXact_on_commit_actions
- Uses different logic for subcommit vs subabort to properly handle ownership transfer
- During subabort, only entries created in the aborting subtransaction are removed
- During subcommit, ownership is transferred to the parent subtransaction for both creating and deleting operations
- The deleting_subid handling differs between commit and abort cases to maintain proper cleanup semantics
- Memory management ensures that removed OnCommitItem structures are properly freed