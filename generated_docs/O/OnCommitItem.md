# OnCommitItem

## Location
src/backend/commands/tablecmds.c: 113 - 127

## Overview
OnCommitItem is a structure that manages ON COMMIT actions for temporary tables and other relations in PostgreSQL. It tracks what action should be performed on a relation at transaction commit or abort time.

## Definition


## Detailed Description
The OnCommitItem structure is used to maintain a list of ON COMMIT actions that need to be executed at the end of a transaction. This is primarily used for temporary tables that have been created with specific ON COMMIT behavior (such as DELETE ROWS, DROP, or PRESERVE ROWS). The structure tracks both the action to be performed and the subtransaction context in which the item was created or marked for deletion, enabling proper cleanup in case of subtransaction rollback.

## Parameters / Member Variables
- : The object identifier (OID) of the relation this ON COMMIT action applies to
- : The specific action to perform at transaction end (of type OnCommitAction)
- : The subtransaction ID that created this entry; zero if created in a prior transaction
- : The subtransaction ID that marked this entry for deletion; zero if no deletion is pending

## Dependencies
- Functions called/Symbols referenced:
  - OnCommitAction (enum type)
  - SubTransactionId (type)
- Called from (representative examples):
  - register_on_commit_action
  - remove_on_commit_action
  - PreCommit_on_commit_actions
  - AtEOXact_on_commit_actions
  - AtEOSubXact_on_commit_actions

## Notes and Other Information
- This structure is part of PostgreSQL's transaction management system for handling temporary table cleanup
- The subtransaction tracking allows for proper rollback behavior when nested transactions are involved
- Used internally by the table command processing system to ensure proper cleanup of temporary relations