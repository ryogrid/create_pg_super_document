# PreCommit_on_commit_actions

## Location
src/backend/commands/tablecmds.c: 17581 - 17687

## Overview
PreCommit_on_commit_actions performs ON COMMIT actions for temporary tables just before transaction commit, handling both table truncation (ON COMMIT DELETE ROWS) and table dropping (ON COMMIT DROP).

## Definition
void PreCommit_on_commit_actions(void)

## Detailed Description
This function processes all pending ON COMMIT actions registered for temporary tables in the current transaction. It is called during the pre-commit phase to handle deferred actions that were specified when creating temporary tables. The function operates in two phases: first truncating tables marked with ON COMMIT DELETE ROWS, then dropping tables marked with ON COMMIT DROP. This ordering ensures proper dependency handling between relations.

The function optimizes performance by skipping truncation of ON COMMIT DELETE ROWS tables if the transaction hasn't accessed the temporary namespace, since such tables would still be empty. For table drops, it uses the internal deletion mechanism with CASCADE behavior to handle dependencies automatically.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - OnCommitItem (struct for on-commit entries)
  - lappend_oid (append OID to list)
  - heap_truncate (truncate heap relations)
  - new_object_addresses (create object address collection)
  - object_address_present (check if object exists in collection)
  - add_exact_object_address (add object to collection)
  - GetTransactionSnapshot (get current transaction snapshot)
  - PushActiveSnapshot/PopActiveSnapshot (snapshot management)
  - performMultipleDeletions (delete multiple database objects)
- Called from:
  - CommitTransaction (during normal transaction commit)
  - PrepareTransaction (during two-phase commit preparation)

## Notes and Other Information
- The function processes the global 'on_commits' list containing OnCommitItem entries
- Handles four ON COMMIT behaviors: NOOP, PRESERVE_ROWS, DELETE_ROWS, and DROP
- Uses XACT_FLAGS_ACCESSEDTEMPNAMESPACE flag to optimize truncation operations
- Employs PERFORM_DELETION_INTERNAL and PERFORM_DELETION_QUIETLY flags for automatic drops
- Includes assertion checking to verify that dropped tables are properly marked as deleted
- Truncation occurs before dropping to ensure all dependencies are properly resolved