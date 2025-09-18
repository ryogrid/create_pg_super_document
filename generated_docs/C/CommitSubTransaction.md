# CommitSubTransaction

## Location
src/backend/access/transam/xact.c: 5048 - 5161

## Overview
CommitSubTransaction commits a subtransaction by performing cleanup operations and transferring resources to its parent transaction, ensuring that all subtransaction changes are made permanent while maintaining transaction hierarchy integrity.

## Definition


## Detailed Description
CommitSubTransaction is a static function responsible for committing a subtransaction in PostgreSQL's transaction management system. The function performs a comprehensive cleanup and resource transfer process:

1. **State Validation**: Checks that the subtransaction is in TRANS_INPROGRESS state
2. **Pre-commit Processing**: Executes pre-commit callbacks and handles parallel operations cleanup
3. **State Transition**: Changes the subtransaction state to TRANS_COMMIT
4. **Command Counter Update**: Increments the command counter to ensure subtransaction commands are visible
5. **Resource Cleanup**: Performs extensive cleanup of various subsystems including:
   - Child XIDs management
   - Triggers, portals, large objects
   - Notifications, relation cache, invalidation
   - Storage manager, locks, and resource owners
   - GUCs, SPI, namespace, files, hash tables, and statistics
6. **Memory Management**: Restores parent transaction's memory context
7. **Transaction Pop**: Removes the current subtransaction from the transaction stack

The function ensures that all subtransaction resources are properly transferred to the parent transaction or cleaned up appropriately.

## Parameters / Member Variables
This function takes no parameters and operates on the global CurrentTransactionState.

## Dependencies
- Functions called/Symbols referenced:
  - ShowTransactionState
  - TransStateAsString  
  - CallSubXactCallbacks
  - AtEOSubXact_Parallel
  - CommandCounterIncrement
  - AtSubCommit_childXids
  - AfterTriggerEndSubXact
  - AtSubCommit_Portals
  - AtEOSubXact_LargeObject
  - AtSubCommit_Notify
  - ResourceOwnerRelease
  - AtEOSubXact_RelationCache
  - AtEOSubXact_Inval
  - AtSubCommit_smgr
  - XactLockTableDelete
  - XidFromFullTransactionId
  - AtEOXact_GUC
  - AtEOSubXact_SPI
  - AtEOSubXact_on_commit_actions
  - AtEOSubXact_Namespace
  - AtEOSubXact_Files
  - AtEOSubXact_HashTables
  - AtEOSubXact_PgStat
  - AtSubCommit_Snapshot
  - ResourceOwnerDelete
  - AtSubCommit_Memory
  - PopTransaction
- Called from (representative examples):
  - CommitTransactionCommandInternal
  - ReleaseCurrentSubTransaction

## Notes and Other Information
- The function includes a warning that callers must reassign CurrentTransactionState local pointers after calling this function
- Prior to version 8.4, PostgreSQL marked subcommit in clog at this point, but now this is only done as part of the atomic update of the whole transaction tree
- The function handles parallel mode cleanup with warnings about leaked resources
- Only the subtransaction XID lock is actually released; other locks are transferred to the parent resource owner
- The function restores the upper transaction's read-only state to handle cases where parent and child have different read-only settings
- Located in src/backend/access/transam/xact.c:5048-5161