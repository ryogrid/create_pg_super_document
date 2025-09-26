# AtEOSubXact_PgStat

## Location
src/backend/utils/activity/pgstat_xact.c: 112 - 134

## Overview
Handles the end-of-subtransaction cleanup for PostgreSQL statistics, merging sub-transaction statistics into the parent transaction and managing the transaction statistics stack.

## Definition
```c
void AtEOSubXact_PgStat(bool isCommit, int nestDepth)
```

## Detailed Description
AtEOSubXact_PgStat manages the completion of subtransactions in PostgreSQL's statistics subsystem. When a subtransaction ends (either by committing or aborting), this function merges the subtransaction's accumulated statistics into its parent transaction's statistics. It operates on the pgStatXactStack, which maintains a stack of nested transaction states. The function removes the current subtransaction's state from the stack and processes both relation statistics and dropped statistics operations before freeing the subtransaction's state structure.

## Parameters / Member Variables
- `isCommit`: Boolean indicating whether the subtransaction is committing (true) or aborting (false)
- `nestDepth`: Integer representing the nesting level of the subtransaction being completed

## Dependencies
- Functions called/Symbols referenced:
  - AtEOSubXact_PgStat_Relations
  - AtEOSubXact_PgStat_DroppedStats
  - pfree
  - PgStat_SubXactStatus (struct type)
- Called from (representative examples):
  - CommitSubTransaction (src/backend/access/transam/xact.c:5136)
  - AbortSubTransaction (src/backend/access/transam/xact.c:5300)

## Notes and Other Information
- This function is called from access/transam/xact.c at subtransaction commit/abort
- It manages the pgStatXactStack by removing the completed subtransaction's state and linking to the previous state
- The function checks that the transaction state exists and has the expected nesting level before processing
- The subtransaction state is immediately delinked from the stack to simplify reuse cases
- Unlike top-level transactions, subtransactions merge their statistics into the parent rather than finalizing them
- The function handles both commit and abort scenarios, with the specific processing logic delegated to specialized relation and dropped stats handlers