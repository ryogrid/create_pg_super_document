# AtEOSubXact_PgStat

## Location
[src/backend/utils/activity/pgstat_xact.c:112-134](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_xact.c#L112-L134)

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
  - [AtEOSubXact_PgStat_Relations](AtEOSubXact_PgStat_Relations.md)
  - [AtEOSubXact_PgStat_DroppedStats](AtEOSubXact_PgStat_DroppedStats.md)
  - [pfree](../p/pfree.md)
  - [PgStat_SubXactStatus](../P/PgStat_SubXactStatus.md) (struct type)
- Called from (representative examples):
  - [CommitSubTransaction](../C/CommitSubTransaction.md) (src/backend/access/transam/xact.c:5136)
  - [AbortSubTransaction](AbortSubTransaction.md) (src/backend/access/transam/xact.c:5300)

## Notes and Other Information
- This function is called from access/transam/xact.c at subtransaction commit/abort
- It manages the pgStatXactStack by removing the completed subtransaction's state and linking to the previous state
- The function checks that the transaction state exists and has the expected nesting level before processing
- The subtransaction state is immediately delinked from the stack to simplify reuse cases
- Unlike top-level transactions, subtransactions merge their statistics into the parent rather than finalizing them
- The function handles both commit and abort scenarios, with the specific processing logic delegated to specialized relation and dropped stats handlers

## Simplified Source

```c
// Simplified version of AtEOSubXact_PgStat
void AtEOSubXact_PgStat(bool isCommit, int nestDepth) {
    PgStat_SubXactStatus *xact_state;

    // Get the current transaction state from the stack
    xact_state = pgStatXactStack;

    // Check if we have a valid state at the expected nesting level
    if (xact_state != NULL && xact_state->nest_level >= nestDepth) {
        // Remove this subtransaction state from the stack
        pgStatXactStack = xact_state->prev;

        // Process relation statistics for this subtransaction
        AtEOSubXact_PgStat_Relations(xact_state, isCommit, nestDepth);

        // Process dropped statistics for this subtransaction
        AtEOSubXact_PgStat_DroppedStats(xact_state, isCommit, nestDepth);

        // Free the subtransaction state memory
        pfree(xact_state);
    }
}
```

Key simplifications made:
- Added descriptive comments explaining each logical step
- Clarified the stack management operation with better variable naming context
- Simplified the conditional check explanation
- Maintained the essential algorithm flow: check state, unlink from stack, process statistics, cleanup memory
- Preserved the delegation pattern to specialized handlers for relations and dropped stats