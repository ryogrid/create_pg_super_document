# PLy_abort_open_subtransactions

## Location
[src/pl/plpython/plpy_exec.c:1104-1126](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_exec.c#L1104-L1126)

## Overview
Forcibly aborts and cleans up lingering subtransactions that were explicitly started by plpy.subtransaction().start() but not properly closed by the Python code.

## Definition

```c
static void
PLy_abort_open_subtransactions(int save_subxact_level)
```
## Detailed Description
This function serves as a cleanup mechanism for PL/Python subtransaction management. It iterates through the explicit_subtransactions list and aborts any subtransactions that remain open beyond the specified save point level. For each open subtransaction, it issues a WARNING message to inform about the forced abort, calls RollbackAndReleaseCurrentSubTransaction() to perform the actual rollback, removes the subtransaction from the tracking list, restores the previous memory context and resource owner, and frees the associated subtransaction data structure. This ensures that incomplete subtransaction management in Python code doesn't leave the PostgreSQL transaction system in an inconsistent state.

## Parameters / Member Variables
- `save_subxact_level`: The target subtransaction nesting level to return to (number of subtransactions that should remain open)
## Dependencies
- Functions called/Symbols referenced:
  - [list_length](../l/list_length.md)
  - [list_delete_first](../l/list_delete_first.md)
  - linitial
  - [RollbackAndReleaseCurrentSubTransaction](../R/RollbackAndReleaseCurrentSubTransaction.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [pfree](../p/pfree.md)
  - ereport
  - [PLySubtransactionData](PLySubtransactionData.md)
- Called from (representative examples):
  - [PLy_procedure_call](PLy_procedure_call.md)

## Notes and Other Information
This function is typically called in PG_FINALLY blocks to ensure cleanup occurs regardless of how the procedure execution terminates (normal completion, error, or exception). The function issues WARNING messages for each forcibly aborted subtransaction to help developers identify problems with their subtransaction management code. The save_subxact_level parameter allows the function to preserve outer subtransactions that were already open before the current procedure call, only aborting those that were started during the current execution. The cleanup includes restoring PostgreSQL's memory context and resource owner state to maintain proper resource management.

## Simplified Source

```c
static void
PLy_abort_open_subtransactions(int save_subxact_level)
{
    Assert(save_subxact_level >= 0);

    // Abort all subtransactions beyond the save point level
    while (list_length(explicit_subtransactions) > save_subxact_level) {
        PLySubtransactionData *subtransactiondata;

        Assert(explicit_subtransactions != NIL);

        // Warn about the forced abort
        ereport(WARNING,
                (errmsg("forcibly aborting a subtransaction that has not been exited")));

        // Rollback and release the current subtransaction
        RollbackAndReleaseCurrentSubTransaction();

        // Remove from tracking list and clean up
        subtransactiondata = (PLySubtransactionData *) linitial(explicit_subtransactions);
        explicit_subtransactions = list_delete_first(explicit_subtransactions);

        // Restore previous context and resource owner
        MemoryContextSwitchTo(subtransactiondata->oldcontext);
        CurrentResourceOwner = subtransactiondata->oldowner;
        pfree(subtransactiondata);
    }
}
```