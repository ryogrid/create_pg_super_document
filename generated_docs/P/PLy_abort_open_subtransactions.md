# PLy_abort_open_subtransactions

## Location
src/pl/plpython/plpy_exec.c: 1104 - 1126

## Overview
Forcibly aborts and cleans up lingering subtransactions that were explicitly started by plpy.subtransaction().start() but not properly closed by the Python code.

## Definition


## Detailed Description
This function serves as a cleanup mechanism for PL/Python subtransaction management. It iterates through the explicit_subtransactions list and aborts any subtransactions that remain open beyond the specified save point level. For each open subtransaction, it issues a WARNING message to inform about the forced abort, calls RollbackAndReleaseCurrentSubTransaction() to perform the actual rollback, removes the subtransaction from the tracking list, restores the previous memory context and resource owner, and frees the associated subtransaction data structure. This ensures that incomplete subtransaction management in Python code doesn't leave the PostgreSQL transaction system in an inconsistent state.

## Parameters / Member Variables
- : The target subtransaction nesting level to return to (number of subtransactions that should remain open)

## Dependencies
- Functions called/Symbols referenced:
  - list_length
  - list_delete_first
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