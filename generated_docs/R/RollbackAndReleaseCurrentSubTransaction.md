# RollbackAndReleaseCurrentSubTransaction

## Location
[src/backend/access/transam/xact.c:4745-4810](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L4745-L4810)

## Overview
RollbackAndReleaseCurrentSubTransaction aborts and completely cleans up the innermost subtransaction, combining rollback and release operations in a single function for internal use.

## Definition

```c
void
RollbackAndReleaseCurrentSubTransaction(void)
```
## Detailed Description
RollbackAndReleaseCurrentSubTransaction combines the functionality of aborting and releasing the current subtransaction in a single operation. This function is designed for internal PostgreSQL operations that need to completely unwind the innermost subtransaction, regardless of its savepoint name.

The function performs a two-phase cleanup process:
1. **Abort Phase**: If the subtransaction is in TBLOCK_SUBINPROGRESS state, it calls AbortSubTransaction() to roll back all changes made within the subtransaction
2. **Cleanup Phase**: Always calls CleanupSubTransaction() to release resources and remove the subtransaction from the transaction stack

This function accepts subtransactions in both TBLOCK_SUBINPROGRESS and TBLOCK_SUBABORT states. For already-aborted subtransactions, it skips the abort phase and proceeds directly to cleanup. This flexibility makes it suitable for error recovery scenarios where the subtransaction state may be uncertain.

Like other internal subtransaction functions, it does not require CommitTransactionCommand/StartTransactionCommand cycling and is designed for use by procedural languages and internal systems. The function includes comprehensive state validation and will generate FATAL errors for unexpected transaction states.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [AbortSubTransaction](../A/AbortSubTransaction.md)
  - [CleanupSubTransaction](../C/CleanupSubTransaction.md)
  - [BlockStateAsString](../B/BlockStateAsString.md)
  - elog
- Transaction state constants:
  - TBLOCK_SUBINPROGRESS
  - TBLOCK_SUBABORT
  - TBLOCK_INPROGRESS, TBLOCK_IMPLICIT_INPROGRESS, TBLOCK_PARALLEL_INPROGRESS
  - TBLOCK_STARTED
- Global variables:
  - CurrentTransactionState
- Called from:
  - CHANGES_THRESHOLD (logical replication buffer management)
  - [ReorderBufferImmediateInvalidation](ReorderBufferImmediateInvalidation.md) (logical replication)
  - plperl_spi_* functions (PL/Perl error handling)
  - [PLy_abort_open_subtransactions](../P/PLy_abort_open_subtransactions.md) (PL/Python cleanup)
  - [PLy_spi_subtransaction_abort](../P/PLy_spi_subtransaction_abort.md) (PL/Python)
  - pltcl_subtrans_abort (PL/Tcl)

## Notes and Other Information
- Must not be used with CommitTransactionCommand/StartTransactionCommand - handles subtransaction lifecycle directly
- Accepts both in-progress and already-aborted subtransactions, making it suitable for error recovery paths
- Performs comprehensive state validation and asserts valid parent transaction states after cleanup
- Primarily used by procedural languages for exception handling and error recovery
- The function is safe to use during parallel operations for internal subtransactions
- Unlike user-level ROLLBACK TO SAVEPOINT commands, this function always operates on the innermost subtransaction
- Combines both abort and cleanup phases, ensuring complete subtransaction removal from the transaction stack