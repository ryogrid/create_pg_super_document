# ReleaseCurrentSubTransaction

## Location
[src/backend/access/transam/xact.c:4717-4744](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L4717-L4744)

## Overview
ReleaseCurrentSubTransaction commits the innermost subtransaction, regardless of its savepoint name, and is designed for internal use without requiring CommitTransactionCommand/StartTransactionCommand cycling.

## Definition

```c
void
ReleaseCurrentSubTransaction(void)
```
## Detailed Description
ReleaseCurrentSubTransaction performs a commit operation on the current innermost subtransaction. This is a specialized function designed for internal PostgreSQL operations that need to commit subtransactions without the overhead and complexity of the full transaction command processing cycle.

The function directly calls CommitSubTransaction() to perform the actual commit work and automatically switches to the parent transaction's memory context (CurTransactionContext) before committing. This ensures proper memory management during the subtransaction cleanup process.

Unlike user-level RELEASE SAVEPOINT commands, this function:
- Does not require a specific savepoint name
- Always operates on the innermost subtransaction
- Does not use CommitTransactionCommand/StartTransactionCommand
- Is designed for internal procedural language and system usage

The function includes strict state validation, ensuring it's only called when the current transaction is in TBLOCK_SUBINPROGRESS state and TRANS_INPROGRESS transaction state. Like BeginInternalSubTransaction, it permits operation during parallel mode for internal subtransactions.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - [CommitSubTransaction](../C/CommitSubTransaction.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [BlockStateAsString](../B/BlockStateAsString.md)
  - elog
- Transaction state constants:
  - TBLOCK_SUBINPROGRESS
  - TRANS_INPROGRESS
- Global variables:
  - CurrentTransactionState
  - CurTransactionContext
- Called from:
  - plperl_spi_* functions (PL/Perl SPI operations)
  - [PLy_spi_subtransaction_commit](../P/PLy_spi_subtransaction_commit.md) (PL/Python)
  - [PLy_subtransaction_exit](../P/PLy_subtransaction_exit.md) (PL/Python)
  - [pltcl_subtrans_commit](../p/pltcl_subtrans_commit.md) (PL/Tcl)

## Notes and Other Information
- Must not be used with CommitTransactionCommand/StartTransactionCommand - it handles subtransaction lifecycle directly
- Automatically manages memory context switching to ensure proper cleanup during commit
- Includes assertions to verify transaction state consistency before and after the operation
- Primarily used by procedural languages for managing internal subtransactions
- Unlike user-level savepoint operations, this function always commits the innermost subtransaction regardless of naming
- Designed to work safely during parallel operations for internal subtransactions that don't assign new XIDs or command IDs