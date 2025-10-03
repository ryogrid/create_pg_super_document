# BeginInternalSubTransaction

## Location
[src/backend/access/transam/xact.c:4643-4716](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L4643-L4716)

## Overview
BeginInternalSubTransaction creates an internal subtransaction that can be safely called from various transaction states, automatically handling transaction command cycling and providing more flexible usage than regular savepoints.

## Definition

```c
void
BeginInternalSubTransaction(const char *name)
```
## Detailed Description
BeginInternalSubTransaction is a specialized version of DefineSavepoint designed for internal PostgreSQL operations that need subtransaction functionality. Unlike regular savepoints, it can be safely called from a wider range of transaction states including TBLOCK_STARTED, TBLOCK_IMPLICIT_INPROGRESS, TBLOCK_PARALLEL_INPROGRESS, TBLOCK_END, and TBLOCK_PREPARE.

This function is particularly useful for:
- Procedural language implementations (PL/pgSQL, PL/Perl, PL/Python, PL/Tcl)
- Logical replication operations
- Deferred trigger execution at COMMIT/PREPARE time
- Any internal operations that might be called outside explicit transaction blocks

The function automatically handles the transaction command cycling by calling CommitTransactionCommand() followed by StartTransactionCommand(), relieving callers from managing these details. It also sets ExitOnAnyError to true during execution to prevent transaction state corruption if errors occur during subtransaction setup.

Unlike regular subtransactions, internal subtransactions are allowed during parallel operations, provided no new XIDs or command IDs are assigned (enforced elsewhere in AssignTransactionId() and CommandCounterIncrement()).

## Parameters / Member Variables
- `*name`: Optional name for the subtransaction savepoint, stored in TopTransactionContext if provided
## Dependencies
- Functions called/Symbols referenced:
  - [PushTransaction](../P/PushTransaction.md)
  - [MemoryContextStrdup](../M/MemoryContextStrdup.md)
  - [CommitTransactionCommand](../C/CommitTransactionCommand.md)
  - [StartTransactionCommand](../S/StartTransactionCommand.md)
  - [BlockStateAsString](BlockStateAsString.md)
  - elog
- Transaction state constants:
  - TBLOCK_STARTED, TBLOCK_INPROGRESS, TBLOCK_IMPLICIT_INPROGRESS
  - TBLOCK_PARALLEL_INPROGRESS, TBLOCK_END, TBLOCK_PREPARE
  - TBLOCK_SUBINPROGRESS
- Global variables:
  - ExitOnAnyError
  - TopTransactionContext
- Called from:
  - [ReorderBufferProcessTXN](../R/ReorderBufferProcessTXN.md) (logical replication)
  - [ReorderBufferImmediateInvalidation](../R/ReorderBufferImmediateInvalidation.md) (logical replication)
  - plperl_spi_* functions (PL/Perl SPI operations)
  - [PLy_spi_subtransaction_begin](../P/PLy_spi_subtransaction_begin.md) (PL/Python)
  - [pltcl_subtrans_begin](../p/pltcl_subtrans_begin.md) (PL/Tcl)

## Notes and Other Information
- Automatically manages ExitOnAnyError flag to ensure FATAL exit on internal errors, preventing transaction state corruption
- Unlike DefineSavepoint, this function allows operation during parallel mode for internal subtransactions
- The function performs automatic CommitTransactionCommand/StartTransactionCommand cycling, making it suitable for use in contexts where transaction state management is complex
- Memory for savepoint names is allocated in TopTransactionContext to ensure proper lifetime management
- Primarily used by procedural languages and internal PostgreSQL subsystems that need subtransaction capabilities without explicit transaction management