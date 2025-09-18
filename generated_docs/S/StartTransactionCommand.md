# StartTransactionCommand

## Location
[src/backend/access/transam/xact.c:2995-3071](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L2995-L3071)

## Overview
StartTransactionCommand manages the initiation of command processing within PostgreSQL's transaction framework, handling different transaction block states appropriately.

## Definition


## Detailed Description
StartTransactionCommand is the entry point for beginning command execution within PostgreSQL's transaction system. Unlike StartTransaction() which creates a new transaction, this function manages the transition into command processing based on the current transaction block state.

The function operates through a state machine that handles various transaction block states:
- **TBLOCK_DEFAULT**: Not in a transaction block - starts a new transaction via StartTransaction() and transitions to TBLOCK_STARTED
- **TBLOCK_INPROGRESS/TBLOCK_IMPLICIT_INPROGRESS/TBLOCK_SUBINPROGRESS**: Already in active transaction states - no action needed as the transaction is ready for command processing
- **TBLOCK_ABORT/TBLOCK_SUBABORT**: In failed transaction states - remains in abort state, awaiting ROLLBACK command
- **Invalid states**: Various intermediate states that should not occur at command start - triggers ERROR

After handling state-specific logic, the function ensures the memory context is switched to CurTransactionContext, providing the appropriate memory environment for command execution.

## Parameters / Member Variables
This function takes no parameters but operates on:
- : The current transaction's state structure
- : Transaction block state that determines the action taken
- : Memory context switched to before returning

## Dependencies
- Functions called/Symbols referenced:
  - [StartTransaction](StartTransaction.md) (called only when in TBLOCK_DEFAULT state)
  - [BlockStateAsString](../B/BlockStateAsString.md) (for error reporting in invalid states)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) (ensures correct memory context)
  - Various TBLOCK_* constants (transaction block state enumeration)

- Called from (representative examples):
  - [start_xact_command](../s/start_xact_command.md) (main command processing entry point)
  - [ParallelWorkerMain](../P/ParallelWorkerMain.md) (parallel worker initialization)
  - Various background processes (autovacuum, logical replication workers)
  - SPI functions (_SPI_commit, _SPI_rollback)
  - Maintenance commands (VACUUM, CLUSTER, REINDEX)

## Notes and Other Information
- This is a public function (not static) used throughout the PostgreSQL system
- Acts as a state machine dispatcher based on transaction block state
- Only actually starts a new transaction when in TBLOCK_DEFAULT state
- Ensures proper memory context setup for all command execution paths
- Commands in failed transaction states (TBLOCK_ABORT/TBLOCK_SUBABORT) can only execute ROLLBACK
- The function includes extensive validation of transaction block states to catch programming errors
- Complements CommitTransactionCommand() to bracket command execution within the transaction system
- Critical for maintaining transaction semantics across different execution contexts (normal commands, background workers, parallel workers)