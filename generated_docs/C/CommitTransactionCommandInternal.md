# CommitTransactionCommandInternal

## Location
src/backend/access/transam/xact.c: 3111 - 3386

## Overview
CommitTransactionCommandInternal handles the core logic for committing transactions and subtransactions through a comprehensive state machine, processing one iteration of commit work per call and supporting transaction chaining.

## Definition
```c
static bool CommitTransactionCommandInternal(void)
```

## Detailed Description
CommitTransactionCommandInternal is the central state machine that manages the complex process of transaction and subtransaction commits in PostgreSQL. This static function implements the core commit logic through a comprehensive switch statement that handles all possible transaction block states.

The function processes one iteration of commit work per call, returning true when all work is complete or false when additional iterations are required (particularly for nested subtransactions). This iterative design prevents dangerous recursion that could occur with deeply nested subtransaction structures.

Key functionality includes:
- Managing transaction block state transitions
- Handling transaction chaining with characteristic preservation
- Processing savepoint operations (ROLLBACK TO, RELEASE)
- Supporting two-phase commit (PREPARE TRANSACTION)
- Managing subtransaction hierarchies
- Implementing proper cleanup for aborted transactions

The function saves transaction characteristics at the beginning and restores them when starting chained transactions, ensuring consistency across transaction boundaries.

## Parameters / Member Variables
- Returns: `bool` - true when no more iterations are required, false when additional processing is needed
- No input parameters (accesses global transaction state)

## Dependencies
- Functions called/Symbols referenced:
  - CurrentTransactionState (global transaction state)
  - [SaveTransactionCharacteristics](../S/SaveTransactionCharacteristics.md)/RestoreTransactionCharacteristics (transaction chaining)
  - [CommitTransaction](CommitTransaction.md)/AbortTransaction/CleanupTransaction (main transaction operations)
  - [StartTransaction](../S/StartTransaction.md)/PrepareTransaction (transaction lifecycle)
  - [StartSubTransaction](../S/StartSubTransaction.md)/CommitSubTransaction/AbortSubTransaction/CleanupSubTransaction (subtransaction operations)
  - CommandCounterIncrement (command sequencing)
  - [DefineSavepoint](../D/DefineSavepoint.md) (savepoint creation)
  - [BlockStateAsString](../B/BlockStateAsString.md) (error reporting)
  - Various TBLOCK_* constants (transaction block states)
- Called from (representative examples):
  - [CommitTransactionCommand](CommitTransactionCommand.md) (wrapper function)

## Notes and Other Information
- Located in src/backend/access/transam/xact.c:3111-3386
- Implements a comprehensive state machine with 15+ different transaction states
- Supports transaction chaining by preserving and restoring transaction characteristics
- Handles O(N²) complexity for subtransaction resource cleanup (acceptable for typical use cases)
- Critical for PostgreSQL's ACID compliance and transaction integrity
- The function's static nature indicates it's an internal implementation detail
- Returns false specifically for TBLOCK_SUBABORT_END and TBLOCK_SUBABORT_PENDING to trigger additional iterations
- Extensive error checking with FATAL errors for unexpected states
- Supports both explicit transactions (BEGIN/COMMIT) and implicit transactions