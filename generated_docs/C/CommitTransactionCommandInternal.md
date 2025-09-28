# CommitTransactionCommandInternal

## Location
[src/backend/access/transam/xact.c:3111-3386](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L3111-L3386)

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

## Dependencies
- Functions called/Symbols referenced:
  - CurrentTransactionState (global transaction state)
  - [SaveTransactionCharacteristics](../S/SaveTransactionCharacteristics.md)/RestoreTransactionCharacteristics (transaction chaining)
  - [CommitTransaction](CommitTransaction.md)/AbortTransaction/CleanupTransaction (main transaction operations)
  - [StartTransaction](../S/StartTransaction.md)/PrepareTransaction (transaction lifecycle)
  - [StartSubTransaction](../S/StartSubTransaction.md)/CommitSubTransaction/AbortSubTransaction/CleanupSubTransaction (subtransaction operations)
  - [CommandCounterIncrement](CommandCounterIncrement.md) (command sequencing)
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

## Simplified Source

```c
// Simplified version of CommitTransactionCommandInternal
static bool CommitTransactionCommandInternal(void) {
    TransactionState s = CurrentTransactionState;
    SavedTransactionCharacteristics savetc;

    // Save transaction characteristics for potential chaining
    SaveTransactionCharacteristics(&savetc);

    // Process based on current transaction block state
    switch (s->blockState) {
        case TBLOCK_DEFAULT:
        case TBLOCK_PARALLEL_INPROGRESS:
            elog(FATAL, "CommitTransactionCommand: unexpected state %s",
                 BlockStateAsString(s->blockState));
            break;

        case TBLOCK_STARTED:
            // Simple transaction commit
            CommitTransaction();
            s->blockState = TBLOCK_DEFAULT;
            break;

        case TBLOCK_BEGIN:
            // Begin transaction block
            s->blockState = TBLOCK_INPROGRESS;
            break;

        case TBLOCK_INPROGRESS:
        case TBLOCK_IMPLICIT_INPROGRESS:
        case TBLOCK_SUBINPROGRESS:
            // Command within transaction - just increment counter
            CommandCounterIncrement();
            break;

        case TBLOCK_END:
            // Commit transaction, handle chaining
            CommitTransaction();
            s->blockState = TBLOCK_DEFAULT;
            if (s->chain) {
                StartTransaction();
                s->blockState = TBLOCK_INPROGRESS;
                s->chain = false;
                RestoreTransactionCharacteristics(&savetc);
            }
            break;

        case TBLOCK_ABORT:
        case TBLOCK_SUBABORT:
            // Stay in abort state until ROLLBACK
            break;

        case TBLOCK_ABORT_END:
            // Clean up aborted transaction
            CleanupTransaction();
            s->blockState = TBLOCK_DEFAULT;
            if (s->chain) {
                StartTransaction();
                s->blockState = TBLOCK_INPROGRESS;
                s->chain = false;
                RestoreTransactionCharacteristics(&savetc);
            }
            break;

        case TBLOCK_ABORT_PENDING:
            // Abort and clean up
            AbortTransaction();
            CleanupTransaction();
            s->blockState = TBLOCK_DEFAULT;
            if (s->chain) {
                StartTransaction();
                s->blockState = TBLOCK_INPROGRESS;
                s->chain = false;
                RestoreTransactionCharacteristics(&savetc);
            }
            break;

        case TBLOCK_PREPARE:
            // Two-phase commit
            PrepareTransaction();
            s->blockState = TBLOCK_DEFAULT;
            break;

        case TBLOCK_SUBBEGIN:
            // Start subtransaction
            StartSubTransaction();
            s->blockState = TBLOCK_SUBINPROGRESS;
            break;

        case TBLOCK_SUBRELEASE:
            // Release savepoint(s)
            do {
                CommitSubTransaction();
                s = CurrentTransactionState;
            } while (s->blockState == TBLOCK_SUBRELEASE);
            break;

        case TBLOCK_SUBCOMMIT:
            // Commit subtransaction hierarchy
            do {
                CommitSubTransaction();
                s = CurrentTransactionState;
            } while (s->blockState == TBLOCK_SUBCOMMIT);

            // Handle main transaction completion
            if (s->blockState == TBLOCK_END) {
                CommitTransaction();
                s->blockState = TBLOCK_DEFAULT;
                if (s->chain) {
                    StartTransaction();
                    s->blockState = TBLOCK_INPROGRESS;
                    s->chain = false;
                    RestoreTransactionCharacteristics(&savetc);
                }
            } else if (s->blockState == TBLOCK_PREPARE) {
                PrepareTransaction();
                s->blockState = TBLOCK_DEFAULT;
            }
            break;

        case TBLOCK_SUBABORT_END:
            // Clean up failed subtransaction - need another iteration
            CleanupSubTransaction();
            return false;

        case TBLOCK_SUBABORT_PENDING:
            // Abort and clean up subtransaction - need another iteration
            AbortSubTransaction();
            CleanupSubTransaction();
            return false;

        case TBLOCK_SUBRESTART:
        case TBLOCK_SUBABORT_RESTART:
            // Handle ROLLBACK TO savepoint
            {
                char *name = s->name;
                int savepointLevel = s->savepointLevel;
                s->name = NULL;

                if (s->blockState == TBLOCK_SUBRESTART) {
                    AbortSubTransaction();
                }
                CleanupSubTransaction();

                // Create new subtransaction with same name
                DefineSavepoint(NULL);
                s = CurrentTransactionState;
                s->name = name;
                s->savepointLevel = savepointLevel;
                StartSubTransaction();
                s->blockState = TBLOCK_SUBINPROGRESS;
            }
            break;
    }

    return true; // No more iterations needed
}
```

Key simplifications made:
- Removed detailed comments (preserved essential logic)
- Consolidated similar case handling
- Added high-level comments for each major case
- Preserved all essential state machine transitions
- Maintained transaction chaining support