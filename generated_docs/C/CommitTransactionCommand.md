# CommitTransactionCommand

## Location
[src/backend/access/transam/xact.c:3093-3110](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L3093-L3110)

## Overview
CommitTransactionCommand is a wrapper function that handles the iterative process of committing transactions and subtransactions, preventing dangerous recursion in CommitTransactionCommandInternal.

## Definition
```c
void CommitTransactionCommand(void)
```

## Detailed Description
CommitTransactionCommand serves as a safe wrapper around the core transaction commit logic implemented in CommitTransactionCommandInternal. The function's primary purpose is to prevent potentially dangerous recursion that could occur when handling complex transaction structures involving subtransactions.

The function implements a simple iterative approach, repeatedly calling CommitTransactionCommandInternal until all transaction-related work is completed. This design ensures that nested subtransactions are properly handled without risking stack overflow from recursive calls. The loop continues until CommitTransactionCommandInternal returns true, indicating that all transaction commit work has been successfully completed.

This wrapper pattern is essential for handling PostgreSQL's hierarchical transaction model, where transactions can contain multiple levels of subtransactions that must be committed in the proper order and manner.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - [CommitTransactionCommandInternal](CommitTransactionCommandInternal.md)
- Called from (representative examples):
  - [finish_xact_command](../f/finish_xact_command.md) (main transaction processing)
  - [InitPostgres](../I/InitPostgres.md) (initialization contexts)
  - [vacuum_rel](../v/vacuum_rel.md) (vacuum operations)
  - [_SPI_commit](../S/_SPI_commit.md) (SPI transaction handling)
  - Various replication workers (logical replication)
  - [ParallelWorkerMain](../P/ParallelWorkerMain.md) (parallel processing)
  - Multiple DDL operations (index creation, table operations)

## Notes and Other Information
- Located in src/backend/access/transam/xact.c:3093-3110
- Prevents stack overflow from recursive subtransaction processing
- Used extensively throughout PostgreSQL for safe transaction commit operations
- The iterative approach ensures all subtransactions are properly handled
- Critical for maintaining transaction integrity in complex nested transaction scenarios
- Called in many contexts including DDL operations, replication, vacuum, and system initialization
- The function's simplicity belies its importance in PostgreSQL's transaction management architecture

## Simplified Source

```c
// Simplified version of CommitTransactionCommand
void CommitTransactionCommand(void) {
    // Repeatedly process transaction commits until all work is done
    // This prevents dangerous recursion in complex subtransaction scenarios
    while (!CommitTransactionCommandInternal()) {
        // Continue processing until internal function indicates completion
    }
}

// Simplified version of CommitTransactionCommandInternal
static bool CommitTransactionCommandInternal(void) {
    TransactionState current_transaction = CurrentTransactionState;
    SavedTransactionCharacteristics saved_characteristics;

    // Save transaction characteristics for potential restoration
    SaveTransactionCharacteristics(&saved_characteristics);

    // Handle different transaction states
    switch (current_transaction->blockState) {

        // Fatal error states - should not occur
        case TBLOCK_DEFAULT:
        case TBLOCK_PARALLEL_INPROGRESS:
            elog(FATAL, "Unexpected transaction state");
            break;

        // Simple transaction commit - not in a transaction block
        case TBLOCK_STARTED:
            CommitTransaction();
            current_transaction->blockState = TBLOCK_DEFAULT;
            break;

        // Begin transaction block - just change state
        case TBLOCK_BEGIN:
            current_transaction->blockState = TBLOCK_INPROGRESS;
            break;

        // Command within transaction block - increment counter
        case TBLOCK_INPROGRESS:
        case TBLOCK_IMPLICIT_INPROGRESS:
        case TBLOCK_SUBINPROGRESS:
            CommandCounterIncrement();
            break;

        // Explicit COMMIT command
        case TBLOCK_END:
            CommitTransaction();
            current_transaction->blockState = TBLOCK_DEFAULT;
            // Handle transaction chaining if requested
            if (current_transaction->chain) {
                StartTransaction();
                current_transaction->blockState = TBLOCK_INPROGRESS;
                current_transaction->chain = false;
                RestoreTransactionCharacteristics(&saved_characteristics);
            }
            break;

        // Transaction is aborted - wait for ROLLBACK
        case TBLOCK_ABORT:
        case TBLOCK_SUBABORT:
            // Do nothing, remain in abort state
            break;

        // ROLLBACK command on aborted transaction
        case TBLOCK_ABORT_END:
            CleanupTransaction();
            current_transaction->blockState = TBLOCK_DEFAULT;
            // Handle chaining
            if (current_transaction->chain) {
                StartTransaction();
                current_transaction->blockState = TBLOCK_INPROGRESS;
                current_transaction->chain = false;
                RestoreTransactionCharacteristics(&saved_characteristics);
            }
            break;

        // ROLLBACK command on good transaction
        case TBLOCK_ABORT_PENDING:
            AbortTransaction();
            CleanupTransaction();
            current_transaction->blockState = TBLOCK_DEFAULT;
            // Handle chaining
            if (current_transaction->chain) {
                StartTransaction();
                current_transaction->blockState = TBLOCK_INPROGRESS;
                current_transaction->chain = false;
                RestoreTransactionCharacteristics(&saved_characteristics);
            }
            break;

        // PREPARE TRANSACTION command
        case TBLOCK_PREPARE:
            PrepareTransaction();
            current_transaction->blockState = TBLOCK_DEFAULT;
            break;

        // SAVEPOINT command - start subtransaction
        case TBLOCK_SUBBEGIN:
            StartSubTransaction();
            current_transaction->blockState = TBLOCK_SUBINPROGRESS;
            break;

        // RELEASE SAVEPOINT command
        case TBLOCK_SUBRELEASE:
            // Commit all pending subtransactions
            do {
                CommitSubTransaction();
                current_transaction = CurrentTransactionState;
            } while (current_transaction->blockState == TBLOCK_SUBRELEASE);
            break;

        // COMMIT from within subtransaction
        case TBLOCK_SUBCOMMIT:
            // Roll up all subtransactions
            do {
                CommitSubTransaction();
                current_transaction = CurrentTransactionState;
            } while (current_transaction->blockState == TBLOCK_SUBCOMMIT);

            // Handle final commit or prepare
            if (current_transaction->blockState == TBLOCK_END) {
                CommitTransaction();
                current_transaction->blockState = TBLOCK_DEFAULT;
                // Handle chaining
                if (current_transaction->chain) {
                    StartTransaction();
                    current_transaction->blockState = TBLOCK_INPROGRESS;
                    current_transaction->chain = false;
                    RestoreTransactionCharacteristics(&saved_characteristics);
                }
            } else if (current_transaction->blockState == TBLOCK_PREPARE) {
                PrepareTransaction();
                current_transaction->blockState = TBLOCK_DEFAULT;
            }
            break;

        // Subtransaction cleanup cases - require iteration
        case TBLOCK_SUBABORT_END:
            CleanupSubTransaction();
            return false; // Need another iteration

        case TBLOCK_SUBABORT_PENDING:
            AbortSubTransaction();
            CleanupSubTransaction();
            return false; // Need another iteration

        // ROLLBACK TO SAVEPOINT cases
        case TBLOCK_SUBRESTART:
        case TBLOCK_SUBABORT_RESTART:
            // Save savepoint information
            char *savepoint_name = current_transaction->name;
            int savepoint_level = current_transaction->savepointLevel;
            current_transaction->name = NULL;

            // Cleanup old subtransaction
            if (current_transaction->blockState == TBLOCK_SUBRESTART) {
                AbortSubTransaction();
            }
            CleanupSubTransaction();

            // Create new subtransaction with same name
            DefineSavepoint(NULL);
            current_transaction = CurrentTransactionState;
            current_transaction->name = savepoint_name;
            current_transaction->savepointLevel = savepoint_level;

            StartSubTransaction();
            current_transaction->blockState = TBLOCK_SUBINPROGRESS;
            break;
    }

    // All work completed for this iteration
    return true;
}
```

Key simplifications made:
- Removed detailed error handling comments for clarity
- Consolidated similar transaction chaining logic
- Simplified variable names for better readability
- Merged similar ROLLBACK TO SAVEPOINT cases
- Focused on the main execution paths
- Added high-level comments explaining the purpose of each state transition
- Abstracted complex assertion checks and detailed state validations