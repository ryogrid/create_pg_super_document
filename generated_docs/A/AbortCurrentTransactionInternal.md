# AbortCurrentTransactionInternal

## Location
[src/backend/access/transam/xact.c:3405-3583](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L3405-L3583)

## Overview
AbortCurrentTransactionInternal is a static function that performs one iteration of transaction abort handling, managing complex transaction state transitions across main transactions and subtransactions.

## Definition

```c
static bool
AbortCurrentTransactionInternal(void)
```
## Detailed Description
This function serves as the core state machine for handling transaction aborts in PostgreSQL. It examines the current transaction's block state and performs appropriate abort actions based on that state. The function is designed to handle both regular transactions and subtransactions, with the ability to require multiple iterations for complex nested subtransaction scenarios.

The function implements a comprehensive switch statement that covers all possible transaction block states, ensuring proper cleanup and state transitions during error recovery. For subtransactions, it may return false to indicate that additional iterations are needed to fully unwind the transaction stack.

## Parameters / Member Variables
- Returns:  - true when no more iterations are required, false when additional iterations are needed (typically for subtransaction cleanup)

## Dependencies
- Functions called/Symbols referenced:
  - [AbortTransaction](AbortTransaction.md)
  - [CleanupTransaction](../C/CleanupTransaction.md)  
  - [AbortSubTransaction](AbortSubTransaction.md)
  - [CleanupSubTransaction](../C/CleanupSubTransaction.md)
  - CurrentTransactionState (global variable)
- Transaction block states referenced:
  - TBLOCK_DEFAULT, TBLOCK_STARTED, TBLOCK_IMPLICIT_INPROGRESS
  - TBLOCK_BEGIN, TBLOCK_INPROGRESS, TBLOCK_PARALLEL_INPROGRESS
  - TBLOCK_END, TBLOCK_ABORT, TBLOCK_SUBABORT
  - TBLOCK_ABORT_END, TBLOCK_ABORT_PENDING, TBLOCK_PREPARE
  - TBLOCK_SUBINPROGRESS, TBLOCK_SUBBEGIN, TBLOCK_SUBRELEASE
  - TBLOCK_SUBCOMMIT, TBLOCK_SUBABORT_PENDING, TBLOCK_SUBRESTART
  - TBLOCK_SUBABORT_END, TBLOCK_SUBABORT_RESTART
- Transaction states referenced:
  - TRANS_DEFAULT, TRANS_START, TRANS_INPROGRESS
- Called from:
  - [AbortCurrentTransaction](AbortCurrentTransaction.md)

## Notes and Other Information
This function is static and internal to xact.c, designed to be called repeatedly by AbortCurrentTransaction until all transaction cleanup is complete. The iterative design is particularly important for handling complex subtransaction hierarchies where multiple cleanup steps may be required. The function carefully manages transaction state transitions to ensure the system reaches a consistent state after error recovery.

## Simplified Source

```c
// Simplified version of AbortCurrentTransactionInternal
static bool AbortCurrentTransactionInternal(void) {
    TransactionState s = CurrentTransactionState;

    switch (s->blockState) {
        // Idle state - handle incomplete transaction starts
        case TBLOCK_DEFAULT:
            if (s->state != TRANS_DEFAULT) {
                // Clean up incomplete transaction
                if (s->state == TRANS_START) {
                    s->state = TRANS_INPROGRESS;
                }
                AbortTransaction();
                CleanupTransaction();
            }
            break;

        // Simple transaction states - abort and return to idle
        case TBLOCK_STARTED:
        case TBLOCK_IMPLICIT_INPROGRESS:
        case TBLOCK_BEGIN:
        case TBLOCK_END:
        case TBLOCK_ABORT_PENDING:
        case TBLOCK_PREPARE:
            AbortTransaction();
            CleanupTransaction();
            s->blockState = TBLOCK_DEFAULT;
            break;

        // In-progress transaction - abort but stay in ABORT state
        case TBLOCK_INPROGRESS:
        case TBLOCK_PARALLEL_INPROGRESS:
            AbortTransaction();
            s->blockState = TBLOCK_ABORT;
            break;

        // Already aborted - just clean up and return to idle
        case TBLOCK_ABORT_END:
            CleanupTransaction();
            s->blockState = TBLOCK_DEFAULT;
            break;

        // Already in abort state - nothing to do
        case TBLOCK_ABORT:
        case TBLOCK_SUBABORT:
            break;

        // Subtransaction in progress - abort subtransaction
        case TBLOCK_SUBINPROGRESS:
            AbortSubTransaction();
            s->blockState = TBLOCK_SUBABORT;
            break;

        // Subtransaction cleanup cases - need more iterations
        case TBLOCK_SUBBEGIN:
        case TBLOCK_SUBRELEASE:
        case TBLOCK_SUBCOMMIT:
        case TBLOCK_SUBABORT_PENDING:
        case TBLOCK_SUBRESTART:
            AbortSubTransaction();
            CleanupSubTransaction();
            return false; // More iterations needed

        case TBLOCK_SUBABORT_END:
        case TBLOCK_SUBABORT_RESTART:
            CleanupSubTransaction();
            return false; // More iterations needed
    }

    return true; // No more iterations required
}
```

Key simplifications made:
- Grouped similar block states together to reduce case redundancy
- Removed detailed comments explaining each state (kept essential logic comments)
- Consolidated cases that perform identical operations
- Focused on the main execution paths and state transitions
- Preserved the critical return value logic for iteration control
- Maintained the essential abort/cleanup function calls