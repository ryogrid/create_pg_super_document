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