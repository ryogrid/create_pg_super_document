# AtSubAbort_ResourceOwner

## Location
[src/backend/access/transam/xact.c:1898-1910](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L1898-L1910)

## Overview
AtSubAbort_ResourceOwner is a static function that restores the current resource owner to the transaction's main resource owner during subtransaction abort processing.

## Definition
static void AtSubAbort_ResourceOwner(void)

## Detailed Description
This function is part of PostgreSQL's subtransaction abort mechanism. When a subtransaction is being aborted, this function ensures that the CurrentResourceOwner global variable is properly restored to point to the main transaction's resource owner (curTransactionOwner). This is critical for proper resource cleanup and management during the abort process, ensuring that resources are tracked and released correctly.

The function is simple but essential - it retrieves the current transaction state and sets the global CurrentResourceOwner to the transaction's main resource owner, effectively undoing any resource owner changes that may have occurred during the subtransaction.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - CurrentTransactionState (global variable)
  - TransactionState (type)
- Called from (representative examples):
  - [AbortSubTransaction](AbortSubTransaction.md)

## Notes and Other Information
- This function is static and only used within the transaction management subsystem
- It's specifically called during subtransaction abort processing to ensure proper resource owner restoration
- The function assumes that CurrentTransactionState is valid and properly initialized
- Part of a coordinated set of AtSubAbort_* functions that handle different aspects of subtransaction cleanup

## Simplified Source

```c
// Simplified version of AtSubAbort_ResourceOwner
static void AtSubAbort_ResourceOwner(void) {
    // Get current transaction state
    TransactionState s = CurrentTransactionState;

    // Restore resource owner to main transaction's owner
    CurrentResourceOwner = s->curTransactionOwner;
}
```

Key simplifications made:
- Function is already very simple, minimal changes needed
- Added descriptive comments explaining the two main steps
- Preserved the essential logic flow: get transaction state and restore resource owner
- Maintained the static function signature and return type