# AtCleanup_Memory

## Location
[src/backend/access/transam/xact.c:1943-1978](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L1943-L1978)

## Overview
AtCleanup_Memory is a static function that performs comprehensive memory context cleanup when a transaction ends, releasing all transaction-local memory and resetting context pointers.

## Definition
static void AtCleanup_Memory(void)

## Detailed Description
This function is responsible for cleaning up memory contexts at the end of transaction processing. It performs several critical memory management operations to ensure proper cleanup and prevent memory leaks when a transaction completes or is aborted.

The function first asserts that we are dealing with a top-level transaction (no parent), then switches the current memory context to TopMemoryContext to ensure subsequent allocations happen in the correct context. It resets the special TransactionAbortContext for reuse in future transactions, and most importantly, deletes the entire TopTransactionContext hierarchy, which contains all memory allocated during the transaction.

Finally, it clears all transaction context pointers (TopTransactionContext, CurTransactionContext, and the current transaction state's context pointer) to prevent dangling references.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - CurrentTransactionState (global variable)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) (memory context function)
  - [MemoryContextReset](../M/MemoryContextReset.md) (memory context function)
  - [MemoryContextDelete](../M/MemoryContextDelete.md) (memory context function)
  - TopMemoryContext (global memory context)
  - TransactionAbortContext (global memory context)
  - TopTransactionContext (global memory context)
  - CurTransactionContext (global memory context)
- Called from (representative examples):
  - [CleanupTransaction](../C/CleanupTransaction.md)
  - [AbortOutOfAnyTransaction](AbortOutOfAnyTransaction.md)

## Notes and Other Information
- This function is static and only used within the transaction management subsystem
- Contains an assertion to ensure it's only called for top-level transactions (not subtransactions)
- Critical for preventing memory leaks by properly releasing all transaction-local memory
- Resets the abort context for reuse rather than deleting it, as it's needed for future transactions
- Part of the transaction cleanup process that runs after commit or abort processing is complete
- The memory context switching ensures that any subsequent allocations don't accidentally use deleted contexts

## Simplified Source

```c
// Simplified version of AtCleanup_Memory
static void AtCleanup_Memory(void) {
    // Ensure we're in a top-level transaction (no parent)
    Assert(CurrentTransactionState->parent == NULL);

    // Switch to top-level memory context for future allocations
    MemoryContextSwitchTo(TopMemoryContext);

    // Reset the abort context for reuse (don't delete it)
    if (TransactionAbortContext != NULL)
        MemoryContextReset(TransactionAbortContext);

    // Delete entire transaction memory hierarchy
    if (TopTransactionContext != NULL)
        MemoryContextDelete(TopTransactionContext);

    // Clear all transaction context pointers to prevent dangling references
    TopTransactionContext = NULL;
    CurTransactionContext = NULL;
    CurrentTransactionState->curTransactionContext = NULL;
}
```

Key simplifications made:
- Added descriptive comments for each major step
- Preserved the original logic flow and structure
- Maintained all essential memory management operations
- Kept the assertion for debugging purposes
- No complex simplifications needed as the original code is already clean and straightforward