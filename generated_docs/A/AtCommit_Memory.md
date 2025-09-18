# AtCommit_Memory

## Location
[src/backend/access/transam/xact.c:1577-1603](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L1577-L1603)

## Overview
AtCommit_Memory handles the cleanup of transaction-specific memory contexts during transaction commit, switching back to the top-level memory context and freeing all memory allocated during the transaction.

## Definition
```c
static void AtCommit_Memory(void)
```

## Detailed Description
This static function is responsible for memory management cleanup when a transaction commits. It performs a crucial transition from transaction-local memory management back to global memory management. The function first switches the current memory context to TopMemoryContext to ensure any subsequent allocations go into the persistent top-level context rather than transaction-specific contexts that are about to be destroyed. It then proceeds to delete the TopTransactionContext, which automatically frees all memory allocated within that context and its children during the transaction. Finally, it resets the transaction context pointers to NULL to prevent any accidental access to the freed contexts.

## Parameters / Member Variables
This function takes no parameters but operates on several global memory context variables:
- : The top-level persistent memory context
- : The root context for the current transaction (deleted by this function)
- : Pointer to current transaction context (set to NULL)
- : Transaction state context pointer (set to NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) (implicitly called)
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
- Called from (representative examples):
  - [CommitTransaction](../C/CommitTransaction.md)
  - [PrepareTransaction](../P/PrepareTransaction.md)

## Notes and Other Information
- This is a static function within xact.c, part of the transaction commit cleanup process
- Critical for PostgreSQL's memory management and preventing memory leaks
- The function ensures that all transaction-local memory allocations are properly freed
- Part of PostgreSQL's hierarchical memory context system
- The context switching and cleanup must happen in the correct order to avoid accessing freed memory
- Essential for long-running PostgreSQL sessions to avoid accumulating transaction memory
- Works in conjunction with PostgreSQL's memory context tree structure where transaction contexts are children of the top context