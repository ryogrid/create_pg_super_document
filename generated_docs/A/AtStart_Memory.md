# AtStart_Memory

## Location
src/backend/access/transam/xact.c: 1173 - 1219

## Overview
AtStart_Memory is a static function responsible for setting up memory contexts at the start of a new transaction, creating both the transaction abort context and the main transaction context.

## Definition
```c
static void AtStart_Memory(void)
```

## Detailed Description
AtStart_Memory handles the critical task of establishing the memory management infrastructure for a new transaction. The function performs two main memory context setups:

1. **TransactionAbortContext**: Creates a private memory context reserved for abort operations. This context is created with a fixed size (32KB) and slow growth rate to ensure that abort operations can proceed even in out-of-memory situations, similar to ErrorContext.

2. **TopTransactionContext**: Creates the main memory context for the transaction using default allocation settings. In a top-level transaction, this also serves as the CurTransactionContext.

The function ensures proper memory isolation for transaction operations and provides a fail-safe mechanism for transaction cleanup through the dedicated abort context.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - TransactionState (at line 1175)
  - AllocSetContextCreate (at lines 1186, 1201)
  - ALLOCSET_DEFAULT_SIZES (at line 1203)
  - CurrentTransactionState (global variable)
  - TopMemoryContext (global variable)
  - MemoryContextSwitchTo (implicit)
- Called from (representative examples):
  - StartTransaction (src/backend/access/transam/xact.c:2104)

## Notes and Other Information
- This is a static function, only accessible within xact.c
- The TransactionAbortContext is created only once and reused across transactions
- The 32KB fixed size for TransactionAbortContext provides insurance against memory exhaustion during abort operations
- TopTransactionContext and CurTransactionContext are the same in top-level transactions
- The function switches to CurTransactionContext as the active memory context
- Critical for memory management and cleanup in PostgreSQL's transaction system