# AtSubStart_Memory

## Location
[src/backend/access/transam/xact.c:1248-1271](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L1248-L1271)

## Overview
AtSubStart_Memory is a static function that sets up memory context management for a new subtransaction by creating a dedicated CurTransactionContext as a child of the parent transaction's context.

## Definition
```c
static void AtSubStart_Memory(void)
```

## Detailed Description
AtSubStart_Memory handles memory context initialization for subtransactions, which are nested transactions within PostgreSQL. The function creates a new CurTransactionContext that serves as the memory allocation context for the subtransaction.

The key characteristic of this memory context is its lifecycle behavior:
- Data allocated in this context survives subtransaction commit (becomes part of parent transaction)
- Data allocated in this context is automatically cleaned up on subtransaction abort

The new context is created as a child of the parent's CurTransactionContext, establishing a hierarchical memory management structure that mirrors the transaction nesting. This enables proper memory cleanup when subtransactions abort while preserving memory that should survive commits.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - TransactionState (at line 1250)
  - AllocSetContextCreate (at line 1259)
  - ALLOCSET_DEFAULT_SIZES (at line 1261)
  - CurrentTransactionState (global variable)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) (implicit)
- Called from (representative examples):
  - [StartSubTransaction](../S/StartSubTransaction.md) (src/backend/access/transam/xact.c:5026)

## Notes and Other Information
- This is a static function, only accessible within xact.c
- Part of the subtransaction infrastructure that enables savepoints and nested transactions
- The parent-child relationship between memory contexts mirrors the transaction hierarchy
- Memory allocated in the subtransaction context is automatically managed based on subtransaction outcome
- Essential for PostgreSQL's savepoint and exception handling mechanisms
- The function switches to the new context as the active memory context for subsequent allocations