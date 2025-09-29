# AtSubCommit_Memory

## Location
[src/backend/access/transam/xact.c:1604-1632](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L1604-L1632)

## Overview
AtSubCommit_Memory manages memory context cleanup during subtransaction commit, switching back to the parent transaction's memory context and optionally freeing the subtransaction's context if it's empty.

## Definition
```c
static void AtSubCommit_Memory(void)
```

## Detailed Description
This static function handles memory management during subtransaction commit within PostgreSQL's nested transaction system. When a subtransaction commits, its changes become part of the parent transaction, but the memory management needs careful handling. The function switches the current memory context back to the parent transaction's context, ensuring future allocations go to the correct location. It then performs an optimization check: if the subtransaction's memory context is empty (contains no allocated memory), it deletes the context to avoid memory leaks. However, if the context contains data, it's preserved because that data will be needed when the parent transaction eventually commits. This approach balances memory efficiency with the need to maintain subtransaction data for the parent transaction.

## Parameters / Member Variables
This function takes no parameters but works with transaction state and memory context variables:
- : Current transaction state (TransactionState)
- : Parent transaction state
- : Current transaction context (updated to parent's context)
- : The subtransaction's memory context (potentially deleted)

## Dependencies
- Functions called/Symbols referenced:
  - TransactionState (type)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) (implicitly called)
  - [MemoryContextIsEmpty](../M/MemoryContextIsEmpty.md)
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
- Called from (representative examples):
  - [CommitSubTransaction](../C/CommitSubTransaction.md)

## Notes and Other Information
- This is a static function within xact.c, part of the subtransaction commit process
- Critical for PostgreSQL's nested transaction memory management
- Implements an important optimization by freeing empty subtransaction contexts
- The function preserves non-empty contexts because their data may be needed by the parent transaction
- Part of PostgreSQL's hierarchical memory context system for managing nested transactions
- Helps prevent memory leaks in scenarios with many trivial subtransactions (e.g., PL/pgSQL exception blocks)
- Works in conjunction with the overall subtransaction commit mechanism
- Essential for maintaining proper memory context hierarchy during nested transaction operations

## Simplified Source

```c
static void AtSubCommit_Memory(void)
{
    TransactionState s = CurrentTransactionState;

    Assert(s->parent != NULL);

    // Switch back to parent transaction's memory context
    CurTransactionContext = s->parent->curTransactionContext;
    MemoryContextSwitchTo(CurTransactionContext);

    // Optimization: if subtransaction context is empty, delete it
    // to avoid memory leak in trivial subtransactions
    if (MemoryContextIsEmpty(s->curTransactionContext))
    {
        MemoryContextDelete(s->curTransactionContext);
        s->curTransactionContext = NULL;
    }
    // Note: Non-empty contexts are preserved as their data
    // will be needed when parent transaction commits
}
```