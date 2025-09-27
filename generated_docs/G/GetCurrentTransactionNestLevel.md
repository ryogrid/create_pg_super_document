# GetCurrentTransactionNestLevel

## Location
[src/backend/access/transam/xact.c:926-937](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L926-L937)

## Overview
Returns the nesting level of the current transaction, indicating how deeply nested within savepoints or subtransactions the current context is.

## Definition
int GetCurrentTransactionNestLevel(void)

## Detailed Description
This function returns the nesting level of the current transaction context by accessing the nestingLevel field of the current transaction state. The nesting level provides important information about the transaction hierarchy:

- Level 0: Not inside any transaction
- Level 1: Inside a top-level transaction
- Level 2+: Inside nested subtransactions/savepoints

This information is crucial for various PostgreSQL subsystems that need to track transaction boundaries and handle nested transaction scenarios appropriately, such as resource management, invalidation tracking, and state cleanup.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - CurrentTransactionState (global variable)
  - TransactionState (type)
- Called from (representative examples):
  - [SetReindexProcessing](../S/SetReindexProcessing.md), SetReindexPending, RestoreReindexState
  - [EnumValuesCreate](../E/EnumValuesCreate.md), AddEnumLabel
  - [RelationCreateStorage](../R/RelationCreateStorage.md), RelationDropStorage
  - [smgrDoPendingDeletes](../s/smgrDoPendingDeletes.md), smgrDoPendingSyncs
  - [Async_Notify](../A/Async_Notify.md), queue_listen
  - [AfterTriggerBeginSubXact](../A/AfterTriggerBeginSubXact.md), AfterTriggerEndSubXact
  - [pgstat_drop_relation](../p/pgstat_drop_relation.md), ensure_tabstat_xact_level
  - [PrepareInvalidationState](../P/PrepareInvalidationState.md), AtEOSubXact_Inval
  - [RelationMapUpdateMap](../R/RelationMapUpdateMap.md)
  - [CreatePortal](../C/CreatePortal.md), PushActiveSnapshot

## Notes and Other Information
- Essential for managing transaction-aware resources and state across nested transactions
- Used extensively by subsystems that need to track when operations occur relative to transaction boundaries
- The nesting level helps determine appropriate cleanup and rollback behavior for subtransactions
- Critical for implementing proper savepoint semantics and nested transaction handling
- Many PostgreSQL subsystems use this to maintain per-transaction-level state and perform appropriate cleanup during commit/abort operations
- The function provides a simple integer interface to what is internally a complex nested transaction state machine

## Simplified Source

```c
// Simplified version of GetCurrentTransactionNestLevel
int GetCurrentTransactionNestLevel(void) {
    // Get the current transaction state from global context
    TransactionState s = CurrentTransactionState;

    // Return the nesting level directly
    // 0 = no transaction, 1 = top-level, 2+ = nested subtransactions
    return s->nestingLevel;
}
```

Key simplifications made:
- Added clarifying comments explaining the nesting level values
- Maintained the original simple logic (function was already quite straightforward)
- Emphasized the direct access pattern to global transaction state