# GetCurrentTransactionNestLevel

## Location
src/backend/access/transam/xact.c: 926 - 937

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
  - SetReindexProcessing, SetReindexPending, RestoreReindexState
  - EnumValuesCreate, AddEnumLabel
  - RelationCreateStorage, RelationDropStorage
  - smgrDoPendingDeletes, smgrDoPendingSyncs
  - Async_Notify, queue_listen
  - AfterTriggerBeginSubXact, AfterTriggerEndSubXact
  - pgstat_drop_relation, ensure_tabstat_xact_level
  - PrepareInvalidationState, AtEOSubXact_Inval
  - RelationMapUpdateMap
  - CreatePortal, PushActiveSnapshot

## Notes and Other Information
- Essential for managing transaction-aware resources and state across nested transactions
- Used extensively by subsystems that need to track when operations occur relative to transaction boundaries
- The nesting level helps determine appropriate cleanup and rollback behavior for subtransactions
- Critical for implementing proper savepoint semantics and nested transaction handling
- Many PostgreSQL subsystems use this to maintain per-transaction-level state and perform appropriate cleanup during commit/abort operations
- The function provides a simple integer interface to what is internally a complex nested transaction state machine