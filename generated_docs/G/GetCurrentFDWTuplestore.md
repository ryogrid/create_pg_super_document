# GetCurrentFDWTuplestore

## Location
src/backend/commands/trigger.c: 4005 - 4040

## Overview
Retrieves or creates the FDW (Foreign Data Wrapper) tuplestore for the current trigger query level, used to manage tuple data during trigger execution.

## Definition
```c
static Tuplestorestate *GetCurrentFDWTuplestore(void)
```

## Detailed Description
This function manages the FDW tuplestore associated with the current query depth in PostgreSQL's after-trigger execution framework. It implements lazy initialization, creating the tuplestore only when first needed. The tuplestore is used to temporarily store tuple data during foreign data wrapper operations within trigger execution contexts.

The function ensures proper memory management by creating the tuplestore in the current transaction context and setting appropriate resource ownership. This guarantees that the tuplestore remains valid for the duration of the subtransaction and is properly cleaned up when the transaction ends.

The tuplestore is stored in the global afterTriggers structure, indexed by the current query depth, allowing for nested trigger executions to maintain separate tuple stores.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - tuplestore_begin_heap
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - CurTransactionContext
  - CurrentResourceOwner
  - CurTransactionResourceOwner
  - work_mem
  - afterTriggers (global structure)
- Called from (representative examples):
  - [AfterTriggerExecute](../A/AfterTriggerExecute.md)
  - AfterTriggerSaveEvent

## Notes and Other Information
- The function is static and used internally within the trigger system
- Implements lazy initialization pattern - tuplestore is created only when first accessed
- Uses transaction-level memory context to ensure proper lifetime management
- The tuplestore is configured for heap storage with specific work_mem limits
- Part of PostgreSQL's after-trigger execution infrastructure
- Supports nested query execution by maintaining separate tuplestores per query depth
- The tuplestore lifetime is tied to the subtransaction, ensuring automatic cleanup
- Specifically designed for FDW operations within trigger contexts