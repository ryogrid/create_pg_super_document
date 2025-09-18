# afterTriggerInvokeEvents

## Location
[src/backend/commands/trigger.c:4714-4882](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L4714-L4882)

## Overview
Scans a given event list for trigger events marked to be fired in the current firing cycle and executes them, with support for efficient resource management and optional cleanup of processed events.

## Definition
```c
static bool afterTriggerInvokeEvents(AfterTriggerEventList *events,
                                     CommandId firing_id,
                                     EState *estate,
                                     bool delete_ok)
```

## Detailed Description
This function is the core execution engine for PostgreSQL's deferred trigger mechanism. It iterates through trigger events that have been marked for execution in the current firing cycle and actually invokes them. The function implements sophisticated resource management by reusing relation information when multiple triggers target the same relation, and creates a local EState if one isn't provided.

The function handles cross-partition update events specially, managing separate source and destination partition relations. It creates a per-tuple memory context for trigger function calls to ensure proper memory cleanup between executions. For foreign tables, it creates specialized tuple table slots using minimal tuple operations.

When delete_ok is true, the function optimizes memory usage by clearing fully-processed event chunks, which helps avoid unnecessary rescanning during transaction end when triggers might queue additional events.

## Parameters / Member Variables
- `events`: Pointer to the AfterTriggerEventList containing trigger events to be executed
- `firing_id`: CommandId identifying the current firing cycle - only events marked with this ID will be executed
- `estate`: Optional EState for reusing result relation info (if NULL, a local EState is created)
- `delete_ok`: Boolean indicating whether it's safe to delete fully-processed events for memory optimization

## Dependencies
- Functions called/Symbols referenced:
  - [CreateExecutorState](../C/CreateExecutorState.md)
  - [ExecGetTriggerResultRel](../E/ExecGetTriggerResultRel.md)
  - [MakeSingleTupleTableSlot](../M/MakeSingleTupleTableSlot.md)
  - [ExecDropSingleTupleTableSlot](../E/ExecDropSingleTupleTableSlot.md)
  - [AfterTriggerExecute](../A/AfterTriggerExecute.md)
  - AllocSetContextCreate
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
  - [ExecCloseResultRelations](../E/ExecCloseResultRelations.md)
  - [ExecResetTupleTable](../E/ExecResetTupleTable.md)
  - [FreeExecutorState](../F/FreeExecutorState.md)
  - GetTriggerSharedData
- Called from (representative examples):
  - [AfterTriggerEndQuery](../A/AfterTriggerEndQuery.md)
  - [AfterTriggerFireDeferred](../A/AfterTriggerFireDeferred.md)
  - [AfterTriggerSetState](../A/AfterTriggerSetState.md)

## Notes and Other Information
- Returns true if no unfired events remain in the list, allowing callers to avoid repeating afterTriggerMarkEvents
- Creates a per-tuple memory context named "AfterTriggerTupleContext" for trigger function calls
- Handles foreign tables specially by creating tuple table slots with TTSOpsMinimalTuple operations
- Implements reference counting checks to ensure proper relcache management
- Supports cross-partition updates by managing separate source and destination partition relations
- Optimizes memory usage by clearing processed chunks when delete_ok is true
- Part of PostgreSQL's two-phase trigger execution: marking events (afterTriggerMarkEvents) followed by invoking them (this function)