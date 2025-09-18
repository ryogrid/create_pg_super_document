# ExecASInsertTriggers

## Location
[src/backend/commands/trigger.c:2447-2459](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L2447-L2459)

## Overview
Schedules AFTER STATEMENT INSERT triggers for deferred execution, capturing transition table data if required for trigger functions that reference OLD TABLE or NEW TABLE.

## Definition
```c
void ExecASInsertTriggers(EState *estate, ResultRelInfo *relinfo,
                          TransitionCaptureState *transition_capture)
```

## Detailed Description
This function handles AFTER STATEMENT INSERT triggers by scheduling them for deferred execution rather than running them immediately. Unlike BEFORE triggers that execute synchronously, AFTER STATEMENT triggers are queued and executed at the end of the statement or transaction (depending on their deferrable setting).

The function performs a simple but crucial role in PostgreSQL's trigger system:
1. Checks if the relation has any AFTER STATEMENT INSERT triggers defined
2. If triggers exist, calls AfterTriggerSaveEvent to queue the trigger event
3. Passes transition capture state to support triggers that use OLD TABLE/NEW TABLE references

The actual trigger execution is handled later by the after-trigger subsystem, which manages proper ordering, constraint checking, and transaction boundaries.

## Parameters / Member Variables
- `estate`: Executor state containing execution context and transaction information
- `relinfo`: Result relation info containing trigger descriptors and relation metadata
- `transition_capture`: State for capturing row data for transition tables (OLD TABLE/NEW TABLE references in trigger functions)

## Dependencies
- Functions called/Symbols referenced:
  - AfterTriggerSaveEvent (queues the trigger event for deferred execution)
- Data structures used:
  - TriggerDesc (trigger descriptor from relinfo)
  - TransitionCaptureState (for transition table support)
- Constants used:
  - TRIGGER_EVENT_INSERT (event type identifier)
- Called from (representative examples):
  - [CopyFrom](../C/CopyFrom.md) (during COPY FROM operations)
  - [fireASTriggers](../f/fireASTriggers.md) (from nodeModifyTable executor)

## Notes and Other Information
- AFTER STATEMENT triggers fire once per SQL statement, after all rows have been processed
- Unlike BEFORE triggers, these are not executed immediately but queued for later execution
- The deferred execution allows for proper constraint checking and maintains transaction semantics
- Transition capture enables triggers to access OLD TABLE and NEW TABLE references
- The function is very lightweight since the heavy lifting is done by AfterTriggerSaveEvent
- Part of PostgreSQL's sophisticated trigger timing system that supports immediate and deferred execution
- Essential for maintaining data integrity in complex trigger scenarios involving multiple tables or constraints