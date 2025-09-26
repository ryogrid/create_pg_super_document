# ExecASDeleteTriggers

## Location
[src/backend/commands/trigger.c:2674-2693](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L2674-L2693)

## Overview
Schedules AFTER STATEMENT DELETE triggers for deferred execution and handles transition table capture for statement-level delete operations.

## Definition
```c
void ExecASDeleteTriggers(EState *estate, ResultRelInfo *relinfo,
                         TransitionCaptureState *transition_capture)
```

## Detailed Description
ExecASDeleteTriggers is responsible for scheduling AFTER STATEMENT DELETE triggers for deferred execution at the end of the statement or transaction. Unlike BEFORE STATEMENT triggers that execute immediately, AFTER STATEMENT triggers are queued using PostgreSQL's deferred trigger mechanism to ensure they execute after all row-level operations have completed.

The function is relatively simple, checking if the relation has any AFTER STATEMENT DELETE triggers defined and, if so, saving the trigger event through AfterTriggerSaveEvent. This event will be processed later during statement completion, allowing the triggers to see the final state after all deletions have occurred.

The function also supports transition table capture, enabling triggers to access OLD TABLE references that contain all deleted rows.

## Parameters / Member Variables
- `estate`: Execution state containing transaction and query context information
- `relinfo`: Information about the target relation including trigger descriptors
- `transition_capture`: State information for capturing deleted tuples into transition tables for trigger OLD TABLE references

## Dependencies
- Functions called/Symbols referenced:
  - [AfterTriggerSaveEvent](../A/AfterTriggerSaveEvent.md)
  - TRIGGER_EVENT_DELETE
- Data structures used:
  - [TransitionCaptureState](../T/TransitionCaptureState.md)
  - [TriggerDesc](../T/TriggerDesc.md)
- Called from (representative examples):
  - [fireASTriggers](../f/fireASTriggers.md)

## Notes and Other Information
- Part of PostgreSQL's deferred trigger execution system
- AFTER STATEMENT triggers execute after all row-level delete operations complete
- Triggers are scheduled for execution, not executed immediately
- Supports transition table capture for OLD TABLE references in trigger functions
- Statement-level triggers execute once per DELETE statement regardless of how many rows are affected
- The actual trigger execution happens during statement/transaction completion phase
- NIL is passed for recheckIndexes since statement-level triggers don't affect individual rows