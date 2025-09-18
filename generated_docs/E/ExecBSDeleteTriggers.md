# ExecBSDeleteTriggers

## Location
[src/backend/commands/trigger.c:2623-2673](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L2623-L2673)

## Overview
Executes BEFORE STATEMENT DELETE triggers for a relation, providing a hook for statement-level validation or logging before any rows are deleted.

## Definition
```c
void ExecBSDeleteTriggers(EState *estate, ResultRelInfo *relinfo)
```

## Detailed Description
ExecBSDeleteTriggers executes all BEFORE STATEMENT DELETE triggers associated with a relation. These triggers fire once per DELETE statement, before any individual rows are processed. The function includes an optimization to prevent duplicate execution of statement triggers within the same command context using before_stmt_triggers_fired().

BEFORE STATEMENT triggers cannot return values (unlike row-level triggers), and the function enforces this by raising an error if any trigger attempts to return a tuple. These triggers are commonly used for logging, auditing, or performing statement-level validation that doesn't depend on specific row data.

The function processes triggers in the order they were defined and calls each enabled trigger through the standard trigger execution mechanism.

## Parameters / Member Variables
- `estate`: Execution state containing transaction and query context information
- `relinfo`: Information about the target relation including trigger descriptors, cached trigger functions, and relation metadata

## Dependencies
- Functions called/Symbols referenced:
  - before_stmt_triggers_fired
  - [ExecCallTriggerFunc](ExecCallTriggerFunc.md)
  - [TriggerEnabled](../T/TriggerEnabled.md)
  - GetPerTupleMemoryContext
  - TRIGGER_TYPE_MATCHES
  - RelationGetRelid
- Constants used:
  - TRIGGER_EVENT_DELETE
  - TRIGGER_EVENT_BEFORE
  - TRIGGER_TYPE_STATEMENT
  - TRIGGER_TYPE_BEFORE
  - TRIGGER_TYPE_DELETE
  - CMD_DELETE
- Data structures used:
  - TriggerDesc
  - TriggerData
  - Trigger
- Called from (representative examples):
  - [fireBSTriggers](../f/fireBSTriggers.md)

## Notes and Other Information
- Executes only once per DELETE statement, not per row
- Includes optimization to prevent duplicate execution within the same command context
- BEFORE STATEMENT triggers must not return any value - function enforces this with an error
- Returns void as statement-level triggers cannot modify the operation
- Triggers execute in definition order before any rows are processed
- Used primarily for auditing, logging, and statement-level validation
- No tuple data is available to these triggers since they execute before row processing begins