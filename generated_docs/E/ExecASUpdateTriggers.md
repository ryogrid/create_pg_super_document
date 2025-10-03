# ExecASUpdateTriggers

## Location
[src/backend/commands/trigger.c:2964-2981](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L2964-L2981)

## Overview
ExecASUpdateTriggers schedules AFTER STATEMENT UPDATE triggers for execution after all rows in an UPDATE statement have been processed.

## Definition

```c
void
ExecASUpdateTriggers(EState *estate, ResultRelInfo *relinfo,
					 TransitionCaptureState *transition_capture)
```
## Detailed Description
This function handles the scheduling of AFTER STATEMENT UPDATE triggers, which fire once per UPDATE statement after all individual rows have been processed. Unlike BEFORE STATEMENT triggers that execute immediately, AFTER STATEMENT triggers are deferred and executed through the AfterTriggerSaveEvent mechanism to ensure proper timing and ordering.

The function is simple but crucial - it checks if there are any AFTER STATEMENT UPDATE triggers defined on the relation and, if so, saves an event to the after-trigger system. The actual trigger execution happens later through the deferred trigger execution system, which ensures that all statement-level effects are completed before the triggers run.

The function also passes information about updated columns and transition capture state, allowing triggers to access comprehensive information about the UPDATE operation.

## Parameters / Member Variables
- : Executor state containing execution context and memory management information
- : ResultRelInfo containing relation metadata and trigger information (must be the root relation, not a partition)  
- : State for capturing tuples into OLD and NEW transition tables for trigger access

## Dependencies
- Functions called/Symbols referenced:
  - [AfterTriggerSaveEvent](../A/AfterTriggerSaveEvent.md)
  - [ExecGetAllUpdatedCols](ExecGetAllUpdatedCols.md)
- Data types referenced:
  - [TransitionCaptureState](../T/TransitionCaptureState.md)
  - [TriggerDesc](../T/TriggerDesc.md)
  - TRIGGER_EVENT_UPDATE
- Called from (representative examples):
  - [fireASTriggers](../f/fireASTriggers.md) (in nodeModifyTable.c)

## Notes and Other Information
- Only operates on the root relation (parent table), not on partitions
- Does not execute triggers immediately - schedules them for deferred execution
- Part of the PostgreSQL deferred trigger execution system
- Passes updated column information and transition capture state to the trigger system
- Returns void as the actual trigger execution is deferred
- Located in src/backend/commands/trigger.c:2964-2981
- Complements ExecBSUpdateTriggers for complete statement-level trigger support
- Essential for maintaining proper trigger execution order in complex UPDATE operations

## Simplified Source

```c
void ExecASUpdateTriggers(EState *estate, ResultRelInfo *relinfo,
                         TransitionCaptureState *transition_capture)
{
    TriggerDesc *trigdesc = relinfo->ri_TrigDesc;

    // Ensure we're operating on the root relation (not a partition)
    Assert(relinfo->ri_RootResultRelInfo == NULL);

    // Schedule AFTER STATEMENT UPDATE triggers for deferred execution
    if (trigdesc && trigdesc->trig_update_after_statement)
        AfterTriggerSaveEvent(estate, relinfo, NULL, NULL,
                             TRIGGER_EVENT_UPDATE,
                             false, NULL, NULL, NIL,
                             ExecGetAllUpdatedCols(relinfo, estate),
                             transition_capture,
                             false);
}
```