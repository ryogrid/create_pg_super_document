# ExecBSUpdateTriggers

## Location
[src/backend/commands/trigger.c:2906-2963](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L2906-L2963)

## Overview
ExecBSUpdateTriggers executes BEFORE STATEMENT UPDATE triggers, which fire once per UPDATE statement before any rows are modified.

## Definition

```c
void
ExecBSUpdateTriggers(EState *estate, ResultRelInfo *relinfo)
```
## Detailed Description
This function executes BEFORE STATEMENT UPDATE triggers, which are fired once per UPDATE statement before any individual rows are processed. These triggers operate at the statement level rather than the row level, making them suitable for operations that need to occur once per statement regardless of how many rows will be affected.

The function first checks if there are any BEFORE STATEMENT UPDATE triggers defined and whether they haven't already been fired in the current context (to avoid duplicate execution). It retrieves information about which columns are being updated using ExecGetAllUpdatedCols and passes this information to each trigger.

BEFORE STATEMENT triggers are not allowed to return values - if a trigger attempts to do so, an error is raised. These triggers are typically used for logging, security checks, or other operations that should occur once per statement.

## Parameters / Member Variables
- `*estate`: Executor state containing execution context and memory management information
- `*relinfo`: ResultRelInfo containing relation metadata and trigger information (must be the root relation, not a partition)
## Dependencies
- Functions called/Symbols referenced:
  - [before_stmt_triggers_fired](../b/before_stmt_triggers_fired.md)
  - [ExecGetAllUpdatedCols](ExecGetAllUpdatedCols.md)
  - [ExecCallTriggerFunc](ExecCallTriggerFunc.md)
  - [TriggerEnabled](../T/TriggerEnabled.md)
  - GetPerTupleMemoryContext
- Data types referenced:
  - [TriggerDesc](../T/TriggerDesc.md)
  - [TriggerData](../T/TriggerData.md)
  - [Trigger](../T/Trigger.md)
  - [Bitmapset](../B/Bitmapset.md)
  - TRIGGER_EVENT_UPDATE
  - TRIGGER_EVENT_BEFORE
  - TRIGGER_TYPE_STATEMENT
  - TRIGGER_TYPE_BEFORE
  - TRIGGER_TYPE_UPDATE
  - CMD_UPDATE
- Macros used:
  - TRIGGER_TYPE_MATCHES
- Called from (representative examples):
  - [fireBSTriggers](../f/fireBSTriggers.md) (in nodeModifyTable.c)

## Notes and Other Information
- Only executes on the root relation (parent table), not on partitions
- Includes duplicate execution prevention through before_stmt_triggers_fired check
- Raises an error if any trigger attempts to return a non-NULL value
- Passes updated column information to triggers via tg_updatedcols
- Returns void as statement-level triggers cannot modify the operation
- Part of the statement-level trigger execution system
- Located in src/backend/commands/trigger.c:2906-2963
- Operates before any row-level processing begins for UPDATE statements

## Simplified Source

```c
void
ExecBSUpdateTriggers(EState *estate, ResultRelInfo *relinfo) {
    TriggerDesc *trigdesc = relinfo->ri_TrigDesc;
    TriggerData LocTriggerData = {0};
    Bitmapset *updatedCols;

    // Early exits: no triggers or no UPDATE BEFORE STATEMENT triggers
    if (trigdesc == NULL || !trigdesc->trig_update_before_statement)
        return;

    // Skip if already fired in this context (optimization)
    if (before_stmt_triggers_fired(RelationGetRelid(relinfo->ri_RelationDesc), CMD_UPDATE))
        return;

    // Statement-level triggers only operate on parent table
    Assert(relinfo->ri_RootResultRelInfo == NULL);

    // Get information about which columns are being updated
    updatedCols = ExecGetAllUpdatedCols(relinfo, estate);

    // Set up trigger event data
    LocTriggerData.type = T_TriggerData;
    LocTriggerData.tg_event = TRIGGER_EVENT_UPDATE | TRIGGER_EVENT_BEFORE;
    LocTriggerData.tg_relation = relinfo->ri_RelationDesc;
    LocTriggerData.tg_updatedcols = updatedCols;

    // Execute each matching trigger
    for (int i = 0; i < trigdesc->numtriggers; i++) {
        Trigger *trigger = &trigdesc->triggers[i];

        // Skip if not a BEFORE STATEMENT UPDATE trigger
        if (!TRIGGER_TYPE_MATCHES(trigger->tgtype, TRIGGER_TYPE_STATEMENT,
                                  TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_UPDATE))
            continue;

        // Skip if trigger is disabled
        if (!TriggerEnabled(estate, relinfo, trigger, LocTriggerData.tg_event,
                           updatedCols, NULL, NULL))
            continue;

        // Execute the trigger
        LocTriggerData.tg_trigger = trigger;
        HeapTuple newtuple = ExecCallTriggerFunc(&LocTriggerData, i,
                                                relinfo->ri_TrigFunctions,
                                                relinfo->ri_TrigInstrument,
                                                GetPerTupleMemoryContext(estate));

        // BEFORE STATEMENT triggers must not return values
        if (newtuple)
            ereport(ERROR, (errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
                           errmsg("BEFORE STATEMENT trigger cannot return a value")));
    }
}
```