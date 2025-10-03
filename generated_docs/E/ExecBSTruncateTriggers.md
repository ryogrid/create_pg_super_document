# ExecBSTruncateTriggers

## Location
[src/backend/commands/trigger.c:3307-3353](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L3307-L3353)

## Overview
Executes BEFORE STATEMENT TRUNCATE triggers that run before a TRUNCATE operation begins, allowing validation or side effects but prohibiting data return.

## Definition

```c
void
ExecBSTruncateTriggers(EState *estate, ResultRelInfo *relinfo)
```
## Detailed Description
This function executes BEFORE STATEMENT TRUNCATE triggers, which fire once per TRUNCATE statement before any rows are actually removed from the table. These triggers operate at the statement level rather than per-row, making them suitable for validation, logging, or other preparatory actions. The function validates that triggers do not attempt to return values (which is prohibited for statement-level triggers) and will raise an error if any trigger violates this protocol.

## Parameters / Member Variables
- `*estate`: Executor state containing execution context and memory management
- `*relinfo`: Relation information including trigger descriptors and table metadata
## Dependencies
- Functions called/Symbols referenced:
  - [TriggerEnabled](../T/TriggerEnabled.md)
  - [ExecCallTriggerFunc](ExecCallTriggerFunc.md)
  - GetPerTupleMemoryContext
  - TRIGGER_TYPE_MATCHES
- Called from (representative examples):
  - [ExecuteTruncateGuts](ExecuteTruncateGuts.md)

## Notes and Other Information
- Returns immediately if no truncate triggers are defined for the relation
- Only processes triggers matching STATEMENT + BEFORE + TRUNCATE type combination
- Raises an error if any trigger attempts to return a non-NULL tuple value
- Executes triggers synchronously before the actual truncate operation begins
- Does not pass any tuple data since TRUNCATE operates at statement level
- Uses NULL values for old/new slots in TriggerEnabled since no tuples are involved
- Part of the TRUNCATE command execution pipeline in tablecmds.c

## Simplified Source

```c
void ExecBSTruncateTriggers(EState *estate, ResultRelInfo *relinfo)
{
    TriggerDesc *trigdesc = relinfo->ri_TrigDesc;

    // Early exit if no triggers or no BEFORE STATEMENT TRUNCATE triggers
    if (trigdesc == NULL || !trigdesc->trig_truncate_before_statement)
        return;

    // Set up trigger event data
    TriggerData LocTriggerData = {0};
    LocTriggerData.type = T_TriggerData;
    LocTriggerData.tg_event = TRIGGER_EVENT_TRUNCATE | TRIGGER_EVENT_BEFORE;
    LocTriggerData.tg_relation = relinfo->ri_RelationDesc;

    // Execute each matching trigger
    for (int i = 0; i < trigdesc->numtriggers; i++)
    {
        Trigger *trigger = &trigdesc->triggers[i];

        // Skip triggers that don't match BEFORE STATEMENT TRUNCATE
        if (!TRIGGER_TYPE_MATCHES(trigger->tgtype,
                                  TRIGGER_TYPE_STATEMENT,
                                  TRIGGER_TYPE_BEFORE,
                                  TRIGGER_TYPE_TRUNCATE))
            continue;

        // Skip disabled triggers
        if (!TriggerEnabled(estate, relinfo, trigger, LocTriggerData.tg_event,
                            NULL, NULL, NULL))
            continue;

        // Execute the trigger function
        LocTriggerData.tg_trigger = trigger;
        HeapTuple newtuple = ExecCallTriggerFunc(&LocTriggerData, i,
                                                 relinfo->ri_TrigFunctions,
                                                 relinfo->ri_TrigInstrument,
                                                 GetPerTupleMemoryContext(estate));

        // BEFORE STATEMENT triggers cannot return values
        if (newtuple)
            ereport(ERROR,
                    (errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
                     errmsg("BEFORE STATEMENT trigger cannot return a value")));
    }
}
```