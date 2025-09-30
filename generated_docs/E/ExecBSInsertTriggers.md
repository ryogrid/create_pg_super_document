# ExecBSInsertTriggers

## Location
[src/backend/commands/trigger.c:2396-2446](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L2396-L2446)

## Overview
Executes all enabled BEFORE STATEMENT INSERT triggers for a given relation, ensuring they fire only once per statement and enforcing trigger protocol rules.

## Definition
```c
void ExecBSInsertTriggers(EState *estate, ResultRelInfo *relinfo)
```

## Detailed Description
This function manages the execution of BEFORE STATEMENT INSERT triggers, which fire once per INSERT statement before any rows are processed. It implements several important safeguards and optimizations:

1. **Duplicate Prevention**: Uses before_stmt_triggers_fired() to ensure triggers don't fire multiple times for the same statement context
2. **Trigger Filtering**: Only executes triggers that match the exact criteria (BEFORE + STATEMENT + INSERT)
3. **Enable Checking**: Respects trigger enable/disable states and trigger conditions
4. **Protocol Enforcement**: Validates that BEFORE STATEMENT triggers don't return values (which would violate the trigger protocol)

The function iterates through all triggers defined on the relation, filtering for the appropriate type, checking if they're enabled, and executing them via ExecCallTriggerFunc. Any attempt by a BEFORE STATEMENT trigger to return a tuple results in an error.

## Parameters / Member Variables
- `estate`: Executor state containing execution context and memory management information
- `relinfo`: Result relation info containing trigger descriptors, function cache, and relation metadata

## Dependencies
- Functions called/Symbols referenced:
  - [before_stmt_triggers_fired](../b/before_stmt_triggers_fired.md) (duplicate execution prevention)
  - TRIGGER_TYPE_MATCHES (trigger type filtering macro)
  - [TriggerEnabled](../T/TriggerEnabled.md) (trigger enable state checking)
  - [ExecCallTriggerFunc](ExecCallTriggerFunc.md) (actual trigger execution)
  - GetPerTupleMemoryContext (memory context management)
- Data structures used:
  - [TriggerDesc](../T/TriggerDesc.md) (trigger descriptor from relinfo)
  - [TriggerData](../T/TriggerData.md) (trigger execution context)
  - [Trigger](../T/Trigger.md) (individual trigger structure)
- Called from (representative examples):
  - [CopyFrom](../C/CopyFrom.md) (during COPY FROM operations)
  - [fireBSTriggers](../f/fireBSTriggers.md) (from nodeModifyTable executor)

## Notes and Other Information
- BEFORE STATEMENT triggers fire exactly once per SQL statement, regardless of how many rows are affected
- These triggers cannot access individual row data since they execute before any rows are processed
- The function enforces the trigger protocol by rejecting any non-NULL return values
- [Trigger](../T/Trigger.md) execution uses per-tuple memory context for proper cleanup
- The function short-circuits if no triggers exist or if no INSERT BEFORE STATEMENT triggers are defined
- Used in both regular INSERT operations and bulk operations like COPY FROM
- Part of PostgreSQL's comprehensive trigger system that supports multiple timing and granularity combinations

## Simplified Source

```c
void
ExecBSInsertTriggers(EState *estate, ResultRelInfo *relinfo)
{
    TriggerDesc *trigdesc = relinfo->ri_TrigDesc;
    TriggerData LocTriggerData = {0};

    // Exit early if no triggers or already fired in this context
    if (trigdesc == NULL || !trigdesc->trig_insert_before_statement)
        return;
    if (before_stmt_triggers_fired(RelationGetRelid(relinfo->ri_RelationDesc), CMD_INSERT))
        return;

    // Set up trigger context
    LocTriggerData.type = T_TriggerData;
    LocTriggerData.tg_event = TRIGGER_EVENT_INSERT | TRIGGER_EVENT_BEFORE;
    LocTriggerData.tg_relation = relinfo->ri_RelationDesc;

    // Execute each BEFORE STATEMENT INSERT trigger
    for (int i = 0; i < trigdesc->numtriggers; i++)
    {
        Trigger *trigger = &trigdesc->triggers[i];

        // Skip if not a BEFORE STATEMENT INSERT trigger or if disabled
        if (!TRIGGER_TYPE_MATCHES(trigger->tgtype, TRIGGER_TYPE_STATEMENT,
                                 TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_INSERT))
            continue;
        if (!TriggerEnabled(estate, relinfo, trigger, LocTriggerData.tg_event,
                           NULL, NULL, NULL))
            continue;

        // Execute trigger function
        LocTriggerData.tg_trigger = trigger;
        HeapTuple newtuple = ExecCallTriggerFunc(&LocTriggerData, i,
                                               relinfo->ri_TrigFunctions,
                                               relinfo->ri_TrigInstrument,
                                               GetPerTupleMemoryContext(estate));

        // BEFORE STATEMENT triggers must not return values
        if (newtuple)
            ereport(ERROR, "BEFORE STATEMENT trigger cannot return a value");
    }
}
```