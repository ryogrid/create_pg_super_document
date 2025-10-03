# EventTriggerInvoke

## Location
[src/backend/commands/event_trigger.c:1069-1133](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/event_trigger.c#L1069-L1133)

## Overview
EventTriggerInvoke is a static function that executes a list of event trigger functions in sequence, managing memory context and ensuring proper isolation between trigger executions.

## Definition

```c
static void
EventTriggerInvoke(List *fn_oid_list, EventTriggerData *trigdata)
```
## Detailed Description
EventTriggerInvoke iterates through a list of event trigger function OIDs and executes each one in sequence. The function provides several important guarantees:

1. **Memory Management**: Creates a dedicated memory context for event trigger execution to prevent memory leaks and ensure cleanup after each trigger.
2. **Stack Protection**: Guards against stack overflow from recursive event trigger calls using check_stack_depth().
3. **Command Visibility**: Ensures each event trigger can see the results of previous triggers by calling CommandCounterIncrement() between executions (except for the first trigger).
4. **Statistics Tracking**: Collects function call statistics for each triggered function.

The function operates in a temporary memory context that is reset after each trigger execution and deleted when all triggers complete.

## Parameters / Member Variables
- `*fn_oid_list`: List of function OIDs representing the event triggers to be invoked
- `*trigdata`: EventTriggerData structure containing context information passed to each trigger function
## Dependencies
- Functions called/Symbols referenced:
  - [check_stack_depth](../c/check_stack_depth.md) (stack overflow protection)
  - AllocSetContextCreate (memory context creation)
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md) (command visibility)
  - [fmgr_info](../f/fmgr_info.md) (function manager lookup)
  - InitFunctionCallInfoData (function call setup)
  - FunctionCallInvoke (actual function execution)
  - [pgstat_init_function_usage](../p/pgstat_init_function_usage.md)/pgstat_end_function_usage (statistics)
  - [MemoryContextReset](../M/MemoryContextReset.md)/MemoryContextDelete (memory management)
- Called from:
  - [EventTriggerDDLCommandStart](EventTriggerDDLCommandStart.md)
  - [EventTriggerDDLCommandEnd](EventTriggerDDLCommandEnd.md)
  - [EventTriggerSQLDrop](EventTriggerSQLDrop.md)
  - [EventTriggerOnLogin](EventTriggerOnLogin.md)
  - [EventTriggerTableRewrite](EventTriggerTableRewrite.md)

## Notes and Other Information
- This is a static function internal to the event trigger system
- Uses DEBUG1 logging to trace function OID execution
- Memory context isolation prevents interference between different trigger executions
- The first trigger doesn't require CommandCounterIncrement() as there are no previous changes to see
- Function call statistics are properly tracked for performance monitoring

## Simplified Source

```c
// Simplified version of EventTriggerInvoke
static void
EventTriggerInvoke(List *fn_oid_list, EventTriggerData *trigdata)
{
    MemoryContext event_context;
    MemoryContext old_context;
    ListCell *cell;
    bool first_trigger = true;

    // Prevent infinite recursion from nested event triggers
    check_stack_depth();

    // Create isolated memory context for trigger execution
    event_context = AllocSetContextCreate(CurrentMemoryContext,
                                        "event trigger context",
                                        ALLOCSET_DEFAULT_SIZES);
    old_context = MemoryContextSwitchTo(event_context);

    // Execute each event trigger function in sequence
    foreach(cell, fn_oid_list)
    {
        Oid trigger_function_oid = lfirst_oid(cell);
        LOCAL_FCINFO(fcinfo, 0);
        FmgrInfo function_info;
        PgStat_FunctionCallUsage stats;

        // Log trigger execution for debugging
        elog(DEBUG1, "EventTriggerInvoke %u", trigger_function_oid);

        // Allow each trigger to see previous trigger results
        if (first_trigger) {
            first_trigger = false;
        } else {
            CommandCounterIncrement();
        }

        // Setup and invoke the trigger function
        fmgr_info(trigger_function_oid, &function_info);
        InitFunctionCallInfoData(*fcinfo, &function_info, 0,
                               InvalidOid, (Node *) trigdata, NULL);

        // Track function usage statistics
        pgstat_init_function_usage(fcinfo, &stats);
        FunctionCallInvoke(fcinfo);
        pgstat_end_function_usage(&stats, true);

        // Clean up memory after each trigger
        MemoryContextReset(event_context);
    }

    // Restore original memory context and cleanup
    MemoryContextSwitchTo(old_context);
    MemoryContextDelete(event_context);
}
```

Key simplifications made:
- Used more descriptive variable names (event_context, first_trigger, etc.)
- Added explanatory comments for each major operation
- Consolidated function call setup into logical groups
- Removed complex macro formatting for better readability
- Focused on the main execution flow while preserving all essential functionality