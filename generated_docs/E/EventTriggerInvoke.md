# EventTriggerInvoke

## Location
src/backend/commands/event_trigger.c: 1069 - 1133

## Overview
EventTriggerInvoke is a static function that executes a list of event trigger functions in sequence, managing memory context and ensuring proper isolation between trigger executions.

## Definition


## Detailed Description
EventTriggerInvoke iterates through a list of event trigger function OIDs and executes each one in sequence. The function provides several important guarantees:

1. **Memory Management**: Creates a dedicated memory context for event trigger execution to prevent memory leaks and ensure cleanup after each trigger.
2. **Stack Protection**: Guards against stack overflow from recursive event trigger calls using check_stack_depth().
3. **Command Visibility**: Ensures each event trigger can see the results of previous triggers by calling CommandCounterIncrement() between executions (except for the first trigger).
4. **Statistics Tracking**: Collects function call statistics for each triggered function.

The function operates in a temporary memory context that is reset after each trigger execution and deleted when all triggers complete.

## Parameters / Member Variables
- : List of function OIDs representing the event triggers to be invoked
- : EventTriggerData structure containing context information passed to each trigger function

## Dependencies
- Functions called/Symbols referenced:
  - check_stack_depth (stack overflow protection)
  - AllocSetContextCreate (memory context creation)
  - CommandCounterIncrement (command visibility)
  - fmgr_info (function manager lookup)
  - InitFunctionCallInfoData (function call setup)
  - FunctionCallInvoke (actual function execution)
  - pgstat_init_function_usage/pgstat_end_function_usage (statistics)
  - MemoryContextReset/MemoryContextDelete (memory management)
- Called from:
  - EventTriggerDDLCommandStart
  - EventTriggerDDLCommandEnd
  - EventTriggerSQLDrop
  - EventTriggerOnLogin
  - EventTriggerTableRewrite

## Notes and Other Information
- This is a static function internal to the event trigger system
- Uses DEBUG1 logging to trace function OID execution
- Memory context isolation prevents interference between different trigger executions
- The first trigger doesn't require CommandCounterIncrement() as there are no previous changes to see
- Function call statistics are properly tracked for performance monitoring