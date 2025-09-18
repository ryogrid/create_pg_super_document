# EventTriggerDDLCommandEnd

## Location
src/backend/commands/event_trigger.c: 772 - 819

## Overview
EventTriggerDDLCommandEnd fires ddl_command_end event triggers after a DDL command completes execution, providing a hook for post-processing and cleanup operations.

## Definition
```c
void EventTriggerDDLCommandEnd(Node *parsetree)
```

## Detailed Description
This function is responsible for firing ddl_command_end event triggers at the conclusion of DDL command execution. It serves as the complementary function to EventTriggerDDLCommandStart, allowing custom functions to execute after DDL operations have completed.

The function implements important safety and consistency checks:
- Like its counterpart, it's disabled in standalone mode and respects the event_triggers GUC setting
- Includes an additional check for currentEventTriggerState to ensure triggers only fire if the command started with active event trigger state
- This prevents issues where triggers might be created during command execution and attempt to call pg_event_trigger_ddl_commands inappropriately
- Ensures main command changes are visible to triggers through CommandCounterIncrement before trigger execution

The function follows the pattern: safety checks, setup eligible triggers, make changes visible, invoke triggers, and clean up resources.

## Parameters / Member Variables
- `parsetree`: Node pointer representing the parsed DDL command that is completing

## Dependencies
- Functions called/Symbols referenced:
  - EventTriggerData (struct for trigger context)
  - EventTriggerCommonSetup (identifies applicable triggers)
  - EVT_DDLCommandEnd (event type constant)
  - CommandCounterIncrement (ensures visibility)
  - EventTriggerInvoke (executes the triggers)
  - list_free (memory cleanup)
- Called from (representative examples):
  - ProcessUtilitySlow (main DDL command processing)

## Notes and Other Information
- Includes a crucial check for currentEventTriggerState to prevent execution when no triggers were active at command start
- This state check prevents potential failures in pg_event_trigger_ddl_commands function calls
- Changes made by the main command are made visible to event triggers before they execute
- Part of PostgreSQL's comprehensive event trigger system, working in tandem with EventTriggerDDLCommandStart
- The state validation ensures consistency between command start and end trigger execution