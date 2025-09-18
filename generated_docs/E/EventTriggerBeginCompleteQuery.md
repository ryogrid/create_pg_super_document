# EventTriggerBeginCompleteQuery

## Location
src/backend/commands/event_trigger.c: 1184 - 1227

## Overview
EventTriggerBeginCompleteQuery initializes event trigger state for tracking objects and commands during the execution of a complete query, establishing the foundation for event trigger processing.

## Definition
```c
bool EventTriggerBeginCompleteQuery(void)
```

## Detailed Description
This function sets up the necessary state tracking infrastructure for event triggers before a complete query begins execution. It performs several critical initialization tasks:

1. **Conditional Setup**: Only creates event trigger state if it's actually needed (determined by trackDroppedObjectsNeeded()). This optimization avoids unnecessary overhead when no relevant event triggers are present.

2. **Memory Context Creation**: Establishes a dedicated memory context in TopMemoryContext for event trigger state that persists across the query execution.

3. **State Structure Initialization**: Creates and initializes an EventTriggerQueryState structure with:
   - Empty SQL drop object list (SQLDropList)
   - sql_drop tracking flag set to false
   - Invalid table rewrite OID
   - Command collection and command list management
   - Linking to previous state (stack-like behavior)

4. **Global State Management**: Sets the new state as the current global event trigger state, maintaining a stack of states for nested operations.

The function returns true if state was successfully created, false if no setup was needed.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - trackDroppedObjectsNeeded (checks if event trigger state is necessary)
  - AllocSetContextCreate (memory context creation)
  - MemoryContextAlloc (state structure allocation)  
  - slist_init (initializes SQL drop list)
  - EventTriggerQueryState (state structure type)
- Called from:
  - ProcessUtilitySlow (utility command processing)
  - CALLED_AS_EVENT_TRIGGER (macro usage)

## Notes and Other Information
- Must be paired with EventTriggerEndCompleteQuery() regardless of query success/failure
- Requires PG_TRY block usage when called due to mandatory cleanup requirements
- Creates state in TopMemoryContext to survive transaction boundaries
- Maintains a stack of event trigger states for handling nested operations
- The commandCollectionInhibited flag is inherited from previous state if it exists
- Only initializes state when sql_drop, table_rewrite, or ddl_command_end events are relevant
- Critical for proper event trigger functionality in DDL operations