# EventTriggerBeginCompleteQuery

## Location
[src/backend/commands/event_trigger.c:1184-1227](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/event_trigger.c#L1184-L1227)

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



## Dependencies
- Functions called/Symbols referenced:
  - [trackDroppedObjectsNeeded](../t/trackDroppedObjectsNeeded.md) (checks if event trigger state is necessary)
  - AllocSetContextCreate (memory context creation)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md) (state structure allocation)  
  - [slist_init](../s/slist_init.md) (initializes SQL drop list)
  - [EventTriggerQueryState](EventTriggerQueryState.md) (state structure type)
- Called from:
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (utility command processing)
  - CALLED_AS_EVENT_TRIGGER (macro usage)

## Notes and Other Information
- Must be paired with EventTriggerEndCompleteQuery() regardless of query success/failure
- Requires PG_TRY block usage when called due to mandatory cleanup requirements
- Creates state in TopMemoryContext to survive transaction boundaries
- Maintains a stack of event trigger states for handling nested operations
- The commandCollectionInhibited flag is inherited from previous state if it exists
- Only initializes state when sql_drop, table_rewrite, or ddl_command_end events are relevant
- Critical for proper event trigger functionality in DDL operations

## Simplified Source

```c
bool
EventTriggerBeginCompleteQuery(void)
{
    EventTriggerQueryState *state;
    MemoryContext cxt;

    // Only create state if event triggers need it
    if (!trackDroppedObjectsNeeded())
        return false;

    // Create dedicated memory context for event trigger state
    cxt = AllocSetContextCreate(TopMemoryContext,
                               "event trigger state",
                               ALLOCSET_DEFAULT_SIZES);

    // Allocate and initialize state structure
    state = MemoryContextAlloc(cxt, sizeof(EventTriggerQueryState));
    state->cxt = cxt;
    slist_init(&(state->SQLDropList));
    state->in_sql_drop = false;
    state->table_rewrite_oid = InvalidOid;

    // Initialize command collection fields
    state->commandCollectionInhibited = currentEventTriggerState ?
        currentEventTriggerState->commandCollectionInhibited : false;
    state->currentCommand = NULL;
    state->commandList = NIL;

    // Link to previous state (stack behavior) and set as current
    state->previous = currentEventTriggerState;
    currentEventTriggerState = state;

    return true;
}
```