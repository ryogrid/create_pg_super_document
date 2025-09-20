# EventTriggerEndCompleteQuery

## Location
[src/backend/commands/event_trigger.c:1228-1245](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/event_trigger.c#L1228-L1245)

## Overview
EventTriggerEndCompleteQuery performs cleanup of event trigger state after a complete query finishes execution, restoring the previous state and freeing associated memory.

## Definition
```c
void EventTriggerEndCompleteQuery(void)
```

## Detailed Description
This function serves as the mandatory cleanup counterpart to EventTriggerBeginCompleteQuery(). It performs essential cleanup operations that must occur regardless of whether the query completed successfully or failed with an error:

1. **State Stack Management**: Retrieves the previous event trigger state from the current state's stack-like structure, preparing to restore it as the active state.

2. **Memory Cleanup**: Deletes the entire memory context associated with the current event trigger state. This approach provides efficient bulk cleanup of all memory allocated during the query execution, including the SQLDropList items and other state-related allocations.

3. **State Restoration**: Restores the previous event trigger state as the current global state, effectively popping the state stack.

The function is designed to be safe for use in error handling contexts (PG_CATCH blocks) and avoids operations that might fail or allocate additional memory.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextDelete](../M/MemoryContextDelete.md) (bulk memory cleanup)
  - [EventTriggerQueryState](EventTriggerQueryState.md) (state structure type)
- Called from:
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (utility command processing cleanup)
  - CALLED_AS_EVENT_TRIGGER (macro usage)

## Notes and Other Information
- Must only be called if EventTriggerBeginCompleteQuery() previously returned true
- Safe to call from PG_CATCH blocks during error handling
- Avoids individual pfree() calls by deleting the entire memory context
- Does not allocate memory to prevent issues in error scenarios
- Critical for preventing memory leaks in event trigger processing
- Maintains the stack-like behavior of nested event trigger states
- The bulk memory context deletion efficiently cleans up SQLDropList items and other associated data
- Essential for proper resource management in both success and failure cases