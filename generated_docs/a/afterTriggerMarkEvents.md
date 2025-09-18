# afterTriggerMarkEvents

## Location
[src/backend/commands/trigger.c:4630-4713](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L4630-L4713)

## Overview
Scans a given event list for trigger events that haven't been invoked yet and marks those that can be invoked now with the current firing ID, with optional transfer of non-invokable events to a separate list.

## Definition


## Detailed Description
This function is a key component of PostgreSQL's deferred trigger execution mechanism. It processes an event list to determine which triggers are ready to be fired based on their current state and configuration. The function iterates through all events in the provided list, checking each event's flags to determine if it has already been processed or is currently in progress.

For events that haven't been called or scheduled yet, the function applies deferral logic based on the  parameter and the trigger's state. Events that can be invoked are marked with the current firing ID and flagged as in progress. Events that should be deferred can optionally be moved to a separate list for later processing.

The function includes security checks to prevent deferred triggers from being fired within security-restricted operations, maintaining PostgreSQL's security model.

## Parameters / Member Variables
- : Pointer to the AfterTriggerEventList containing trigger events to be processed
- : Optional pointer to an AfterTriggerEventList where deferred events should be moved (can be NULL)
- : Boolean flag indicating whether to process only immediate triggers (true) or include deferred triggers (false, typically only at main transaction exit)

## Dependencies
- Functions called/Symbols referenced:
  - GetTriggerSharedData
  - [afterTriggerCheckState](afterTriggerCheckState.md)
  - [afterTriggerAddEvent](afterTriggerAddEvent.md)
  - [InSecurityRestrictedOperation](../I/InSecurityRestrictedOperation.md)
  - for_each_event_chunk (macro)
- Called from (representative examples):
  - [AfterTriggerEndQuery](../A/AfterTriggerEndQuery.md)
  - [AfterTriggerFireDeferred](../A/AfterTriggerFireDeferred.md)
  - [AfterTriggerSetState](../A/AfterTriggerSetState.md)

## Notes and Other Information
- Returns true if any invokable events were found, false otherwise
- Uses the global  to assign firing IDs to events
- Implements security restrictions by preventing deferred trigger execution in security-restricted operations
- The function modifies event flags in place, marking processed events as either IN_PROGRESS or DONE
- Part of PostgreSQL's sophisticated trigger deferral mechanism that allows triggers to be executed at specific points in transaction processing