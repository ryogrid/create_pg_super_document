# AfterTriggerEvent

## Location
[src/backend/commands/trigger.c:3728-3729](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L3728-L3729)

## Overview
AfterTriggerEvent is a pointer type that references AfterTriggerEventData structures, used to represent deferred trigger events in PostgreSQL's trigger system.

## Definition

```c
typedef struct AfterTriggerEventData *AfterTriggerEvent;
```
## Detailed Description
AfterTriggerEvent serves as a handle to trigger event data structures in PostgreSQL's deferred trigger execution system. It is essentially a pointer to AfterTriggerEventData, which contains the actual event information including status flags, tuple identifiers (CTIDs), and partition OIDs for cross-partition operations.

This type provides a clean abstraction for managing trigger events throughout the trigger execution pipeline, allowing functions to pass around references to trigger events without directly manipulating the underlying data structure.

## Parameters / Member Variables
Since this is a typedef for a pointer to AfterTriggerEventData, it doesn't have direct members but points to:
- AfterTriggerEventData structure containing trigger event details

## Dependencies
- Functions called/Symbols referenced:
  - [AfterTriggerEventData](AfterTriggerEventData.md) (the pointed-to structure)
- Called from (representative examples):
  - for_each_event
  - for_each_event_from
  - [afterTriggerAddEvent](../a/afterTriggerAddEvent.md)
  - [AfterTriggerExecute](AfterTriggerExecute.md)
  - [afterTriggerMarkEvents](../a/afterTriggerMarkEvents.md)
  - [afterTriggerInvokeEvents](../a/afterTriggerInvokeEvents.md)
  - [AfterTriggerEndSubXact](AfterTriggerEndSubXact.md)
  - [AfterTriggerPendingOnRel](AfterTriggerPendingOnRel.md)
  - [cancel_prior_stmt_triggers](../c/cancel_prior_stmt_triggers.md)

## Notes and Other Information
- Used extensively throughout the trigger execution system as a convenient handle for trigger events
- The actual storage and memory management is handled at the AfterTriggerEventData level
- Part of PostgreSQL's deferred constraint and trigger execution infrastructure
- Provides type safety and code clarity when working with trigger event pointers