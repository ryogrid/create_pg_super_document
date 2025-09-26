# AfterTriggerShared

## Location
[src/backend/commands/trigger.c:3716-3717](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L3716-L3717)

## Overview
AfterTriggerShared is a type alias that represents a pointer to AfterTriggerSharedData structure, used for managing shared data among multiple after-trigger events in PostgreSQL.

## Definition
```c
typedef struct AfterTriggerSharedData *AfterTriggerShared;
```

## Detailed Description
This typedef creates a convenient pointer type for AfterTriggerSharedData structures. It is part of PostgreSQL's after-trigger event management system, where multiple trigger events can share common data to minimize memory consumption. The pointer type allows efficient passing and manipulation of shared trigger data across various trigger-related functions without copying the underlying structure.

## Parameters / Member Variables
- This is a typedef for a pointer to AfterTriggerSharedData, so it provides access to the shared data structure members through pointer dereferencing

## Dependencies
- Functions called/Symbols referenced:
  - AfterTriggerSharedData
- Called from (representative examples):
  - GetTriggerSharedData
  - afterTriggerCheckState
  - afterTriggerAddEvent
  - AfterTriggerExecute
  - afterTriggerMarkEvents
  - afterTriggerInvokeEvents
  - AfterTriggerEndSubXact
  - AfterTriggerPendingOnRel
  - cancel_prior_stmt_triggers

## Notes and Other Information
- Part of PostgreSQL's after-trigger event management system that defers trigger execution until later in the transaction
- The pointer type enables efficient memory management by allowing multiple trigger events to reference shared data
- Used extensively throughout the trigger subsystem for managing trigger execution state
- Follows PostgreSQL's pattern of creating pointer typedefs for frequently used structure types
- Critical component in the optimization strategy to minimize per-event memory consumption in trigger processing
- Located in src/backend/commands/trigger.c within the trigger execution framework