# afterTriggerRestoreEventList

## Location
[src/backend/commands/trigger.c:4253-4292](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L4253-L4292)

## Overview
Restores an AfterTriggerEventList to a previous state by removing events added after a given checkpoint, supporting rollback scenarios in trigger processing.

## Definition
```c
static void afterTriggerRestoreEventList(AfterTriggerEventList *events, const AfterTriggerEventList *old_events)
```

## Detailed Description
This function implements a rollback mechanism for after-trigger event lists by restoring them to a previous state. It handles two scenarios: complete restoration (when old_events->tail is NULL, indicating a completely empty state) and partial restoration (preserving events up to the old_events checkpoint). In complete restoration, it calls afterTriggerFreeEventList to free everything. In partial restoration, it copies the old_events state, frees any chunks after the preserved tail chunk, and adjusts the tail chunk's freeptr to match the old state. The function deliberately does not remove shared data records as they might still be useful for other purposes.

## Parameters / Member Variables
- `events`: The current AfterTriggerEventList to be restored
- `old_events`: The checkpoint state to restore to, representing the desired prior state

## Dependencies
- Functions called/Symbols referenced:
  - [afterTriggerFreeEventList](afterTriggerFreeEventList.md)
  - [pfree](../p/pfree.md)
  - [AfterTriggerEventList](../A/AfterTriggerEventList.md) (structure type)
  - [AfterTriggerEventChunk](../A/AfterTriggerEventChunk.md) (structure type)
- Called from (representative examples):
  - [AfterTriggerEndSubXact](../A/AfterTriggerEndSubXact.md)

## Notes and Other Information
- Used primarily for subtransaction rollback scenarios in trigger processing
- Efficiently handles both complete and partial restoration without unnecessary memory operations
- Shared data records are intentionally preserved for potential reuse
- Part of PostgreSQL's transaction safety mechanisms for deferred triggers
- The function maintains the integrity of the event list structure during restoration