# afterTriggerFreeEventList

## Location
src/backend/commands/trigger.c: 4232 - 4252

## Overview
Frees all event storage chunks in a given AfterTriggerEventList, releasing memory used by after-trigger events.

## Definition
```c
static void afterTriggerFreeEventList(AfterTriggerEventList *events)
```

## Detailed Description
This function iterates through all chunks in an AfterTriggerEventList and frees each chunk using pfree(). It systematically traverses the linked list of event chunks, starting from the head, and deallocates each chunk while updating the list head pointer. After freeing all chunks, it sets the tail and tailfree pointers to NULL, ensuring the event list is in a clean, empty state. This function is essential for memory cleanup during trigger event processing and transaction cleanup.

## Parameters / Member Variables
- `events`: Pointer to the AfterTriggerEventList structure whose chunks should be freed

## Dependencies
- Functions called/Symbols referenced:
  - [pfree](../p/pfree.md)
  - [AfterTriggerEventList](../A/AfterTriggerEventList.md) (structure type)
  - [AfterTriggerEventChunk](../A/AfterTriggerEventChunk.md) (structure type)
- Called from (representative examples):
  - [afterTriggerRestoreEventList](afterTriggerRestoreEventList.md)
  - [AfterTriggerFreeQuery](../A/AfterTriggerFreeQuery.md)

## Notes and Other Information
- The function is static, used only within the trigger.c module
- Performs complete cleanup of event list memory, leaving the list structure empty but valid
- Part of PostgreSQL's memory management for deferred trigger execution
- Does not free the events structure itself, only the chunks it contains
- Safe to call on already-empty event lists