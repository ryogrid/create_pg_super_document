# AfterTriggerEventChunk

## Location
[src/backend/commands/trigger.c:3785-3792](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L3785-L3792)

## Overview
AfterTriggerEventChunk is a memory management structure that stores trigger events in successively-larger chunks to avoid palloc overhead, implementing an expandable array design for deferred trigger events.

## Definition
```c
typedef struct AfterTriggerEventChunk
{
    struct AfterTriggerEventChunk *next;    /* list link */
    char       *freeptr;        /* start of free space in chunk */
    char       *endfree;        /* end of free space in chunk */
    char       *endptr;         /* end of chunk */
    /* event data follows here */
} AfterTriggerEventChunk;
```

## Detailed Description
The AfterTriggerEventChunk structure implements a sophisticated memory management system for storing trigger events. It uses a chunk-based allocation strategy where trigger events are stored in progressively larger memory chunks to minimize allocation overhead. The space between CHUNK_DATA_START and freeptr contains AfterTriggerEventData records, while the space between endfree and endptr contains AfterTriggerSharedData records. This bidirectional allocation approach optimizes memory usage by packing both event data and shared data into the same chunks.

## Parameters / Member Variables
- `next`: Pointer to the next chunk in the linked list, enabling dynamic expansion of storage
- `freeptr`: Pointer to the start of available free space in the chunk for new allocations
- `endfree`: Pointer marking the end of free space available for forward allocation
- `endptr`: Pointer to the absolute end of the allocated chunk memory

## Dependencies
- Functions called/Symbols referenced:
  - [AfterTriggerEventChunk](AfterTriggerEventChunk.md) (self-reference for linked list)
- Called from (representative examples):
  - [afterTriggerAddEvent](../a/afterTriggerAddEvent.md)
  - [afterTriggerFreeEventList](../a/afterTriggerFreeEventList.md)
  - [afterTriggerRestoreEventList](../a/afterTriggerRestoreEventList.md)
  - [afterTriggerDeleteHeadEventChunk](../a/afterTriggerDeleteHeadEventChunk.md)
  - [afterTriggerMarkEvents](../a/afterTriggerMarkEvents.md)
  - [afterTriggerInvokeEvents](../a/afterTriggerInvokeEvents.md)

## Notes and Other Information
The chunk design uses a bidirectional allocation strategy where event data grows from the beginning (freeptr) and shared data grows from the end (endfree), meeting in the middle. The CHUNK_DATA_START macro calculates the actual start of usable data space accounting for proper memory alignment. This structure is fundamental to PostgreSQL's deferred trigger execution system, providing efficient storage for potentially large numbers of trigger events that must be held until transaction commit time.