# afterTriggerAddEvent

## Location
src/backend/commands/trigger.c: 4111 - 4151

## Overview
Adds a new trigger event to a specified event queue, handling memory allocation and optimization for trigger event storage in PostgreSQL.

## Definition
```c
static void afterTriggerAddEvent(AfterTriggerEventList *events,
                               AfterTriggerEvent event, 
                               AfterTriggerShared evtshared)
```

## Detailed Description
The `afterTriggerAddEvent` function is responsible for adding trigger events to PostgreSQL's deferred trigger execution system. It manages memory allocation for trigger events using a chunked storage strategy that dynamically adapts to usage patterns.

The function implements several key optimizations:
- **Chunked Memory Management**: Uses variable-sized chunks (1KB to 1MB) to store trigger events efficiently
- **Shared Record Deduplication**: Attempts to reuse existing shared trigger data records within the same chunk to reduce memory consumption
- **Adaptive Chunk Sizing**: Adjusts chunk size based on the number of shared records in previous chunks to optimize for different usage patterns
- **Memory Context Management**: Creates and manages a dedicated memory context for trigger events

The function ensures proper linking between trigger events and their shared metadata using offset-based pointers stored in the event flags.

## Parameters / Member Variables
- `events`: Pointer to the AfterTriggerEventList where the event should be added
- `event`: The trigger event data to be copied and stored
- `evtshared`: Shared trigger metadata that may be deduplicated across multiple events

## Dependencies
- Functions called/Symbols referenced:
  - SizeofTriggerEvent (macro for calculating event size)
  - AllocSetContextCreate (memory context creation)
  - MemoryContextAlloc (memory allocation)
  - afterTriggerCopyBitmap (bitmap copying for modified columns)
  - bms_equal (bitmap comparison)
  - memcpy (memory copying)

- Called from (representative examples):
  - afterTriggerMarkEvents (marks events for execution)
  - AfterTriggerSaveEvent (saves events during trigger processing)

## Notes and Other Information
- The function is static and only used within the trigger system implementation
- Implements sophisticated memory management with adaptive chunk sizing based on shared record density
- Maximum chunk size is limited by AFTER_TRIGGER_OFFSET to ensure proper event-to-shared-record linking
- The chunk size doubles if the previous chunk had few shared records (≤100), otherwise it halves to optimize memory usage
- All trigger event data is copied rather than referenced, ensuring data integrity across transaction boundaries
- The function maintains the integrity of the event list structure by properly updating head, tail, and free pointer references