# AfterTriggerEventList

## Location
src/backend/commands/trigger.c: 3797 - 3802

## Overview
AfterTriggerEventList is a container structure that manages a linked list of AfterTriggerEventChunk structures, providing efficient access to both the head and tail of the event storage chain.

## Definition
```c
typedef struct AfterTriggerEventList
{
    AfterTriggerEventChunk *head;
    AfterTriggerEventChunk *tail;
    char       *tailfree;       /* freeptr of tail chunk */
} AfterTriggerEventList;
```

## Detailed Description
The AfterTriggerEventList structure serves as a high-level container for managing collections of trigger events stored across multiple chunks. It maintains pointers to both the first and last chunks in the list, enabling efficient insertion at the tail and traversal from the head. The structure caches the tail chunk's free pointer for quick access during event addition, avoiding the need to dereference through the tail pointer repeatedly.

## Parameters / Member Variables
- `head`: Pointer to the first AfterTriggerEventChunk in the linked list
- `tail`: Pointer to the last AfterTriggerEventChunk in the linked list for efficient appending
- `tailfree`: Cached copy of the tail chunk's freeptr for fast access during allocations

## Dependencies
- Functions called/Symbols referenced:
  - AfterTriggerEventChunk (for head and tail pointers)
- Called from (representative examples):
  - AfterTriggersData (events field)
  - AfterTriggersQueryData (events field)
  - AfterTriggersTransData (events field)
  - afterTriggerAddEvent
  - afterTriggerFreeEventList
  - afterTriggerMarkEvents
  - afterTriggerInvokeEvents

## Notes and Other Information
This structure provides the primary interface for managing trigger event storage at various levels of the trigger system hierarchy. It's used in transaction-level, query-level, and table-level trigger data structures. The tailfree optimization is particularly important for performance, as trigger event addition is a frequent operation that would otherwise require pointer chasing through the chunk structure. The list design supports efficient cleanup and restoration operations needed for transaction rollback scenarios.