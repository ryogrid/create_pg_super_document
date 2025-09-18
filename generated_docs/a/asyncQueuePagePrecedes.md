# asyncQueuePagePrecedes

## Location
[src/backend/commands/async.c:476-484](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/async.c#L476-L484)

## Overview
A simple inline function that determines whether one queue page number precedes another in PostgreSQL's asynchronous notification system.

## Definition
```c
static inline bool asyncQueuePagePrecedes(int64 p, int64 q)
```

## Detailed Description
The `asyncQueuePagePrecedes` function performs a straightforward comparison to determine if page number `p` precedes page number `q` in the queue ordering. The implementation is simply `p < q`, which indicates a linear ordering system. According to the code comments, this function previously handled wraparound scenarios, but like its companion function `asyncQueuePageDiff`, it has been simplified to use basic arithmetic comparison. This suggests the queue page numbering system has been redesigned to avoid wraparound complexities.

## Parameters / Member Variables
- `p`: The first queue page number to compare (int64)
- `q`: The second queue page number to compare (int64)

## Dependencies
- Functions called/Symbols referenced: None
- Called from (representative examples):
  - QUEUE_POS_MIN
  - QUEUE_POS_MAX  
  - NotificationHash
  - [AsyncShmemInit](../A/AsyncShmemInit.md)
  - [asyncQueueAdvanceTail](asyncQueueAdvanceTail.md)

## Notes and Other Information
- The function is declared as `static inline`, making it internal to the async.c file and suitable for compiler inlining
- Returns a boolean value indicating the precedence relationship
- The simplification from wraparound-aware logic to simple comparison suggests improved queue design
- Used extensively throughout the notification queue management system for ordering and positioning operations
- The use of int64 provides sufficient range to avoid practical wraparound concerns