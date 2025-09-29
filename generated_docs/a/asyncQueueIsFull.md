# asyncQueueIsFull

## Location
[src/backend/commands/async.c:1272-1286](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/async.c#L1272-L1286)

## Overview
Tests whether there is room to insert more notification messages into the asynchronous notification queue.

## Definition

```c
static bool
asyncQueueIsFull(void)
```
## Detailed Description
This function determines if the notification queue has reached its maximum capacity by calculating the number of occupied pages in the queue. It computes the difference between the head page position (where new notifications are inserted) and the tail page position (where old notifications are read/consumed). The queue is considered full when the number of occupied pages equals or exceeds the configured maximum.

The function uses page-level granularity for capacity checking rather than individual notification entries, which provides efficient space management for the shared memory notification queue.

## Parameters / Member Variables
This function takes no parameters and returns:
- : true if the queue is full (occupied pages >= max_notify_queue_pages), false otherwise

## Dependencies
- Functions called/Symbols referenced:
  - QUEUE_POS_PAGE (macro to extract page number from queue position)
  - QUEUE_HEAD (current head position of the queue)
  - QUEUE_TAIL (current tail position of the queue)
  - max_notify_queue_pages (configuration parameter)

- Called from:
  - [PreCommit_Notify](../P/PreCommit_Notify.md) (to check if notifications can be added before commit)

## Notes and Other Information
- This is a static function internal to async.c
- Caller must hold at least shared NotifyQueueLock to ensure consistent queue state
- Uses 64-bit arithmetic to handle queue position wraparound correctly  
- The queue operates on a page-based model where each page can contain multiple notification entries
- Part of PostgreSQL's flow control mechanism to prevent unbounded queue growth
- When the queue is full, new NOTIFY commands will be blocked or delayed

## Simplified Source

```c
static bool
asyncQueueIsFull(void)
{
    // Calculate occupied pages in the queue
    int64 headPage = QUEUE_POS_PAGE(QUEUE_HEAD);  // Where new notifications go
    int64 tailPage = QUEUE_POS_PAGE(QUEUE_TAIL);  // Where old notifications are consumed
    int64 occupied = headPage - tailPage;

    // Queue is full when occupied pages exceed maximum
    return occupied >= max_notify_queue_pages;
}
```