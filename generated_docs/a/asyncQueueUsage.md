# asyncQueueUsage

## Location
[src/backend/commands/async.c:1506-1526](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/async.c#L1506-L1526)

## Overview
Internal function that calculates and returns the fraction of the notification queue currently occupied, measured as the ratio of occupied pages to maximum allowed pages.

## Definition

```c
static double
asyncQueueUsage(void)
```
## Detailed Description
This function computes the current utilization of the asynchronous notification queue by measuring the distance between the head and tail page positions. It calculates the number of occupied pages and returns this as a fraction of the maximum allowed queue pages. The function uses logical tail page position rather than physical tail page position to avoid instability caused by SLRU (Simple Least Recently Used) segment boundaries and other implementation details.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - QUEUE_POS_PAGE (macro to extract page number from queue position)
  - QUEUE_HEAD (macro/variable representing current head position)
  - QUEUE_TAIL (macro/variable representing current tail position)
  - max_notify_queue_pages (global variable defining maximum queue size)
- Called from:
  - [pg_notification_queue_usage](../p/pg_notification_queue_usage.md) (SQL-callable function for monitoring)
  - [asyncQueueFillWarning](asyncQueueFillWarning.md) (checks if queue is getting full)
  - NotificationHash (context reference in async.c)

## Notes and Other Information
- Caller must hold NotifyQueueLock in at least shared mode before calling
- Uses logical tail page rather than physical tail page for stability
- Returns 0.0 immediately if no pages are occupied (common case optimization)
- The calculation is based on page-level granularity rather than individual notification granularity
- Internal static function, not exposed outside async.c module
- Used for both monitoring (via SQL function) and internal queue management decisions

## Simplified Source

```c
static double asyncQueueUsage(void) {
    // Get head and tail page positions
    int64 headPage = QUEUE_POS_PAGE(QUEUE_HEAD);
    int64 tailPage = QUEUE_POS_PAGE(QUEUE_TAIL);
    int64 occupied = headPage - tailPage;

    // Fast exit for empty queue
    if (occupied == 0)
        return 0.0;

    // Return fraction of queue occupied
    return (double) occupied / (double) max_notify_queue_pages;
}
```