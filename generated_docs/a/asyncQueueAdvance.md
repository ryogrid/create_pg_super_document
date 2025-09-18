# asyncQueueAdvance

## Location
src/backend/commands/async.c: 1287 - 1319

## Overview
Advances a QueuePosition to the next entry location after accounting for the current entry's length, and detects when a page boundary is crossed.

## Definition


## Detailed Description
This function is responsible for calculating the next position in the notification queue after consuming or writing an entry of a given length. It performs two main operations:

1. **Position Advancement**: Moves the current position forward by the specified entry length
2. **Page Boundary Detection**: Determines if there's enough space on the current page for another minimum-sized entry (AsyncQueueEntryEmptySize), and if not, advances to the next page

The function implements the queue's page-based storage model where each page has a fixed size (QUEUE_PAGESIZE) and entries cannot span across page boundaries. When advancing to a new page, the offset is reset to 0 and the page number is incremented.

## Parameters / Member Variables
- : Pointer to volatile QueuePosition structure to be updated with the new position
- : Size in bytes of the current entry that was just processed
- **Returns**:  - true if advancement resulted in jumping to a new page, false if staying on the same page

## Dependencies
- Functions called/Symbols referenced:
  - QUEUE_POS_PAGE (macro to extract page number from position)
  - QUEUE_POS_OFFSET (macro to extract offset from position)
  - SET_QUEUE_POS (macro to set new page and offset in position)
  - QUEUEALIGN (alignment macro for queue entries)
  - QUEUE_PAGESIZE (constant defining page size)
  - AsyncQueueEntryEmptySize (minimum entry size)

- Called from:
  - asyncQueueAddEntries (when adding new notification entries)
  - asyncQueueProcessPageEntries (when reading/processing entries)

## Notes and Other Information
- This is a static function internal to async.c
- The function ensures proper alignment and prevents entries from spanning page boundaries
- Uses volatile qualifier on position parameter to handle shared memory access safely
- The page jump detection is crucial for queue management and cleanup operations
- Part of the low-level queue management infrastructure for PostgreSQL's LISTEN/NOTIFY system
- Assert statement ensures that entry length never exceeds page size