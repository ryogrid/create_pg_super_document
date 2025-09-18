# asyncQueueAddEntries

## Location
src/backend/commands/async.c: 1356 - 1480

## Overview
Adds pending notification messages to the shared memory notification queue, processing them page by page and handling page boundaries correctly.

## Definition


## Detailed Description
This function is the core implementation for writing notification entries into the asynchronous notification queue. It operates with the following key characteristics:

1. **Page-by-Page Processing**: Works one page at a time, stopping when it needs to advance to a new page to allow for proper error handling and resource management.

2. **Atomic Page Operations**: Uses a local copy of QUEUE_HEAD to ensure that if page allocation fails (e.g., out of disk space), the global queue head is not corrupted.

3. **Page Filling Strategy**: Ensures every page is completely filled by writing dummy entries (with InvalidOid) when a real notification doesn't fit, simplifying subsequent reads.

4. **SLRU Integration**: Properly handles the Simple LRU (SLRU) buffer management, including page locking and initialization.

5. **Cleanup Scheduling**: Sets the tryAdvanceTail flag when appropriate to schedule tail pointer advancement for garbage collection.

The function processes notifications from the pendingNotifies list, converting each to the proper queue entry format and writing them to shared memory pages.

## Parameters / Member Variables
- : Pointer to the next ListCell in pendingNotifies->events to process
- **Returns**:  - Pointer to the first unprocessed notification (NULL if all processed)

## Dependencies
- Functions called/Symbols referenced:
  - [asyncQueueNotificationToEntry](asyncQueueNotificationToEntry.md) (to convert notifications to queue entries)
  - [asyncQueueAdvance](asyncQueueAdvance.md) (to advance queue position)
  - [SimpleLruGetBankLock](../S/SimpleLruGetBankLock.md) (to get SLRU bank lock for page)
  - [SimpleLruZeroPage](../S/SimpleLruZeroPage.md) (to initialize new pages)
  - [SimpleLruReadPage](../S/SimpleLruReadPage.md) (to read existing pages)
  - LWLockAcquire/LWLockRelease (for locking)
  - QUEUE_HEAD, QUEUE_POS_PAGE, QUEUE_POS_OFFSET (queue position macros)
  - QUEUE_POS_IS_ZERO (to check for first write)
  - QUEUE_PAGESIZE (page size constant)
  - QUEUE_CLEANUP_DELAY (cleanup scheduling constant)
  - NotifyCtl (SLRU control structure)
  - [lnext](../l/lnext.md) (list navigation)
  - memcpy (for data copying)

- Called from:
  - [PreCommit_Notify](../P/PreCommit_Notify.md) (during transaction commit to flush pending notifications)

## Notes and Other Information
- This is a static function internal to async.c
- Caller must already hold NotifyQueueLock in exclusive mode
- Uses local queue_head copy for transactional safety - global QUEUE_HEAD only updated on success
- Handles both first-time initialization (QUEUE_POS_IS_ZERO) and normal operation
- Properly manages SLRU bank locks, which may change when advancing to new pages
- The dummy entry mechanism ensures consistent page layout for readers
- Part of PostgreSQL's asynchronous notification system's write path
- Critical for maintaining queue integrity across transaction boundaries