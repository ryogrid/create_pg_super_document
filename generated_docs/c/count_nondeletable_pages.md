# count_nondeletable_pages

## Location
[src/backend/access/heap/vacuumlazy.c:2681-2822](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/vacuumlazy.c#L2681-L2822)

## Overview
Rescans pages from the end of a relation backwards to verify they are still empty and safe for truncation, detecting concurrent lock conflicts.

## Definition
```c
static BlockNumber
count_nondeletable_pages(LVRelState *vacrel, bool *lock_waiter_detected)
```

## Detailed Description
This function performs a critical validation step during heap truncation by scanning backwards from the relation end to find the last page containing tuples. It implements an optimized scanning strategy using prefetching to improve I/O performance while holding AccessExclusiveLock. The function monitors for lock conflicts by periodically checking if other processes are waiting for the exclusive lock, allowing truncation to be suspended to avoid blocking other operations. For each page, it examines all item identifiers to determine if any are in use, considering even LP_DEAD items as reasons to keep the page since their index entries may not have been cleaned yet.

## Parameters / Member Variables
- `vacrel`: Vacuum relation state containing relation reference and page boundaries
- `lock_waiter_detected`: Output parameter set to true if lock conflicts are detected

## Dependencies
- Functions called/Symbols referenced:
  - [instr_time](../i/instr_time.md)
  - INSTR_TIME_SET_CURRENT
  - INSTR_TIME_SUBTRACT
  - INSTR_TIME_GET_MICROSEC
  - [LockHasWaitersRelation](../L/LockHasWaitersRelation.md)
  - [PrefetchBuffer](../P/PrefetchBuffer.md)
  - [ReadBufferExtended](../R/ReadBufferExtended.md)
  - [LockBuffer](../L/LockBuffer.md)
  - [BufferGetPage](../B/BufferGetPage.md)
  - [PageIsNew](../P/PageIsNew.md)
  - [PageIsEmpty](../P/PageIsEmpty.md)
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - ItemIdIsUsed
  - VACUUM_TRUNCATE_LOCK_CHECK_INTERVAL
  - PREFETCH_SIZE
- Called from (representative examples):
  - [lazy_truncate_heap](../l/lazy_truncate_heap.md)

## Notes and Other Information
The function uses sophisticated I/O optimization with block prefetching and avoids vacuum delay points since it holds an exclusive lock that should be released quickly. It implements a time-based lock conflict detection mechanism that checks for waiting processes every 32 blocks but only performs the expensive lock waiter check after a specified time interval has elapsed. The backward scanning approach with prefetching allows for efficient verification while minimizing the time spent holding the exclusive lock.