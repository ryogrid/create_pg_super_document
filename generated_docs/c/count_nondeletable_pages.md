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

## Simplified Source

```c
static BlockNumber
count_nondeletable_pages(LVRelState *vacrel, bool *lock_waiter_detected)
{
    BlockNumber blkno;
    BlockNumber prefetchedUntil;
    instr_time starttime;

    // Initialize timing for lock conflict detection
    INSTR_TIME_SET_CURRENT(starttime);

    // Scan backwards from relation end with prefetching
    blkno = vacrel->rel_pages;
    prefetchedUntil = InvalidBlockNumber;

    while (blkno > vacrel->nonempty_pages) {
        Buffer buf;
        Page page;
        OffsetNumber offnum, maxoff;
        bool hastup;

        // Check for lock conflicts every 32 blocks
        if ((blkno % 32) == 0) {
            instr_time currenttime, elapsed;
            INSTR_TIME_SET_CURRENT(currenttime);
            elapsed = currenttime;
            INSTR_TIME_SUBTRACT(elapsed, starttime);

            if ((INSTR_TIME_GET_MICROSEC(elapsed) / 1000) >=
                VACUUM_TRUNCATE_LOCK_CHECK_INTERVAL) {
                if (LockHasWaitersRelation(vacrel->rel, AccessExclusiveLock)) {
                    ereport(vacrel->verbose ? INFO : DEBUG2,
                            (errmsg("suspending truncate due to conflicting lock request")));
                    *lock_waiter_detected = true;
                    return blkno;
                }
                starttime = currenttime;
            }
        }

        CHECK_FOR_INTERRUPTS();
        blkno--;

        // Prefetch blocks for better I/O performance
        if (prefetchedUntil > blkno) {
            BlockNumber prefetchStart = blkno & ~(PREFETCH_SIZE - 1);
            for (BlockNumber pblkno = prefetchStart; pblkno <= blkno; pblkno++) {
                PrefetchBuffer(vacrel->rel, MAIN_FORKNUM, pblkno);
                CHECK_FOR_INTERRUPTS();
            }
            prefetchedUntil = prefetchStart;
        }

        // Read and examine the page
        buf = ReadBufferExtended(vacrel->rel, MAIN_FORKNUM, blkno,
                                RBM_NORMAL, vacrel->bstrategy);
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buf);

        if (PageIsNew(page) || PageIsEmpty(page)) {
            UnlockReleaseBuffer(buf);
            continue;
        }

        // Check if page has any used items
        hastup = false;
        maxoff = PageGetMaxOffsetNumber(page);
        for (offnum = FirstOffsetNumber; offnum <= maxoff;
             offnum = OffsetNumberNext(offnum)) {
            ItemId itemid = PageGetItemId(page, offnum);
            if (ItemIdIsUsed(itemid)) {
                hastup = true;
                break;
            }
        }

        UnlockReleaseBuffer(buf);

        // If page has tuples, we found the end
        if (hastup)
            return blkno + 1;
    }

    // All pages are empty
    return vacrel->nonempty_pages;
}
```