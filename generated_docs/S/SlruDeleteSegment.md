# SlruDeleteSegment

## Location
[src/backend/access/transam/slru.c:1523-1599](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/slru.c#L1523-L1599)

## Overview
Public function to delete an individual SLRU segment, ensuring all buffer references are cleaned out before physical deletion.

## Definition
```c
void SlruDeleteSegment(SlruCtl ctl, int64 segno)
```

## Detailed Description
SlruDeleteSegment provides a safe way to delete an SLRU segment by first cleaning out any existing references to the segment in the SLRU buffer pool, then calling SlruInternalDeleteSegment to perform the actual file deletion. The function uses a sophisticated locking mechanism with bank locks to ensure thread safety while scanning through all slots.

The function performs these key operations:
1. Acquires appropriate bank locks for thread-safe buffer scanning
2. Scans all SLRU slots to find pages belonging to the target segment
3. For clean pages, marks them as empty; for dirty pages, writes them out first
4. Uses a restart mechanism to handle cases where new pages are loaded during I/O
5. Finally calls SlruInternalDeleteSegment to delete the physical file

The bank locking mechanism minimizes contention by only holding locks on relevant banks, switching locks as needed when scanning across different banks.

## Parameters / Member Variables
- `ctl`: SlruCtl structure containing SLRU control information and configuration
- `segno`: int64 segment number identifying which SLRU segment to delete

## Dependencies
- Functions called/Symbols referenced:
  - SlotGetBankNumber (to determine which bank lock to use)
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease (for bank locking)
  - [SlruInternalWritePage](SlruInternalWritePage.md) (to flush dirty pages)
  - [SimpleLruWaitIO](SimpleLruWaitIO.md) (to wait for ongoing I/O)
  - [SlruInternalDeleteSegment](SlruInternalDeleteSegment.md) (to perform actual file deletion)
- Called from (representative examples):
  - [PerformMembersTruncation](../P/PerformMembersTruncation.md) (in multixact.c)
  - [test_slru_page_delete](../t/test_slru_page_delete.md) (in test code)

## Notes and Other Information
- This is a public function exposed to other PostgreSQL subsystems
- Uses restart logic to handle cases where I/O operations might load new pages
- Employs bank-based locking to reduce contention in multi-slot scenarios
- Ensures data consistency by writing out dirty pages before deletion
- Part of PostgreSQL's SLRU subsystem used for transaction logs like CLOG, subtrans, etc.
- The function is designed to be safe even when concurrent operations might be accessing the SLRU

## Simplified Source

```c
void SlruDeleteSegment(SlruCtl ctl, int64 segno) {
    SlruShared shared = ctl->shared;
    int prevbank = SlotGetBankNumber(0);
    bool did_write;

    // Lock the first bank and scan all slots
    LWLockAcquire(&shared->bank_locks[prevbank].lock, LW_EXCLUSIVE);

restart:
    did_write = false;
    for (int slotno = 0; slotno < shared->num_slots; slotno++) {
        int64 pagesegno;
        int curbank = SlotGetBankNumber(slotno);

        // Switch bank locks if needed
        if (curbank != prevbank) {
            LWLockRelease(&shared->bank_locks[prevbank].lock);
            LWLockAcquire(&shared->bank_locks[curbank].lock, LW_EXCLUSIVE);
            prevbank = curbank;
        }

        // Skip empty slots
        if (shared->page_status[slotno] == SLRU_PAGE_EMPTY)
            continue;

        // Check if this page belongs to our target segment
        pagesegno = shared->page_number[slotno] / SLRU_PAGES_PER_SEGMENT;
        if (pagesegno != segno)
            continue;

        // Handle clean pages - just mark as empty
        if (shared->page_status[slotno] == SLRU_PAGE_VALID &&
            !shared->page_dirty[slotno]) {
            shared->page_status[slotno] = SLRU_PAGE_EMPTY;
            continue;
        }

        // Handle dirty pages - write them out first
        if (shared->page_status[slotno] == SLRU_PAGE_VALID)
            SlruInternalWritePage(ctl, slotno, NULL);
        else
            SimpleLruWaitIO(ctl, slotno);

        did_write = true;
    }

    // Restart if we did any I/O (new pages might have been loaded)
    if (did_write)
        goto restart;

    // Actually delete the segment file
    SlruInternalDeleteSegment(ctl, segno);

    LWLockRelease(&shared->bank_locks[prevbank].lock);
}
```