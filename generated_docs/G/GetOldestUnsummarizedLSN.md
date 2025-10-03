# GetOldestUnsummarizedLSN

## Location
[src/backend/postmaster/walsummarizer.c:505-636](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/walsummarizer.c#L505-L636)

## Overview
Determines the oldest LSN that has not yet been summarized and updates shared memory state accordingly.

## Definition

```c
XLogRecPtr
GetOldestUnsummarizedLSN(TimeLineID *tli, bool *lsn_is_exact)
```
## Detailed Description
This function calculates the oldest Log Sequence Number (LSN) that still needs to be summarized by examining existing WAL files and summary files. It serves two main purposes: for the WAL summarizer process, it provides the starting point for summarization work; for other processes, it provides information for WAL retention decisions. The function examines the timeline history, finds the oldest available WAL segments, and accounts for existing summary files to determine where summarization should begin or resume.

The function handles initialization of shared memory state and provides different behaviors for the WAL summarizer process versus other processes. It ensures that existing summary files are not re-summarized and that WAL retention policies can make informed decisions.

## Parameters / Member Variables
- `*tli`: Output parameter for the timeline ID corresponding to the returned LSN (optional, can be NULL)
- `*lsn_is_exact`: Output parameter indicating whether the returned LSN is exact (start of WAL record) or approximate (start of WAL segment) (optional, can be NULL)
## Dependencies
- Functions called/Symbols referenced:
  - AmWalSummarizerProcess (checks if current process is WAL summarizer)
  - [GetLatestLSN](GetLatestLSN.md) (gets current latest LSN and timeline)
  - [readTimeLineHistory](../r/readTimeLineHistory.md) (reads timeline history for the latest timeline)
  - [XLogGetOldestSegno](../X/XLogGetOldestSegno.md) (finds oldest WAL segment for a timeline)
  - XLogSegNoOffsetToRecPtr (converts segment number to LSN)
  - [GetWalSummaries](GetWalSummaries.md) (retrieves existing summary files)
  - Various PostgreSQL list and memory management functions
- Called from (representative examples):
  - [KeepLogSeg](../K/KeepLogSeg.md) (in src/backend/access/transam/xlog.c:8004)
  - [WalSummarizerMain](../W/WalSummarizerMain.md) (in src/backend/postmaster/walsummarizer.c:341)

## Notes and Other Information
- Returns InvalidXLogRecPtr if WAL summarization is disabled (summarize_wal = false)
- For non-summarizer processes, returns cached values from shared memory if available
- Handles both initialized and uninitialized shared memory states
- Ensures WAL retention doesn't remove unsummarized WAL
- Updates shared memory with authoritative values when called by WAL summarizer
- Uses exclusive locking when updating shared memory state
- Location: src/backend/postmaster/walsummarizer.c:505-636

## Simplified Source

```c
// Simplified version of GetOldestUnsummarizedLSN
XLogRecPtr GetOldestUnsummarizedLSN(TimeLineID *tli, bool *lsn_is_exact) {
    TimeLineID latest_tli;
    XLogRecPtr unsummarized_lsn = InvalidXLogRecPtr;
    TimeLineID unsummarized_tli = 0;
    bool should_make_exact = false;
    bool am_wal_summarizer = AmWalSummarizerProcess();

    // Early exit: If WAL summarization is disabled
    if (!summarize_wal)
        return InvalidXLogRecPtr;

    // Non-summarizer processes: Try to use cached values from shared memory
    if (!am_wal_summarizer) {
        LWLockAcquire(WALSummarizerLock, LW_SHARED);
        if (WalSummarizerCtl->initialized) {
            // Return cached values if available
            unsummarized_lsn = WalSummarizerCtl->summarized_lsn;
            if (tli != NULL)
                *tli = WalSummarizerCtl->summarized_tli;
            if (lsn_is_exact != NULL)
                *lsn_is_exact = WalSummarizerCtl->lsn_is_exact;
            LWLockRelease(WALSummarizerLock);
            return unsummarized_lsn;
        }
        LWLockRelease(WALSummarizerLock);
    }

    // Find oldest available WAL: Examine timeline history
    GetLatestLSN(&latest_tli);
    List *tles = readTimeLineHistory(latest_tli);

    // Search backwards through timeline history for oldest WAL
    for (int n = list_length(tles) - 1; n >= 0; --n) {
        TimeLineHistoryEntry *tle = list_nth(tles, n);
        XLogSegNo oldest_segno = XLogGetOldestSegno(tle->tli);

        if (oldest_segno != 0) {
            // Convert segment number to LSN
            XLogSegNoOffsetToRecPtr(oldest_segno, 0, wal_segment_size, unsummarized_lsn);
            unsummarized_tli = tle->tli;
            break;
        }
    }

    // Skip already summarized WAL: Check existing summary files
    List *existing_summaries = GetWalSummaries(unsummarized_tli, InvalidXLogRecPtr, InvalidXLogRecPtr);
    ListCell *lc;
    foreach(lc, existing_summaries) {
        WalSummaryFile *ws = lfirst(lc);
        if (ws->end_lsn > unsummarized_lsn) {
            unsummarized_lsn = ws->end_lsn;
            should_make_exact = true;
        }
    }

    // Validation: Ensure we found some WAL
    if (unsummarized_tli == 0)
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                       errmsg_internal("no WAL found on timeline %u", latest_tli)));

    // Update shared memory: Store computed values
    LWLockAcquire(WALSummarizerLock, LW_EXCLUSIVE);
    if (am_wal_summarizer || !WalSummarizerCtl->initialized) {
        WalSummarizerCtl->initialized = true;
        WalSummarizerCtl->summarized_lsn = unsummarized_lsn;
        WalSummarizerCtl->summarized_tli = unsummarized_tli;
        WalSummarizerCtl->lsn_is_exact = should_make_exact;
        WalSummarizerCtl->pending_lsn = unsummarized_lsn;
    } else {
        unsummarized_lsn = WalSummarizerCtl->summarized_lsn;
    }

    // Set output parameters
    if (tli != NULL)
        *tli = WalSummarizerCtl->summarized_tli;
    if (lsn_is_exact != NULL)
        *lsn_is_exact = WalSummarizerCtl->lsn_is_exact;

    LWLockRelease(WALSummarizerLock);
    return unsummarized_lsn;
}
```

Key simplifications made:
- Simplified variable declarations and initialization
- Added clear section comments for major logic phases
- Consolidated timeline search logic into cleaner loop
- Streamlined shared memory access patterns
- Focused on the main execution path while preserving error handling
- Maintained all essential functionality and return value semantics