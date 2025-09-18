# GetOldestUnsummarizedLSN

## Location
src/backend/postmaster/walsummarizer.c: 505 - 636

## Overview
Determines the oldest LSN that has not yet been summarized and updates shared memory state accordingly.

## Definition


## Detailed Description
This function calculates the oldest Log Sequence Number (LSN) that still needs to be summarized by examining existing WAL files and summary files. It serves two main purposes: for the WAL summarizer process, it provides the starting point for summarization work; for other processes, it provides information for WAL retention decisions. The function examines the timeline history, finds the oldest available WAL segments, and accounts for existing summary files to determine where summarization should begin or resume.

The function handles initialization of shared memory state and provides different behaviors for the WAL summarizer process versus other processes. It ensures that existing summary files are not re-summarized and that WAL retention policies can make informed decisions.

## Parameters / Member Variables
- : Output parameter for the timeline ID corresponding to the returned LSN (optional, can be NULL)
- : Output parameter indicating whether the returned LSN is exact (start of WAL record) or approximate (start of WAL segment) (optional, can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - AmWalSummarizerProcess (checks if current process is WAL summarizer)
  - GetLatestLSN (gets current latest LSN and timeline)
  - readTimeLineHistory (reads timeline history for the latest timeline)
  - XLogGetOldestSegno (finds oldest WAL segment for a timeline)
  - XLogSegNoOffsetToRecPtr (converts segment number to LSN)
  - GetWalSummaries (retrieves existing summary files)
  - Various PostgreSQL list and memory management functions
- Called from (representative examples):
  - KeepLogSeg (in src/backend/access/transam/xlog.c:8004)
  - WalSummarizerMain (in src/backend/postmaster/walsummarizer.c:341)

## Notes and Other Information
- Returns InvalidXLogRecPtr if WAL summarization is disabled (summarize_wal = false)
- For non-summarizer processes, returns cached values from shared memory if available
- Handles both initialized and uninitialized shared memory states
- Ensures WAL retention doesn't remove unsummarized WAL
- Updates shared memory with authoritative values when called by WAL summarizer
- Uses exclusive locking when updating shared memory state
- Location: src/backend/postmaster/walsummarizer.c:505-636