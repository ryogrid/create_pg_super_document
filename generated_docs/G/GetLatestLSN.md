# GetLatestLSN

## Location
src/backend/postmaster/walsummarizer.c: 800 - 857

## Overview
Determines the latest LSN that is eligible to be summarized by the WAL summarizer, handling both normal operation and recovery scenarios.

## Definition


## Detailed Description
This static function determines the highest LSN that the WAL summarizer can safely process, considering the current state of the database (normal operation vs. recovery). The function must ensure that it only summarizes WAL that has been safely flushed to disk and is stable.

During normal operation, it returns the flush position. During recovery, the logic is more complex because it must handle various recovery scenarios including crash recovery, streaming replication, and archive recovery. The function implements special handling for the transition period when recovery is ending but the database hasn't yet been marked as in production.

## Parameters / Member Variables
- : Output parameter - pointer to TimeLineID that will be set to the timeline corresponding to the returned LSN

## Dependencies
- Functions called/Symbols referenced:
  - RecoveryInProgress
  - GetFlushRecPtr
  - GetWALInsertionTimeLineIfSet
  - GetXLogReplayRecPtr
  - GetWalRcvFlushRecPtr
- Called from (representative examples):
  - WalSummarizerMain
  - GetOldestUnsummarizedLSN
  - summarizer_read_local_xlog_page

## Notes and Other Information
- This is a static function, only accessible within walsummarizer.c
- Returns the LSN and sets the corresponding timeline ID via output parameter
- During recovery, chooses the more advanced position between WAL receiver flush position and replay position
- Handles the edge case where recovery has effectively ended but RecoveryInProgress() still returns true
- Critical for ensuring WAL summarization doesn't process unstable or unflushed WAL data
- The function logic ensures that summarization only proceeds on WAL that is guaranteed to be persistent