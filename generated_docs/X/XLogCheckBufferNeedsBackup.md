# XLogCheckBufferNeedsBackup

## Location
src/backend/access/transam/xloginsert.c: 1027 - 1064

## Overview
XLogCheckBufferNeedsBackup determines whether a buffer requires a full-page backup in the WAL record for crash recovery safety.

## Definition


## Detailed Description
XLogCheckBufferNeedsBackup evaluates whether a given buffer needs to be included as a full-page image in a WAL record. The function checks if full-page writes are currently enabled and compares the page's LSN with the current Redo recovery pointer. If the page's LSN is at or before the Redo pointer and full-page writes are enabled, the buffer requires backup to ensure crash recovery can reconstruct the page state. Since this check occurs before acquiring the WAL insertion lock, the result should only be used for optimization purposes as the full-page write settings could change.

## Parameters / Member Variables
- : The buffer to check for backup requirements

## Dependencies
- Functions called/Symbols referenced:
  - GetFullPageWriteInfo (gets current full-page write settings and redo pointer)
  - BufferGetPage (extracts page from buffer)
  - PageGetLSN (gets the page's log sequence number)
- Called from:
  - log_heap_update (in heap operations)
  - heap_page_prune_and_freeze (during page pruning)
  - Various WAL logging functions

## Notes and Other Information
- Result is for optimization only since full-page write settings can change
- Returns true only when both full-page writes are enabled and page LSN <= Redo pointer
- Critical for crash recovery correctness by ensuring modified pages can be reconstructed
- Check performed before acquiring WAL insertion lock for performance