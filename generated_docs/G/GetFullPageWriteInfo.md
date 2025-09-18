# GetFullPageWriteInfo

## Location
src/backend/access/transam/xlog.c: 6446 - 6460

## Overview
GetFullPageWriteInfo returns cached information needed to decide whether a modified block requires a full-page image in the WAL record.

## Definition


## Detailed Description
GetFullPageWriteInfo provides cached backend-private copies of two critical pieces of information for WAL generation: the current Redo record pointer and the full-page write flag. This information helps determine whether modified data blocks need full-page images included in WAL records for crash recovery safety. The function returns cached values that may be out-of-date or uninitialized (InvalidXLogRecPtr and false respectively), which is acceptable because XLogInsertRecord will re-verify these values while holding the WAL insert lock before making the final decision.

## Parameters / Member Variables
- : Output parameter pointer to receive the cached RedoRecPtr value
- : Output parameter pointer to receive the cached doPageWrites flag

## Dependencies
- Functions called/Symbols referenced:
  - RedoRecPtr (backend-private cached variable)
  - doPageWrites (backend-private cached variable)
- Called from (representative examples):
  - [XLogInsert](../X/XLogInsert.md)
  - [XLogCheckBufferNeedsBackup](../X/XLogCheckBufferNeedsBackup.md)
  - [WALAvailability](../W/WALAvailability.md)

## Notes and Other Information
- Returns cached values that may be stale or uninitialized for performance
- [XLogInsertRecord](../X/XLogInsertRecord.md) re-validates these values under WAL insert lock
- Critical for full-page write decision making in WAL generation
- [Backend](../B/Backend.md)-private cached values avoid expensive shared memory access during initial checks
- Values are refreshed by XLogInsertRecord when holding appropriate locks
- Located in src/backend/access/transam/xlog.c:6446-6460
- Part of the optimization strategy for WAL insertion performance
- Essential for crash recovery correctness through full-page image management