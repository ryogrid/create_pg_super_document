# GetFullPageWriteInfo

## Location
[src/backend/access/transam/xlog.c:6446-6460](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L6446-L6460)

## Overview
GetFullPageWriteInfo returns cached information needed to decide whether a modified block requires a full-page image in the WAL record.

## Definition

```c
void
GetFullPageWriteInfo(XLogRecPtr *RedoRecPtr_p, bool *doPageWrites_p)
```
## Detailed Description
GetFullPageWriteInfo provides cached backend-private copies of two critical pieces of information for WAL generation: the current Redo record pointer and the full-page write flag. This information helps determine whether modified data blocks need full-page images included in WAL records for crash recovery safety. The function returns cached values that may be out-of-date or uninitialized (InvalidXLogRecPtr and false respectively), which is acceptable because XLogInsertRecord will re-verify these values while holding the WAL insert lock before making the final decision.

## Parameters / Member Variables
- `*RedoRecPtr_p`: Output parameter pointer to receive the cached RedoRecPtr value
- `*doPageWrites_p`: Output parameter pointer to receive the cached doPageWrites flag
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

## Simplified Source

```c
// Simplified version of GetFullPageWriteInfo
void GetFullPageWriteInfo(XLogRecPtr *RedoRecPtr_p, bool *doPageWrites_p) {
    // Return cached backend-private values
    *RedoRecPtr_p = RedoRecPtr;
    *doPageWrites_p = doPageWrites;
}
```

Key simplifications made:
- Focused on the core function: return cached values for full-page write decisions
- Emphasized the performance optimization of using cached backend-private copies
- Preserved the essential contract: values may be stale but will be re-verified
- Maintained the simple getter interface for WAL insertion optimization