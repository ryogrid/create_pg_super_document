# XLogFileNameById

## Location
[src/include/access/xlog_internal.h:174-179](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/xlog_internal.h#L174-L179)

## Overview
XLogFileNameById generates a WAL segment file name directly from timeline ID, log file number, and segment number components.

## Definition

```c
static inline void
XLogFileNameById(char *fname, TimeLineID tli, uint32 log, uint32 seg)
```
## Detailed Description
XLogFileNameById constructs a WAL segment file name by directly combining the provided timeline ID, log file number, and segment number into the standard PostgreSQL WAL filename format. Unlike XLogFileName which calculates the log and segment numbers from a logical segment number, this function takes the pre-calculated components directly. This provides a more direct approach when the log and segment numbers are already known.

## Parameters / Member Variables
- `*fname`: Output buffer to store the generated filename (must be at least MAXFNAMELEN bytes)
- `tli`: Timeline ID that identifies the recovery timeline
- `log`: Log file number component (32-bit unsigned integer)
- `seg`: Segment number component (32-bit unsigned integer)
## Dependencies
- Functions called/Symbols referenced:
  - MAXFNAMELEN
  - snprintf (standard C library)
- Called from (representative examples):
  - [SetWALFileNameForCleanup](../S/SetWALFileNameForCleanup.md) (in pg_archivecleanup)

## Notes and Other Information
- This is an inline function defined in xlog_internal.h for performance
- More direct than XLogFileName when log and segment numbers are pre-calculated
- Primarily used in archive cleanup utilities where specific log/segment combinations need to be processed
- The generated filename follows the same format as XLogFileName: TTTTTTTTFFFFFFFFSSSSSSSS