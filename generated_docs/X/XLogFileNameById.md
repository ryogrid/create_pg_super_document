# XLogFileNameById

## Location
src/include/access/xlog_internal.h: 174 - 179

## Overview
XLogFileNameById generates a WAL segment file name directly from timeline ID, log file number, and segment number components.

## Definition


## Detailed Description
XLogFileNameById constructs a WAL segment file name by directly combining the provided timeline ID, log file number, and segment number into the standard PostgreSQL WAL filename format. Unlike XLogFileName which calculates the log and segment numbers from a logical segment number, this function takes the pre-calculated components directly. This provides a more direct approach when the log and segment numbers are already known.

## Parameters / Member Variables
- : Output buffer to store the generated filename (must be at least MAXFNAMELEN bytes)
- : Timeline ID that identifies the recovery timeline
- : Log file number component (32-bit unsigned integer)
- : Segment number component (32-bit unsigned integer)

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