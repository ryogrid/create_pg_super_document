# bbsink_state

## Location
src/include/backup/basebackup_sink.h: 66 - 75

## Overview
A structure that maintains shared state information across all bbsink objects during a PostgreSQL base backup operation, tracking progress, tablespace information, and WAL positioning.

## Definition


## Detailed Description
The  structure serves as a centralized repository of backup state information that is shared among all bbsink objects participating in a single base backup operation. This structure enables coordination between different components of the backup pipeline by providing a common view of backup progress, tablespace processing status, and WAL stream positioning.

The structure must be initialized before starting a backup and remains valid for the entire backup duration. It provides both static information (like the list of tablespaces and backup start position) that is set once and never modified, as well as dynamic information (like current tablespace index and bytes processed) that is updated as the backup progresses.

## Parameters / Member Variables
- : List of tablespaceinfo objects representing all tablespaces to be backed up (set once, never modified)
- : Current index within the tablespaces list indicating which tablespace is being processed
- : Running count of bytes read so far from $PGDATA during the backup
- : Total estimated number of bytes present in $PGDATA (used for progress reporting)
- : Boolean flag indicating whether bytes_total contains a valid estimate
- : WAL stream position (XLogRecPtr) where the backup began (set once, never modified)
- : Timeline ID corresponding to the backup start position (set once, never modified)

## Dependencies
- Functions called/Symbols referenced:
  -  (PostgreSQL list structure for tablespaces)
  -  (WAL position type)
  -  (WAL timeline identifier type)
- Called from (representative examples):
  -  in src/backend/backup/basebackup.c:237
  -  in src/include/backup/basebackup_sink.h:175
  - Various bbsink implementations for progress tracking and state consultation
  -  in src/backend/backup/basebackup_progress.c:206

## Notes and Other Information
- Must be initialized by the caller before calling bbsink_begin_backup() and must persist for the entire backup lifetime
- Contains both immutable fields (tablespaces, startptr, starttli) set at backup start and mutable fields (tablespace_num, bytes_done, bytes_total) updated during backup progress
- Enables progress reporting by providing both current state (bytes_done) and total estimates (bytes_total)
- The same bbsink_state object is shared by all bbsink objects in a backup chain, ensuring consistent state visibility
- Critical for coordinating multi-tablespace backups where processing must track which tablespace is currently being handled
- WAL positioning information (startptr, starttli) is essential for backup consistency and recovery procedures