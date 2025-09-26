# bbsink_progress_end_archive

## Location
[src/backend/backup/basebackup_progress.c:114-149](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_progress.c#L114-L149)

## Overview
A progress reporting function that updates backup progress when an archive (tablespace) streaming operation is completed during base backup.

## Definition
static void bbsink_progress_end_archive(bbsink *sink)

## Detailed Description
This static function handles progress reporting at the end of each archive streaming operation during a base backup. In PostgreSQL's backup architecture, each archive corresponds to a tablespace, so completing an archive means completing a tablespace. The function updates the count of streamed tablespaces in the progress reporting system, ensuring it doesn't exceed the total number of tablespaces (which can happen when WAL is included in the backup). After delegating to the next sink in the chain, it increments the tablespace counter in the shared state object.

## Parameters / Member Variables
- : The basebackup sink containing shared state information including tablespace numbers and counts

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_progress_update_param](../p/pgstat_progress_update_param.md) (progress parameter update function)
  - PROGRESS_BASEBACKUP_TBLSPC_STREAMED (progress parameter constant)
  - [bbsink_forward_end_archive](bbsink_forward_end_archive.md) (forwarding function)
  - [list_length](../l/list_length.md) (list utility function)
- Called from (representative examples):
  - This is a static function, typically called through function pointers in the bbsink operations structure

## Notes and Other Information
- This is a static function accessible only within the same source file
- Implements a guard to prevent the streamed tablespace count from exceeding the total tablespace count
- The guard is necessary because when WAL is included in the backup, the last tablespace may be marked complete before the last archive is complete
- Updates the shared bbsink_state's tablespace_num counter, which is shared across all bbsink objects
- The function is positioned as the outermost sink operation and performs state updates as the last operation
- Part of the progress tracking system that provides real-time feedback during PostgreSQL base backup operations