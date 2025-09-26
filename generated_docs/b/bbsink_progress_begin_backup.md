# bbsink_progress_begin_backup

## Location
[src/backend/backup/basebackup_progress.c:84-113](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_progress.c#L84-L113)

## Overview
A progress reporting function that updates backup progress information at the start of a base backup streaming phase.

## Definition
static void bbsink_progress_begin_backup(bbsink *sink)

## Detailed Description
This static function is responsible for updating progress information when the base backup begins streaming database files. It reports the current phase of the backup operation (streaming backup), updates the total backup size if known, and provides the total number of tablespaces involved in the backup. The function uses the PostgreSQL statistics system to update multiple progress parameters simultaneously and then delegates the actual backup operation to the next sink in the chain.

## Parameters / Member Variables
- : The basebackup sink containing state information including total bytes and tablespace list

## Dependencies
- Functions called/Symbols referenced:
  - PROGRESS_BASEBACKUP_PHASE (progress parameter constant)
  - PROGRESS_BASEBACKUP_BACKUP_TOTAL (progress parameter constant)
  - PROGRESS_BASEBACKUP_TBLSPC_TOTAL (progress parameter constant)
  - PROGRESS_BASEBACKUP_PHASE_STREAM_BACKUP (phase constant)
  - [pgstat_progress_update_multi_param](../p/pgstat_progress_update_multi_param.md) (progress update function)
  - [bbsink_forward_begin_backup](bbsink_forward_begin_backup.md) (forwarding function)
  - [list_length](../l/list_length.md) (list utility function)
- Called from (representative examples):
  - This is a static function, typically called through function pointers in the bbsink operations structure

## Notes and Other Information
- This is a static function, meaning it's only accessible within the same source file
- Updates three progress parameters simultaneously: phase, total backup size, and tablespace count
- If the total backup size is not yet determined (bytes_total_is_valid is false), it reports -1 which translates to NULL in progress reporting
- The function follows the sink pattern by forwarding the operation to the next sink in the chain
- Part of the progress tracking infrastructure that provides real-time feedback during PostgreSQL base backup operations