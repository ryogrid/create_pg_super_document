# bbsink_progress_archive_contents

## Location
src/backend/backup/basebackup_progress.c: 150 - 185

## Overview
A progress tracking function that updates backup progress counters when new archive content data is processed during base backup operations.

## Definition
static void bbsink_progress_archive_contents(bbsink *sink, size_t len)

## Detailed Description
This static function handles progress tracking for incoming archive content data during a base backup. It increments the running counter of bytes processed, forwards the data to the next sink in the chain, and updates the PostgreSQL progress reporting system. The function implements intelligent total size adjustment to prevent progress percentages from exceeding 100% - if the actual data processed exceeds the estimated total, it updates the total to match the current progress. This accommodation is necessary because backup size estimates can be inaccurate, especially when WAL (Write-Ahead Logging) data is included in the backup.

## Parameters / Member Variables
- : The basebackup sink containing state information for tracking progress
- : The number of bytes of new archive content data being processed

## Dependencies
- Functions called/Symbols referenced:
  - bbsink_state (state structure type)
  - PROGRESS_BASEBACKUP_BACKUP_STREAMED (progress parameter constant)
  - PROGRESS_BASEBACKUP_BACKUP_TOTAL (progress parameter constant)  
  - [bbsink_forward_archive_contents](bbsink_forward_archive_contents.md) (forwarding function)
  - [pgstat_progress_update_multi_param](../p/pgstat_progress_update_multi_param.md) (progress update function)
- Called from (representative examples):
  - This is a static function, typically called through function pointers in the bbsink operations structure

## Notes and Other Information
- This is a static function accessible only within the same source file
- Updates the shared state's bytes_done counter with the processed data length
- Implements dynamic total size adjustment to prevent progress overflow past 100%
- The total size may change during backup execution when estimates prove inaccurate
- Particularly important when WAL is included in backups, as this can significantly affect size estimates
- Uses variable parameter count (nparam) to conditionally update either one or two progress parameters
- Part of the real-time progress reporting infrastructure for PostgreSQL base backup operations