# basebackup_progress_wait_wal_archive

## Location
src/backend/backup/basebackup_progress.c: 206 - 228

## Overview
Updates the progress tracking to indicate that the backup process is waiting for WAL archiving to complete at the end of a base backup operation.

## Definition
```c
void basebackup_progress_wait_wal_archive(bbsink_state *state)
```

## Detailed Description
This function updates the base backup progress tracking system to indicate that the backup operation has entered the "wait for WAL archive" phase. This occurs at the end of the backup process when the system needs to wait for Write-Ahead Log (WAL) files to be archived before the backup can be considered complete. The function updates two progress parameters simultaneously: sets the phase to PROGRESS_BASEBACKUP_PHASE_WAIT_WAL_ARCHIVE and reports the total number of tablespaces that have been streamed. It considers all tablespaces as finished at this point since any remaining additions will be WAL files rather than actual tablespace content.

## Parameters / Member Variables
- `state`: Pointer to bbsink_state structure containing backup state information, particularly the list of tablespaces

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_progress_update_multi_param](../p/pgstat_progress_update_multi_param.md)
  - list_length
  - PROGRESS_BASEBACKUP_PHASE (parameter constant)
  - PROGRESS_BASEBACKUP_TBLSPC_STREAMED (parameter constant)  
  - PROGRESS_BASEBACKUP_PHASE_WAIT_WAL_ARCHIVE (phase constant)
- Called from (representative examples):
  - [perform_base_backup](../p/perform_base_backup.md)
  - bbsink_cleanup

## Notes and Other Information
- This function marks the transition to the final phase of base backup where WAL archiving completion is awaited
- Uses pgstat_progress_update_multi_param to efficiently update multiple progress parameters in a single call
- The comment explains the rationale for marking all tablespaces as complete even if the main tablespace archive is still open
- Located in src/backend/backup/basebackup_progress.c at lines 206-228
- Part of PostgreSQL's comprehensive progress tracking for backup operations