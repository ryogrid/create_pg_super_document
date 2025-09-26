# basebackup_progress_transfer_wal

## Location
[src/backend/backup/basebackup_progress.c:229-238](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_progress.c#L229-L238)

## Overview
Updates the progress tracking phase to indicate that the system is currently transferring WAL files into the final backup archive during a base backup operation.

## Definition
```c
void basebackup_progress_transfer_wal(void)
```

## Detailed Description
This function is a progress reporting utility used during PostgreSQL base backup operations to indicate that the backup process has entered the "transfer WAL" phase. It updates the PostgreSQL statistics progress tracking system by setting the phase parameter to PROGRESS_BASEBACKUP_PHASE_TRANSFER_WAL. This phase occurs when the system is actively transferring Write-Ahead Log (WAL) files into the final backup archive, which is typically one of the final steps in the base backup process before completion.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_progress_update_param](../p/pgstat_progress_update_param.md)
  - PROGRESS_BASEBACKUP_PHASE (parameter constant)
  - PROGRESS_BASEBACKUP_PHASE_TRANSFER_WAL (phase constant)
- Called from (representative examples):
  - [perform_base_backup](../p/perform_base_backup.md)
  - [bbsink_cleanup](bbsink_cleanup.md)

## Notes and Other Information
- This function is part of PostgreSQL's progress tracking infrastructure for base backup operations
- Provides visibility into the WAL transfer stage, which is crucial for understanding backup timing and progress
- Located in src/backend/backup/basebackup_progress.c at lines 229-238
- Simple wrapper function that standardizes progress reporting for the WAL transfer phase
- Follows the same pattern as other progress reporting functions in the basebackup progress module