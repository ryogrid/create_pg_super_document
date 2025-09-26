# basebackup_progress_estimate_backup_size

## Location
[src/backend/backup/basebackup_progress.c:196-205](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_progress.c#L196-L205)

## Overview
Updates the progress tracking phase to indicate that the system is currently estimating the backup size during a base backup operation.

## Definition

```c
void
basebackup_progress_estimate_backup_size(void)
```
## Detailed Description
This function is a simple progress reporting utility used during PostgreSQL base backup operations. It updates the progress tracking system to indicate that the backup process is currently in the "estimate backup size" phase. The function calls the PostgreSQL statistics system to update the progress parameter, setting the phase to . This allows monitoring tools and users to track the current stage of the backup operation.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_progress_update_param](../p/pgstat_progress_update_param.md)
  - PROGRESS_BASEBACKUP_PHASE (parameter constant)
  - PROGRESS_BASEBACKUP_PHASE_ESTIMATE_BACKUP_SIZE (phase constant)
- Called from (representative examples):
  - [perform_base_backup](../p/perform_base_backup.md)
  - [bbsink_cleanup](bbsink_cleanup.md)

## Notes and Other Information
- This is part of PostgreSQL's progress tracking infrastructure for base backup operations
- The function provides visibility into the backup process stages for monitoring and debugging purposes
- Located in src/backend/backup/basebackup_progress.c at lines 196-205
- Simple wrapper function that standardizes progress reporting across the backup subsystem