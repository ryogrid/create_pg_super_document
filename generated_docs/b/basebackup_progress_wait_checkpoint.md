# basebackup_progress_wait_checkpoint

## Location
[src/backend/backup/basebackup_progress.c:186-195](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_progress.c#L186-L195)

## Overview  
A progress reporting function that advertises that the base backup process is waiting for the start-of-backup checkpoint to complete.

## Definition
void basebackup_progress_wait_checkpoint(void)

## Detailed Description
This function is a simple progress reporting utility that updates the base backup phase to indicate that the system is currently waiting for the start-of-backup checkpoint operation to complete. In PostgreSQL's base backup process, a checkpoint must be performed at the beginning to ensure data consistency. This function communicates this waiting state to the progress reporting system, allowing users and monitoring tools to understand that the backup is in the checkpoint waiting phase rather than actively transferring data.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_progress_update_param](../p/pgstat_progress_update_param.md) (progress parameter update function)
  - PROGRESS_BASEBACKUP_PHASE (progress phase parameter constant)
  - PROGRESS_BASEBACKUP_PHASE_WAIT_CHECKPOINT (checkpoint waiting phase constant)
- Called from (representative examples):
  - [perform_base_backup](../p/perform_base_backup.md) (in basebackup.c at line 266)
  - [bbsink_cleanup](bbsink_cleanup.md) (referenced in basebackup_sink.h at line 295)

## Notes and Other Information
- This is a public function (not static) and can be called from other source files
- Part of PostgreSQL's base backup progress reporting infrastructure
- The checkpoint waiting phase is a critical early step in the base backup process that ensures data consistency
- Provides visibility into backup operations for monitoring and user feedback purposes
- Simple single-purpose function that only updates the phase indicator without additional logic
- Essential for understanding backup timing, as checkpoint operations can take significant time depending on system load and data size