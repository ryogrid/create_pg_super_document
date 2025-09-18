# basebackup_progress_done

## Location
src/backend/backup/basebackup_progress.c: 239 - 242

## Overview
Signals the end of a base backup operation by terminating progress tracking for the backup command.

## Definition
```c
void basebackup_progress_done(void)
```

## Detailed Description
This function marks the completion of a PostgreSQL base backup operation by calling pgstat_progress_end_command() to end the progress tracking for the backup command. It serves as the final cleanup step in the backup progress reporting system, indicating to the PostgreSQL statistics system that the backup operation is no longer active. This allows monitoring tools and the system to recognize that the backup has completed and clears any associated progress tracking state.

## Parameters / Member Variables
- None (void function with no parameters)

## Dependencies
- Functions called/Symbols referenced:
  - pgstat_progress_end_command
- Called from (representative examples):
  - perform_base_backup
  - bbsink_cleanup

## Notes and Other Information
- This function should always be called when a base backup operation completes, regardless of success or failure
- Essential for proper cleanup of PostgreSQL's progress tracking infrastructure
- Located in src/backend/backup/basebackup_progress.c at lines 239-242  
- Simple wrapper function that provides a clean interface to end backup progress tracking
- Complements the other progress functions by handling the termination phase
- Ensures that progress tracking resources are properly released after backup completion