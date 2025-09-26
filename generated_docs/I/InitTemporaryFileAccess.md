# InitTemporaryFileAccess

## Location
src/backend/storage/file/fd.c: 930 - 960

## Overview
Initializes the temporary file access subsystem during backend startup and registers cleanup hooks to ensure proper temporary file cleanup during shutdown while statistics reporting is still available.

## Definition
void InitTemporaryFileAccess(void)

## Detailed Description
InitTemporaryFileAccess sets up PostgreSQL's temporary file handling system, which is kept separate from the main file access initialization to ensure proper coordination with the statistics reporting system. The function's primary responsibility is to register cleanup hooks that will be called during backend shutdown to clean up temporary files.

The separation from InitFileAccess() is crucial because:
1. Temporary file cleanup can trigger pgstat (PostgreSQL statistics) reporting
2. pgstat is shut down during before_shmem_exit() phase
3. Temporary file cleanup must occur before pgstat shutdown to allow proper reporting
4. Low-level file access needs to remain available longer than temporary file handling

The function registers BeforeShmemExit_Files as a before-shmem-exit hook, ensuring that all temporary files (including inter-transaction temporary files) are properly cleaned up during backend shutdown while statistics can still be reported.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - before_shmem_exit
  - BeforeShmemExit_Files
- Called from (representative examples):
  - BaseInit

## Notes and Other Information
- Must be called after InitFileAccess() - enforced by Assert(SizeVfdCache != 0)
- Can only be called once per backend - enforced by Assert(!temporary_files_allowed)
- Called during both normal and standalone backend startup, but NOT in the postmaster
- The cleanup hook (BeforeShmemExit_Files) calls CleanupTempFiles(false, true) to remove all temporary files
- Critical for preventing temporary file leaks during backend crashes or abnormal shutdowns
- The separate initialization allows for proper ordering with other shutdown procedures
- Uses USE_ASSERT_CHECKING to track whether temporary files are allowed to be created
- Essential for maintaining system cleanliness and preventing disk space issues from accumulated temporary files