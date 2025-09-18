# CreateDataDirLockFile

## Location
src/backend/utils/init/miscinit.c: 1510 - 1518

## Overview
Creates a lock file in the PostgreSQL data directory to prevent multiple server instances from accessing the same data directory simultaneously.

## Definition


## Detailed Description
This function creates the data directory lock file using the standard DIRECTORY_LOCK_FILE name (typically "postmaster.pid"). It serves as a critical safety mechanism to prevent data corruption by ensuring only one PostgreSQL instance can access a data directory at a time. The function assumes the working directory has already been switched to the DataDir, allowing it to use a relative path for enhanced security. Initially, the socket directory path line in the lock file is written as empty, which will later be updated by postmaster.c when the first Unix socket is created.

## Parameters / Member Variables
- `amPostmaster`: Boolean flag indicating whether the calling process is the postmaster (true) or another process type like single-user mode (false)

## Dependencies
- Functions called/Symbols referenced:
  - CreateLockFile
  - DIRECTORY_LOCK_FILE (constant)
- Called from (representative examples):
  - BootstrapModeMain
  - PostmasterMain
  - PostgresSingleUserMain

## Notes and Other Information
- Must be called after switching working directory to DataDir for security
- The socket directory path line is initially empty and updated later by postmaster.c
- Critical for preventing multiple PostgreSQL instances from corrupting shared data
- Used in bootstrap mode, normal postmaster startup, and single-user mode