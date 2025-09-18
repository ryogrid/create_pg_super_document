# sigchld_handler

## Location
src/bin/pg_basebackup/pg_basebackup.c: 297 - 307

## Overview
A signal handler function that detects premature termination of background child processes during pg_basebackup operations.

## Definition


## Detailed Description
This function serves as a SIGCHLD signal handler specifically designed to detect when a background child process (bgchild) terminates unexpectedly during a backup operation. When the background process exits prematurely, it raises a SIGCHLD signal which this handler catches and sets a flag to indicate the child has exited.

This allows pg_basebackup to abort processing immediately rather than waiting for the backup to complete and discovering the error later. The handler provides early failure detection which improves the user experience by avoiding unnecessary waiting time.

On Windows systems, this signal-based approach is not used; instead, a background thread communicates directly without needing signal handling mechanisms.

## Parameters / Member Variables
- : Standard PostgreSQL macro for signal handler arguments (typically includes signal number and context information)

The function operates on:
- : Global boolean flag set to true when the background child process exits

## Dependencies
- Functions called/Symbols referenced:
  - SIGNAL_ARGS (PostgreSQL signal handling macro)
- Called from (representative examples):
  - main (in pg_basebackup.c via signal() registration)

## Notes and Other Information
- This is a static function with internal linkage within pg_basebackup.c
- Only used on Unix-like systems; Windows uses a different approach with background threads
- The handler is minimal and async-signal-safe, only setting a boolean flag
- Part of the early failure detection mechanism in pg_basebackup
- Helps prevent wasted time waiting for backups that have already failed due to child process issues
- The bgchild typically handles tasks like WAL streaming or compression during backup operations