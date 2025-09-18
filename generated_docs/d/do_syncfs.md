# do_syncfs

## Location
src/common/file_utils.c: 62 - 96

## Overview
Performs a file system synchronization operation using the syncfs() system call to ensure all data for a given file system is written to storage.

## Definition


## Detailed Description
The  function is a low-level utility that synchronizes an entire file system by calling the Linux-specific  system call. It opens a file descriptor to the specified path, performs the synchronization, and then closes the file descriptor. This function is used as part of PostgreSQL's data directory synchronization process to ensure data durability during critical operations like startup or shutdown.

The function includes progress reporting and error handling, logging any failures to open files or perform synchronization operations. It uses transient file handles to avoid consuming limited file descriptor resources.

## Parameters / Member Variables
- : File system path to synchronize - typically points to a file or directory whose containing file system should be synchronized

## Dependencies
- Functions called/Symbols referenced:
  - ereport_startup_progress
  - OpenTransientFile
  - CloseTransientFile
  - syncfs (system call)
  - ereport
- Called from (representative examples):
  - SyncDataDirectory
  - [sync_pgdata](../s/sync_pgdata.md)
  - [sync_dir_recurse](../s/sync_dir_recurse.md)

## Notes and Other Information
- This function is Linux-specific due to its use of the  system call
- The function is declared as , making it internal to the fd.c file
- Progress reporting helps administrators monitor long-running synchronization operations
- Error conditions are logged but do not cause the function to fail fatally
- Uses transient file descriptors to avoid exhausting the system's file descriptor pool