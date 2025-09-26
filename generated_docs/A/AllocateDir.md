# AllocateDir

## Location
src/backend/storage/file/fd.c: 2840 - 2905

## Overview
AllocateDir opens a directory for reading using opendir() while being managed by PostgreSQL's file descriptor management system to prevent resource exhaustion and ensure proper cleanup after errors.

## Definition


## Detailed Description
AllocateDir is the managed equivalent of opendir() that integrates with PostgreSQL's file descriptor tracking and resource management system. It attempts to open a directory for reading while ensuring that the system doesn't exceed its configured limit of allocated descriptors (maxAllocatedDescs). The function automatically handles situations where file descriptors are exhausted by releasing least-recently-used files and retrying the operation.

If successful, the function registers the directory handle in the allocatedDescs array with the current subtransaction ID, enabling automatic cleanup if the transaction aborts. The function is designed to be the exclusive method for opening directories in the PostgreSQL backend, replacing direct calls to opendir().

## Parameters / Member Variables
- : The path to the directory to open for reading

## Dependencies
- Functions called/Symbols referenced:
  - DO_DB (debug logging macro)
  - reserveAllocatedDesc (checks if another descriptor can be allocated)
  - ReleaseLruFiles (releases least recently used files to free descriptors)
  - opendir (system call to open directory)
  - AllocateDesc (descriptor structure type)
  - AllocateDescDir (enum value for directory descriptor type)
  - GetCurrentSubTransactionId (gets current subtransaction ID for cleanup)
  - ReleaseLruFile (releases one LRU file and retries)
  - ereport/elog (PostgreSQL error reporting)
- Called from (representative examples):
  - SlruScanDirectory
  - XLogGetOldestSegno
  - RemoveOldXlogFiles
  - perform_base_backup
  - sendDir
  - movedb
  - copydir
  - RemovePgTempFiles
  - SyncDataDirectory
  - pg_ls_dir

## Notes and Other Information
- Returns NULL with errno set on failure, though failure detection is commonly left to subsequent ReadDir/ReadDirExtended calls
- Automatically retries if opendir() fails with EMFILE or ENFILE (too many open files) after releasing an LRU file
- Integrates with PostgreSQL's transaction system - directory handles are automatically closed on transaction abort
- Should be paired with FreeDir() rather than direct closedir() calls for proper resource management
- This should ideally be the only direct call to opendir() in the PostgreSQL backend
- Part of PostgreSQL's comprehensive file descriptor management strategy to prevent resource leaks
- Used extensively for directory traversal operations in WAL management, backup processes, extension handling, and database maintenance