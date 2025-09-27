# AllocateDir

## Location
[src/backend/storage/file/fd.c:2840-2905](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L2840-L2905)

## Overview
AllocateDir opens a directory for reading using opendir() while being managed by PostgreSQL's file descriptor management system to prevent resource exhaustion and ensure proper cleanup after errors.

## Definition

```c
DIR *
AllocateDir(const char *dirname)
```
## Detailed Description
AllocateDir is the managed equivalent of opendir() that integrates with PostgreSQL's file descriptor tracking and resource management system. It attempts to open a directory for reading while ensuring that the system doesn't exceed its configured limit of allocated descriptors (maxAllocatedDescs). The function automatically handles situations where file descriptors are exhausted by releasing least-recently-used files and retrying the operation.

If successful, the function registers the directory handle in the allocatedDescs array with the current subtransaction ID, enabling automatic cleanup if the transaction aborts. The function is designed to be the exclusive method for opening directories in the PostgreSQL backend, replacing direct calls to opendir().

## Parameters / Member Variables
- : The path to the directory to open for reading

## Dependencies
- Functions called/Symbols referenced:
  - DO_DB (debug logging macro)
  - [reserveAllocatedDesc](../r/reserveAllocatedDesc.md) (checks if another descriptor can be allocated)
  - [ReleaseLruFiles](../R/ReleaseLruFiles.md) (releases least recently used files to free descriptors)
  - [opendir](../o/opendir.md) (system call to open directory)
  - AllocateDesc (descriptor structure type)
  - AllocateDescDir (enum value for directory descriptor type)
  - [GetCurrentSubTransactionId](../G/GetCurrentSubTransactionId.md) (gets current subtransaction ID for cleanup)
  - [ReleaseLruFile](../R/ReleaseLruFile.md) (releases one LRU file and retries)
  - ereport/elog (PostgreSQL error reporting)
- Called from (representative examples):
  - [SlruScanDirectory](../S/SlruScanDirectory.md)
  - [XLogGetOldestSegno](../X/XLogGetOldestSegno.md)
  - [RemoveOldXlogFiles](../R/RemoveOldXlogFiles.md)
  - [perform_base_backup](../p/perform_base_backup.md)
  - [sendDir](../s/sendDir.md)
  - [movedb](../m/movedb.md)
  - [copydir](../c/copydir.md)
  - [RemovePgTempFiles](../R/RemovePgTempFiles.md)
  - [SyncDataDirectory](../S/SyncDataDirectory.md)
  - [pg_ls_dir](../p/pg_ls_dir.md)

## Notes and Other Information
- Returns NULL with errno set on failure, though failure detection is commonly left to subsequent ReadDir/ReadDirExtended calls
- Automatically retries if opendir() fails with EMFILE or ENFILE (too many open files) after releasing an LRU file
- Integrates with PostgreSQL's transaction system - directory handles are automatically closed on transaction abort
- Should be paired with FreeDir() rather than direct closedir() calls for proper resource management
- This should ideally be the only direct call to opendir() in the PostgreSQL backend
- Part of PostgreSQL's comprehensive file descriptor management strategy to prevent resource leaks
- Used extensively for directory traversal operations in WAL management, backup processes, extension handling, and database maintenance

## Simplified Source

```c
// Simplified version of AllocateDir
DIR *AllocateDir(const char *dirname) {
    // Step 1: Check if we can allocate another descriptor
    if (!reserveAllocatedDesc()) {
        ereport(ERROR, "exceeded maxAllocatedDescs while trying to open directory");
    }

    // Step 2: Release excess file descriptors to make room
    ReleaseLruFiles();

TryAgain:
    // Step 3: Attempt to open the directory
    DIR *dir = opendir(dirname);
    if (dir != NULL) {
        // Step 4: Register the directory handle for management
        AllocateDesc *desc = &allocatedDescs[numAllocatedDescs];
        desc->kind = AllocateDescDir;
        desc->desc.dir = dir;
        desc->create_subid = GetCurrentSubTransactionId();
        numAllocatedDescs++;
        return dir;
    }

    // Step 5: Handle "too many open files" errors by releasing one file and retrying
    if (errno == EMFILE || errno == ENFILE) {
        int save_errno = errno;
        ereport(LOG, "out of file descriptors: release and retry");
        errno = 0;
        if (ReleaseLruFile()) {
            goto TryAgain;
        }
        errno = save_errno;
    }

    // Step 6: Return failure
    return NULL;
}
```

Key simplifications made:
- Removed debug logging for clarity
- Simplified error reporting messages
- Abstracted complex error code handling
- Added step-by-step comments explaining the algorithm
- Focused on the main execution path
- Consolidated variable declarations with usage