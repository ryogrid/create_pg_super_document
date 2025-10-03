# AllocateFile

## Location
[src/backend/storage/file/fd.c:2580-2629](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L2580-L2629)

## Overview
AllocateFile is PostgreSQL's managed wrapper around the standard C library fopen() function, providing automatic file descriptor management and transaction-aware cleanup.

## Definition

```c
FILE *
AllocateFile(const char *name, const char *mode)
```
## Detailed Description
AllocateFile serves as the primary interface for opening files using stdio (FILE*) within the PostgreSQL backend. Unlike direct fopen() calls, this function integrates with PostgreSQL's file descriptor management system to prevent resource exhaustion. It automatically handles closing of least-recently-used files when file descriptor limits are reached, and ensures all opened files are properly closed during transaction commit or abort to prevent file descriptor leakage.

The function is specifically designed for short-lived file operations, such as reading configuration files that will be immediately closed. Files intended to remain open for extended periods should not use this mechanism as they cannot share kernel file descriptors with other files, risking FD exhaustion.

## Parameters / Member Variables
- `*name`: The path to the file to be opened
- `*mode`: The file opening mode string (same as standard fopen() modes: "r", "w", "a", etc.)
## Dependencies
- Functions called/Symbols referenced:
  - DO_DB (debug logging macro)
  - [reserveAllocatedDesc](../r/reserveAllocatedDesc.md) (reserves an allocated descriptor slot)
  - [ReleaseLruFiles](../R/ReleaseLruFiles.md) (closes least-recently-used files to free FDs)
  - fopen (standard C library file opening function)
  - [GetCurrentSubTransactionId](../G/GetCurrentSubTransactionId.md) (tracks which subtransaction opened the file)
  - [ReleaseLruFile](../R/ReleaseLruFile.md) (releases a single LRU file when retrying after EMFILE/ENFILE)
- Called from (representative examples):
  - [readTimeLineHistory](../r/readTimeLineHistory.md) (timeline.c:105)
  - [BeginCopyFrom](../B/BeginCopyFrom.md) (copyfrom.c:1731)
  - [parse_extension_control_file](../p/parse_extension_control_file.md) (extension.c:493)
  - [open_auth_file](../o/open_auth_file.md) (hba.c:617)
  - [load_relcache_init_file](../l/load_relcache_init_file.md) (relcache.c:6095)

## Notes and Other Information
- This should be the only direct call to fopen() in the PostgreSQL backend
- Files opened with AllocateFile must be closed with FreeFile, not fclose()
- All files are automatically closed at transaction commit/abort for cleanup
- The function implements retry logic when encountering EMFILE/ENFILE errors
- Maximum allocated descriptors is controlled by maxAllocatedDescs parameter
- Each opened file is tracked in the allocatedDescs array with metadata including the creating subtransaction ID

## Simplified Source

```c
// Simplified version of AllocateFile
FILE *AllocateFile(const char *name, const char *mode) {
    FILE *file;

    // Debug logging
    DO_DB(elog(LOG, "AllocateFile: Allocated %d (%s)", numAllocatedDescs, name));

    // Check if we can allocate another file descriptor
    if (!reserveAllocatedDesc()) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                       errmsg("exceeded maxAllocatedDescs (%d) while trying to open file \"%s\"",
                              maxAllocatedDescs, name)));
    }

    // Close excess kernel file descriptors
    ReleaseLruFiles();

TryAgain:
    // Attempt to open the file
    if ((file = fopen(name, mode)) != NULL) {
        // Success: Track the opened file in our descriptor array
        AllocateDesc *desc = &allocatedDescs[numAllocatedDescs];
        desc->kind = AllocateDescFile;
        desc->desc.file = file;
        desc->create_subid = GetCurrentSubTransactionId();
        numAllocatedDescs++;
        return file;
    }

    // Handle file descriptor exhaustion
    if (errno == EMFILE || errno == ENFILE) {
        int save_errno = errno;
        ereport(LOG, (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                     errmsg("out of file descriptors: %m; release and retry")));
        errno = 0;
        if (ReleaseLruFile()) {
            goto TryAgain;  // Retry after freeing a file descriptor
        }
        errno = save_errno;
    }

    return NULL;  // Failed to open file
}
```

Key simplifications made:
- Preserved the core logic flow: validate → free resources → open → track or retry
- Kept essential error handling for resource exhaustion
- Maintained the retry mechanism for FD limits
- Simplified comments to explain each major step
- Focused on the main execution path while preserving correctness