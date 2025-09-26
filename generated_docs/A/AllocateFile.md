# AllocateFile

## Location
src/backend/storage/file/fd.c: 2580 - 2629

## Overview
AllocateFile is PostgreSQL's managed wrapper around the standard C library fopen() function, providing automatic file descriptor management and transaction-aware cleanup.

## Definition


## Detailed Description
AllocateFile serves as the primary interface for opening files using stdio (FILE*) within the PostgreSQL backend. Unlike direct fopen() calls, this function integrates with PostgreSQL's file descriptor management system to prevent resource exhaustion. It automatically handles closing of least-recently-used files when file descriptor limits are reached, and ensures all opened files are properly closed during transaction commit or abort to prevent file descriptor leakage.

The function is specifically designed for short-lived file operations, such as reading configuration files that will be immediately closed. Files intended to remain open for extended periods should not use this mechanism as they cannot share kernel file descriptors with other files, risking FD exhaustion.

## Parameters / Member Variables
- : The path to the file to be opened
- : The file opening mode string (same as standard fopen() modes: "r", "w", "a", etc.)

## Dependencies
- Functions called/Symbols referenced:
  - DO_DB (debug logging macro)
  - reserveAllocatedDesc (reserves an allocated descriptor slot)
  - ReleaseLruFiles (closes least-recently-used files to free FDs)
  - fopen (standard C library file opening function)
  - GetCurrentSubTransactionId (tracks which subtransaction opened the file)
  - ReleaseLruFile (releases a single LRU file when retrying after EMFILE/ENFILE)
- Called from (representative examples):
  - readTimeLineHistory (timeline.c:105)
  - BeginCopyFrom (copyfrom.c:1731)
  - parse_extension_control_file (extension.c:493)
  - open_auth_file (hba.c:617)
  - load_relcache_init_file (relcache.c:6095)

## Notes and Other Information
- This should be the only direct call to fopen() in the PostgreSQL backend
- Files opened with AllocateFile must be closed with FreeFile, not fclose()
- All files are automatically closed at transaction commit/abort for cleanup
- The function implements retry logic when encountering EMFILE/ENFILE errors
- Maximum allocated descriptors is controlled by maxAllocatedDescs parameter
- Each opened file is tracked in the allocatedDescs array with metadata including the creating subtransaction ID