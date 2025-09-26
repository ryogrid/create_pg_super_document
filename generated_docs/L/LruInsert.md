# LruInsert

## Location
src/backend/storage/file/fd.c: 1332 - 1378

## Overview
Inserts a file descriptor into the LRU cache, reopening the underlying file if necessary, and places it at the head of the LRU list.

## Definition


## Detailed Description
LruInsert is a higher-level function that manages both the file system and LRU cache aspects of file descriptor management. When called, it first checks if the underlying file is currently open. If the file is not open (FileIsNotOpen returns true), it attempts to reopen the file by:

1. First releasing some LRU files to free up kernel file descriptors via ReleaseLruFiles()
2. Attempting to reopen the file using BasicOpenFilePerm()
3. If the open fails due to system limits, it returns an error
4. If successful, it increments the global file counter (nfile)

After ensuring the file is open, it calls Insert() to place the file descriptor at the head of the LRU list, marking it as the most recently used.

This function is crucial for PostgreSQL's virtual file descriptor system, which allows the database to manage more logical file descriptors than the operating system limit by closing and reopening files as needed.

## Parameters / Member Variables
- : The File descriptor index to insert into the LRU list and potentially reopen

## Dependencies
- Functions called/Symbols referenced:
  - Assert (debugging assertion)
  - DO_DB (debug macro for conditional logging)
  - elog (error/log reporting function)
  - VfdCache (global virtual file descriptor cache array)
  - FileIsNotOpen (macro to check if file is currently closed)
  - ReleaseLruFiles (function to close excess LRU files)
  - BasicOpenFilePerm (low-level file opening function)
  - Insert (function to insert file into LRU list)
  - nfile (global counter of open files)

- Called from (representative examples):
  - AllocateDesc (when allocating a new file descriptor)
  - FileAccess (when accessing a file that may need reopening)

## Notes and Other Information
- Returns 0 on success, -1 on re-open failure (with errno set by BasicOpenFilePerm)
- This is a static function internal to the file descriptor management module
- The function handles the common case where PostgreSQL has more logical file descriptors than actual kernel file descriptors
- The two-phase approach (release files first, then try to open) helps manage system file descriptor limits
- Debug logging is conditional on DO_DB macro compilation
- Critical for PostgreSQL's ability to handle large numbers of concurrent file operations while respecting OS limits