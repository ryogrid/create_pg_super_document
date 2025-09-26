# ReleaseLruFile

## Location
src/backend/storage/file/fd.c: 1379 - 1400

## Overview
Releases one kernel file descriptor by closing the least-recently-used virtual file descriptor (VFD) from the LRU cache.

## Definition


## Detailed Description
ReleaseLruFile implements the core LRU eviction policy for PostgreSQL's virtual file descriptor management system. When the system needs to free up a kernel file descriptor (typically because it's approaching the OS limit), this function identifies and closes the least recently used file.

The function works by:
1. Checking if there are any currently open files (nfile > 0)
2. If files are open, it finds the least recently used file by looking at VfdCache[0].lruMoreRecently (the sentinel node's next pointer points to the LRU end)
3. Calls LruDelete() to remove and close that file descriptor
4. Returns true if a file was successfully released, false if no files were available to close

This function is essential for PostgreSQL's ability to manage more logical file descriptors than the operating system allows by dynamically closing and reopening files based on usage patterns.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - DO_DB (debug macro for conditional logging)
  - elog (error/log reporting function)
  - nfile (global counter of currently open files)
  - Assert (debugging assertion)
  - VfdCache (global virtual file descriptor cache array)
  - LruDelete (function to delete a file from LRU list and close it)

- Called from (representative examples):
  - AllocateDesc (when allocating descriptors and need to free space)
  - BasicOpenFilePerm (when opening files and hitting descriptor limits)
  - ReleaseLruFiles (when releasing multiple files)
  - AllocateFile (when allocating FILE* structures)
  - OpenPipeStream (when opening pipe streams)
  - AllocateDir (when allocating directory handles)

## Notes and Other Information
- Returns true if a file was successfully released, false if no files were available to free
- This is a static function internal to the file descriptor management module
- The function maintains the LRU invariant by always closing the least recently used file
- Critical for preventing file descriptor exhaustion in PostgreSQL
- Debug logging is conditional on DO_DB macro compilation
- The assertion ensures consistency - if nfile > 0, there must be at least one file in the LRU ring