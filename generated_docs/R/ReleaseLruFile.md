# ReleaseLruFile

## Location
[src/backend/storage/file/fd.c:1379-1400](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L1379-L1400)

## Overview
Releases one kernel file descriptor by closing the least-recently-used virtual file descriptor (VFD) from the LRU cache.

## Definition

```c
static bool
ReleaseLruFile(void)
```
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
  - [LruDelete](../L/LruDelete.md) (function to delete a file from LRU list and close it)

- Called from (representative examples):
  - AllocateDesc (when allocating descriptors and need to free space)
  - [BasicOpenFilePerm](../B/BasicOpenFilePerm.md) (when opening files and hitting descriptor limits)
  - [ReleaseLruFiles](ReleaseLruFiles.md) (when releasing multiple files)
  - [AllocateFile](../A/AllocateFile.md) (when allocating FILE* structures)
  - [OpenPipeStream](../O/OpenPipeStream.md) (when opening pipe streams)
  - [AllocateDir](../A/AllocateDir.md) (when allocating directory handles)

## Notes and Other Information
- Returns true if a file was successfully released, false if no files were available to free
- This is a static function internal to the file descriptor management module
- The function maintains the LRU invariant by always closing the least recently used file
- Critical for preventing file descriptor exhaustion in PostgreSQL
- Debug logging is conditional on DO_DB macro compilation
- The assertion ensures consistency - if nfile > 0, there must be at least one file in the LRU ring

## Simplified Source

```c
// Simplified version of ReleaseLruFile
static bool ReleaseLruFile(void) {
    // Log current state for debugging
    DO_DB(elog(LOG, "ReleaseLruFile. Opened %d", nfile));

    // Check if any files are currently open
    if (nfile > 0) {
        // Ensure LRU ring has at least one file (consistency check)
        Assert(VfdCache[0].lruMoreRecently != 0);

        // Remove and close the least recently used file
        LruDelete(VfdCache[0].lruMoreRecently);

        return true;  // Successfully freed a file descriptor
    }

    return false;  // No files available to close
}
```

Key simplifications made:
- Preserved the core LRU eviction logic
- Kept essential error checking (Assert)
- Added descriptive comments for each logical step
- Maintained the simple control flow structure
- Focused on the main execution path without low-level implementation details