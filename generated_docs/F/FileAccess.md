# FileAccess

## Location
[src/backend/storage/file/fd.c:1489-1524](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L1489-L1524)

## Overview
FileAccess is a static function that ensures a virtual file descriptor is open and accessible, managing the LRU (Least Recently Used) cache to optimize file descriptor usage.

## Definition

```c
static int
FileAccess(File file)
```
## Detailed Description
FileAccess is a core function in PostgreSQL's virtual file descriptor management system that ensures a given file is open and ready for I/O operations. The function implements an LRU cache strategy to manage the limited number of available file descriptors efficiently.

The function operates in two main scenarios:
1. **File is not open**: Uses LruInsert() to open the file and place it at the head of the LRU ring, potentially closing the least recently used file if necessary to free up a file descriptor.
2. **File is open but not most recent**: Moves the file to the head of the LRU ring by removing it from its current position (Delete()) and inserting it at the head (Insert()).

If the file is already open and is the most recently used (at the head of the LRU ring), no action is taken.

## Parameters / Member Variables
- : The File (virtual file descriptor number) that needs to be accessed

## Return Value
- Returns 0 on success
- Returns -1 on re-open failure (with errno set by the underlying system call)

## Dependencies
- Functions called/Symbols referenced:
  - File (type definition for virtual file descriptor)
  - DO_DB (debug logging macro)
  - FileIsNotOpen (macro to check if file is not open)
  - [LruInsert](../L/LruInsert.md) (function to insert file at head of LRU ring)
  - [Delete](../D/Delete.md) (function to remove file from LRU ring)
  - [Insert](../I/Insert.md) (function to insert file at head of LRU ring)
- Called from (representative examples):
  - AllocateDesc
  - [FilePrefetch](FilePrefetch.md)
  - [FileWriteback](FileWriteback.md)
  - [FileReadV](FileReadV.md)
  - [FileWriteV](FileWriteV.md)
  - [FileSync](FileSync.md)
  - [FileZero](FileZero.md)
  - [FileFallocate](FileFallocate.md)
  - [FileSize](FileSize.md)
  - [FileTruncate](FileTruncate.md)

## Notes and Other Information
- This is a static function internal to fd.c, not exposed in the public API
- Essential for maintaining the LRU cache behavior in PostgreSQL's virtual file descriptor system
- The function may trigger closing of other files if the system is at the file descriptor limit
- Debug logging shows the file number and filename being accessed
- The LRU strategy helps optimize performance by keeping frequently accessed files open
- Failure typically occurs when the system cannot provide a file descriptor or file cannot be reopened

## Simplified Source

```c
static int
FileAccess(File file)
{
    // Debug logging: show which file is being accessed
    DO_DB(elog(LOG, "FileAccess %d (%s)", file, VfdCache[file].fileName));

    // Check if file needs to be opened
    if (FileIsNotOpen(file))
    {
        // Open file and place at head of LRU ring
        int result = LruInsert(file);
        if (result != 0)
            return result;  // Return error if open failed
    }
    else if (VfdCache[0].lruLessRecently != file)
    {
        // File is open but not most recent - move to LRU head
        Delete(file);   // Remove from current position
        Insert(file);   // Insert at head of LRU ring
    }
    // If file is already open and most recent, do nothing

    return 0;  // Success
}
```