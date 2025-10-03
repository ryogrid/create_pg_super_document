# LruInsert

## Location
[src/backend/storage/file/fd.c:1332-1378](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L1332-L1378)

## Overview
Inserts a file descriptor into the LRU cache, reopening the underlying file if necessary, and places it at the head of the LRU list.

## Definition

```c
static int
LruInsert(File file)
```
## Detailed Description
LruInsert is a higher-level function that manages both the file system and LRU cache aspects of file descriptor management. When called, it first checks if the underlying file is currently open. If the file is not open (FileIsNotOpen returns true), it attempts to reopen the file by:

1. First releasing some LRU files to free up kernel file descriptors via ReleaseLruFiles()
2. Attempting to reopen the file using BasicOpenFilePerm()
3. If the open fails due to system limits, it returns an error
4. If successful, it increments the global file counter (nfile)

After ensuring the file is open, it calls Insert() to place the file descriptor at the head of the LRU list, marking it as the most recently used.

This function is crucial for PostgreSQL's virtual file descriptor system, which allows the database to manage more logical file descriptors than the operating system limit by closing and reopening files as needed.

## Parameters / Member Variables
- `file`: The File descriptor index to insert into the LRU list and potentially reopen
## Dependencies
- Functions called/Symbols referenced:
  - Assert (debugging assertion)
  - DO_DB (debug macro for conditional logging)
  - elog (error/log reporting function)
  - VfdCache (global virtual file descriptor cache array)
  - FileIsNotOpen (macro to check if file is currently closed)
  - [ReleaseLruFiles](../R/ReleaseLruFiles.md) (function to close excess LRU files)
  - [BasicOpenFilePerm](../B/BasicOpenFilePerm.md) (low-level file opening function)
  - [Insert](../I/Insert.md) (function to insert file into LRU list)
  - nfile (global counter of open files)

- Called from (representative examples):
  - AllocateDesc (when allocating a new file descriptor)
  - [FileAccess](../F/FileAccess.md) (when accessing a file that may need reopening)

## Notes and Other Information
- Returns 0 on success, -1 on re-open failure (with errno set by BasicOpenFilePerm)
- This is a static function internal to the file descriptor management module
- The function handles the common case where PostgreSQL has more logical file descriptors than actual kernel file descriptors
- The two-phase approach (release files first, then try to open) helps manage system file descriptor limits
- Debug logging is conditional on DO_DB macro compilation
- Critical for PostgreSQL's ability to handle large numbers of concurrent file operations while respecting OS limits

## Simplified Source

```c
static int
LruInsert(File file)
{
    Vfd *vfdP = &VfdCache[file];

    // If file is not currently open, reopen it
    if (FileIsNotOpen(file)) {
        // Free up kernel file descriptors first
        ReleaseLruFiles();

        // Attempt to reopen the file
        vfdP->fd = BasicOpenFilePerm(vfdP->fileName, vfdP->fileFlags, vfdP->fileMode);
        if (vfdP->fd < 0) {
            return -1;  // Failed to reopen
        }
        ++nfile;  // Increment open file count
    }

    // Place file at head of LRU list (most recently used)
    Insert(file);

    return 0;  // Success
}
```