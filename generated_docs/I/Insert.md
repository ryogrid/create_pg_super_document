# Insert

## Location
src/backend/storage/file/fd.c: 1310 - 1331

## Overview
Inserts a file descriptor into the LRU (Least Recently Used) cache list, placing it at the most recently used position.

## Definition

```c
static void
Insert(File file)
```
## Detailed Description
The Insert function manages the LRU cache for virtual file descriptors (Vfd) in PostgreSQL's file management system. When a file descriptor is accessed or newly opened, this function inserts it at the head of the LRU list, marking it as the most recently used file. The function maintains the doubly-linked LRU list structure by updating the lruMoreRecently and lruLessRecently pointers in the VfdCache array.

The function uses a circular doubly-linked list where VfdCache[0] serves as the sentinel/head node. When inserting a file, it becomes the most recently used item by being placed immediately after the sentinel node.

## Parameters / Member Variables
- : The File descriptor index to insert into the LRU list (must not be 0, as 0 is reserved for the sentinel)

## Dependencies
- Functions called/Symbols referenced:
  - Assert (for debugging assertions)
  - VfdCache (global array of virtual file descriptors)
  - DO_DB (debug macro for logging)
  - elog (error/log reporting)
  - _dump_lru (debug function to dump LRU state)

- Called from (representative examples):
  - AllocateDesc (when allocating a new file descriptor)
  - LruInsert (as part of LRU insertion process)
  - FileAccess (when accessing a file)
  - PathNameOpenFilePerm (when opening a file with permissions)

## Notes and Other Information
- This is a static function internal to the file descriptor management module
- The function assumes file != 0, as index 0 is reserved for the LRU list sentinel
- Debug logging is conditional on DO_DB macro compilation
- The LRU list structure is critical for PostgreSQL's file descriptor management, allowing the system to close least recently used files when approaching file descriptor limits
- The function maintains the invariant that after insertion, the specified file becomes the most recently used item in the cache