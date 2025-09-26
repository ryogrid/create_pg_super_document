# BufFileClose

## Location
[src/backend/storage/file/buffile.c:412-433](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/buffile.c#L412-L433)

## Overview
BufFileClose closes a BufFile and releases all associated resources, including flushing unwritten data and closing underlying files.

## Definition

```c
void
BufFileClose(BufFile *file)
```
## Detailed Description
BufFileClose performs a complete cleanup of a BufFile structure, similar to the standard fclose() function. It ensures data integrity by flushing any unwritten data before closing the file, then proceeds to close all underlying file descriptors and free allocated memory. This function handles BufFiles that may span multiple physical files, closing each one individually.

The function performs three main operations in sequence:
1. Flushes any pending data in the buffer to ensure no data is lost
2. Closes all underlying file descriptors associated with the BufFile
3. Releases all dynamically allocated memory including the file array and BufFile structure itself

## Parameters / Member Variables
- : Pointer to the BufFile structure to be closed and cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - BufFileFlush (flushes pending data before closing)
  - FileClose (closes individual underlying files)
  - pfree (frees allocated memory)
- Called from (representative examples):
  - gistFreeBuildBuffers (GiST index building cleanup)
  - ExecHashTableDestroy (hash table cleanup in executor)
  - LogicalTapeSetClose (logical tape management)
  - tuplestore_end (tuple store cleanup)
  - stream_close_file (logical replication worker)

## Notes and Other Information
- This function implicitly calls FileClose on all underlying files, similar to how fclose() works with standard file handles
- The function properly handles BufFiles that may consist of multiple underlying files (file->numFiles)
- Memory cleanup includes both the files array and the BufFile structure itself
- Should always be called when done with a BufFile to prevent resource leaks
- Used extensively in PostgreSQL's internal buffering systems including hash joins, tuple stores, and index building operations