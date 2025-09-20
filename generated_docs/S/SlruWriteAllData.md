# SlruWriteAllData

## Location
[src/backend/access/transam/slru.c:126-131](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/slru.c#L126-L131)

## Overview
SlruWriteAllData is a structure used to track open file descriptors and their corresponding segment numbers during bulk write operations in PostgreSQL's Simple Log-based Recovery Unit (SLRU) system.

## Definition

```c
typedef struct SlruWriteAllData
{
	int			num_files;		/* # files actually open */
	int			fd[MAX_WRITEALL_BUFFERS];	/* their FD's */
	int64		segno[MAX_WRITEALL_BUFFERS];	/* their log seg#s */
} SlruWriteAllData;
```
## Detailed Description
This structure is designed to optimize I/O operations during SimpleLruWriteAll() by consolidating write requests and keeping files open until the entire write operation is complete. Instead of opening and closing files repeatedly, this structure maintains a cache of up to MAX_WRITEALL_BUFFERS (16) open file descriptors along with their corresponding segment numbers. This approach reduces system call overhead and improves performance when writing multiple pages that may belong to the same physical files.

## Parameters / Member Variables
- `num_files`: The actual number of files currently open and tracked in this structure
- `fd[MAX_WRITEALL_BUFFERS]`: Array of file descriptors for the open files, with a maximum of 16 entries
- `segno[MAX_WRITEALL_BUFFERS]`: Array of corresponding log segment numbers for each open file descriptor
## Dependencies
- Functions called/Symbols referenced:
  - MAX_WRITEALL_BUFFERS (constant defining maximum buffer size of 16)
- Called from (representative examples):
  - [SlruWriteAll](SlruWriteAll.md) (type alias pointing to this struct)
  - [SimpleLruWriteAll](SimpleLruWriteAll.md) (function that uses this structure for bulk writes)

## Notes and Other Information
- This structure is specifically designed for the SimpleLruWriteAll() operation to minimize file I/O overhead
- The MAX_WRITEALL_BUFFERS limit of 16 is chosen to balance memory usage with the typical number of files that need to be written during bulk operations
- The structure allows for efficient batching of write operations across multiple SLRU files
- File descriptors are kept open throughout the duration of the bulk write operation to avoid repeated open/close system calls