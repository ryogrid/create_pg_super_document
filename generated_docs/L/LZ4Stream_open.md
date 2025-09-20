# LZ4Stream_open

## Location
[src/bin/pg_dump/compress_lz4.c:734-752](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/compress_lz4.c#L734-L752)

## Overview
Opens a file handle for LZ4 compressed stream operations, supporting both file path and file descriptor based access.

## Definition

```c
static bool
LZ4Stream_open(const char *path, int fd, const char *mode,
			   CompressFileHandle *CFH)
```
## Detailed Description
LZ4Stream_open initializes file access for LZ4 compression operations by opening a file handle using either a file path or an existing file descriptor. The function supports both modes of access: if a valid file descriptor (fd >= 0) is provided, it duplicates the descriptor and creates a FILE* using fdopen(); otherwise, it opens the file using the provided path with fopen(). This dual-mode approach provides flexibility for different use cases in pg_dump operations.

The function serves as the initialization step for LZ4 stream operations, establishing the underlying file handle that will be used by subsequent compression/decompression operations. It stores the opened file handle in the LZ4State structure for later use by other LZ4Stream functions.

## Parameters / Member Variables
- : File path to open (used when fd < 0)
- : File descriptor to use (when >= 0, overrides path parameter)
- : File opening mode string (e.g., "rb", "wb")
- : Pointer to the CompressFileHandle structure containing the LZ4 state

## Dependencies
- Functions called/Symbols referenced:
  - fdopen (creates FILE* from file descriptor)
  - dup (duplicates file descriptor)
  - fopen (opens file by path)
- Types referenced:
  - [CompressFileHandle](../C/CompressFileHandle.md) (compression file handle structure)
  - [LZ4State](LZ4State.md) (LZ4 compression state structure)
- Called from (representative examples):
  - No direct callers found in the current codebase

## Notes and Other Information
- This is a static function, meaning it's only accessible within the compress_lz4.c file
- Returns true on successful open, false on failure
- Supports both file descriptor and file path based opening
- When using file descriptor, it duplicates the descriptor to avoid conflicts with caller's usage
- Stores errno in state->errcode on failure for later error reporting
- The opened file handle is stored in state->fp for use by other LZ4Stream functions
- The function is designed to be used as a callback function pointer in the CompressFileHandle structure
- Part of PostgreSQL's modular compression system that supports multiple compression algorithms
- Does not perform any LZ4-specific initialization - that is handled by LZ4Stream_init()
- Essential first step before any compression or decompression operations can begin
- The mode parameter determines whether the stream will be used for reading or writing