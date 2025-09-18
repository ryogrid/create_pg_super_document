# slurpFile

## Location
src/bin/pg_rewind/file_ops.c: 314 - 361

## Overview
Reads an entire file into memory, returning the contents in a malloc'd buffer with automatic zero-termination for convenient text file processing.

## Definition
```c
char *slurpFile(const char *datadir, const char *path, size_t *filesize)
```

## Detailed Description
This utility function reads a complete file from the filesystem into memory in a single operation. It constructs the full file path by combining the datadir and path parameters, opens the file in binary mode, determines its size using fstat, allocates an appropriately sized buffer, reads the entire contents, and automatically zero-terminates the buffer. The function is designed to handle both text and binary files, with the zero-termination being particularly useful for text processing while remaining ignorable for binary data.

## Parameters / Member Variables
- `datadir`: Base directory path where the file is located
- `path`: Relative path of the file to read within the datadir
- `filesize`: Output parameter that receives the actual file size (excluding the zero-terminator)

## Dependencies
- Functions called/Symbols referenced:
  - open (system call with PG_BINARY flag)
  - fstat (system call for file statistics)
  - read (system call)
  - close (system call)
  - pg_malloc (PostgreSQL memory allocation)
  - snprintf (standard library)
  - pg_fatal (PostgreSQL error reporting)
- Called from (representative examples):
  - local_fetch_file (local_source.c:68)
  - main (pg_rewind.c:331, 341)
  - getTimelineHistory (pg_rewind.c:882)
- Declared in:
  - file_ops.h:24

## Notes and Other Information
- Returns a buffer that is always (filesize + 1) bytes to accommodate the zero-terminator
- Uses MAXPGPATH for safe path construction to prevent buffer overflows
- Provides comprehensive error handling for all file operations (open, fstat, read)
- The caller is responsible for freeing the returned buffer using appropriate memory management
- Handles partial read scenarios with detailed error reporting showing expected vs actual bytes read
- Uses PG_BINARY flag to ensure consistent behavior across different platforms
- Common utility function used throughout pg_rewind for configuration file reading and timeline processing