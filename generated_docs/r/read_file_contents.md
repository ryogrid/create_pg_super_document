# read_file_contents

## Location
src/bin/pgbench/pgbench.c: 6078 - 6110

## Overview
Reads the entire contents of a file descriptor into a dynamically allocated buffer and returns it as a null-terminated string.

## Definition
static char *read_file_contents(FILE *fd)

## Detailed Description
This utility function reads the complete contents of an open file descriptor into memory using a dynamically growing buffer approach. It starts with a BUFSIZ-sized buffer and expands it as needed by reading data in BUFSIZ chunks. The function continues reading until EOF or an error occurs, then null-terminates the buffer and returns it. The caller is responsible for freeing the returned buffer. This function is optimized for convenience rather than memory efficiency, as it may allocate more memory than strictly necessary.

## Parameters / Member Variables
- fd: FILE pointer to an open file descriptor to read from

## Dependencies
- Functions called/Symbols referenced:
  - pg_malloc
  - pg_realloc
  - fread (standard C library)
  - BUFSIZ (standard C constant)
- Called from:
  - [process_file](../p/process_file.md) (src/bin/pgbench/pgbench.c:6122)

## Notes and Other Information
- Returns a malloc'd buffer that must be freed by the caller
- Buffer is always null-terminated for safe string operations
- May over-allocate memory but this is acceptable for pgbench's use case
- Uses standard BUFSIZ for read chunks (typically 8192 bytes)
- Handles files of arbitrary size through dynamic buffer growth
- Part of pgbench's file processing infrastructure for reading script files
- Does not perform error checking on fread() - relies on caller to handle file errors