# read_binary_file

## Location
[src/backend/utils/adt/genfile.c:103-154](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/genfile.c#L103-L154)

## Overview
Reads a section of a file and returns it as a bytea (binary data) object, with support for seeking to specific offsets and reading specified byte ranges.

## Definition
static bytea *read_binary_file(const char *filename, int64 seek_offset, int64 bytes_to_read, bool missing_ok)

## Detailed Description
This function provides low-level binary file reading capabilities with flexible positioning and size control. It opens a file in binary mode, seeks to a specified offset, and reads either a specified number of bytes or the entire remaining file content. The function handles both positive seek offsets (from beginning) and negative offsets (from end). When bytes_to_read is negative, it reads the entire remaining file from the current position using a dynamic string buffer. The function includes comprehensive error handling for file access issues and respects the missing_ok flag to allow graceful handling of non-existent files.

## Parameters / Member Variables
- : Path to the file to be read
- : Position in file to start reading from (positive from start, negative from end)
- : Number of bytes to read (-1 means read to end of file)
- : If true, return NULL for non-existent files instead of throwing error

## Dependencies
- Functions called/Symbols referenced:
  - MaxAllocSize: Maximum allowed allocation size for security
  - ereport: Error reporting mechanism
  - [AllocateFile](../A/AllocateFile.md): PostgreSQL file allocation wrapper
  - PG_BINARY_R: Binary read mode constant
  - fseeko: File seeking with 64-bit offsets
  - VARHDRSZ: Variable-length data header size
  - VARDATA: Access variable-length data content
  - [palloc](../p/palloc.md): PostgreSQL memory allocator
  - [initStringInfo](../i/initStringInfo.md): Initialize dynamic string buffer
  - [appendBinaryStringInfo](../a/appendBinaryStringInfo.md): Append binary data to string buffer
  - [FreeFile](../F/FreeFile.md): Release file handle
- Called from (representative examples):
  - [read_text_file](read_text_file.md): For text file reading with encoding validation
  - [pg_read_binary_file_common](../p/pg_read_binary_file_common.md): Higher-level binary file reading wrapper

## Notes and Other Information
- The caller is responsible for all permissions checking before calling this function
- File size requests are clamped to MaxAllocSize - VARHDRSZ to prevent memory exhaustion
- Uses PostgreSQL's AllocateFile/FreeFile wrappers instead of direct fopen/fclose
- Supports both exact-size reads and read-to-EOF operations
- Returns bytea format compatible with PostgreSQL's binary data handling
- Error messages include filename for better debugging
- The function properly handles partial reads and file errors