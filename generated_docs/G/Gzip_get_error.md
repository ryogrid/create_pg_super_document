# Gzip_get_error

## Location
[src/bin/pg_dump/compress_gzip.c:345-358](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/compress_gzip.c#L345-L358)

## Overview
Retrieves the last error message associated with a gzip-compressed file handle, providing detailed error information for debugging and error handling.

## Definition
static const char *Gzip_get_error(CompressFileHandle *CFH)

## Detailed Description
This function obtains the most recent error message from a gzip file operation. It extracts the gzFile handle from the CompressFileHandle structure's private_data field and uses zlib's gzerror() function to retrieve both an error message and error number. 

The function includes special handling for system-level errors: when the error number is Z_ERRNO (indicating a system error rather than a zlib-specific error), it uses strerror() to get the appropriate system error message instead of the zlib error message.

This function is part of PostgreSQL's compression abstraction layer, providing standardized error reporting across different compression methods used by pg_dump utilities.

## Parameters / Member Variables
- `CFH`: Pointer to CompressFileHandle structure containing the gzip file handle in its private_data field

## Dependencies
- Functions called/Symbols referenced:
  - gzerror (from zlib library)
  - strerror (standard C library function)
  - Z_ERRNO (zlib constant)
  - [CompressFileHandle](../C/CompressFileHandle.md) (structure type)
- Called from (representative examples):
  - No direct references found in the codebase (likely called through function pointers in compression interface)

## Notes and Other Information
- Returns a const char* pointing to the error message string
- Automatically distinguishes between zlib-specific errors and system errors
- For system errors (Z_ERRNO), returns the system error message via strerror(errno)
- This is a static function, so it's only accessible within the compress_gzip.c file
- Part of the gzip compression backend for PostgreSQL's pg_dump utility
- Requires HAVE_LIBZ to be defined for compilation (depends on zlib library)
- Essential for proper error handling and troubleshooting during compression/decompression operations