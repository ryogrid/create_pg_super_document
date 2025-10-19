# Gzip_close

## Location
[src/bin/pg_dump/compress_gzip.c:327-336](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/compress_gzip.c#L327-L336)

## Overview
Closes a gzip-compressed file handle and performs cleanup operations for the compression file handle structure.

## Definition
static bool Gzip_close(CompressFileHandle *CFH)

## Detailed Description
This function closes a gzip file that was previously opened through the PostgreSQL compression interface. It extracts the gzFile handle from the CompressFileHandle structure's private_data field, closes the underlying gzip file using zlib's gzclose() function, and cleans up by setting the private_data pointer to NULL to prevent further access.

The function is part of PostgreSQL's compression abstraction layer used by pg_dump utilities to handle compressed archive files. It provides a standardized interface for closing gzip-compressed files regardless of the specific compression method used.

## Parameters / Member Variables
- `CFH`: Pointer to CompressFileHandle structure containing the gzip file handle in its private_data field

## Dependencies
- Functions called/Symbols referenced:
  - gzclose (from zlib library)
  - Z_OK (zlib constant)
- Called from (representative examples):
  - No direct references found in the codebase (likely called through function pointers in compression interface)

## Notes and Other Information
- Returns true if the file was successfully closed (gzclose returns Z_OK), false otherwise
- Sets CFH->private_data to NULL after closing to prevent dangling pointer access
- This is a static function, so it's only accessible within the compress_gzip.c file
- Part of the gzip compression backend for PostgreSQL's pg_dump utility
- Requires HAVE_LIBZ to be defined for compilation (depends on zlib library)

## Simplified Source

```c
static bool Gzip_close(CompressFileHandle *CFH)
{
    gzFile gzfp = (gzFile) CFH->private_data;

    // Clear private data to prevent further access
    CFH->private_data = NULL;

    // Close gzip file and return success status
    return gzclose(gzfp) == Z_OK;
}
```