# Gzip_eof

## Location
[src/bin/pg_dump/compress_gzip.c:337-344](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/compress_gzip.c#L337-L344)

## Overview
Checks whether the end-of-file indicator is set for a gzip-compressed file handle.

## Definition
static bool Gzip_eof(CompressFileHandle *CFH)

## Detailed Description
This function determines if the end-of-file (EOF) condition has been reached for a gzip-compressed file. It extracts the gzFile handle from the CompressFileHandle structure's private_data field and uses zlib's gzeof() function to check the EOF status.

The function is part of PostgreSQL's compression abstraction layer used by pg_dump utilities. It provides a standardized way to check EOF conditions across different compression methods, allowing the caller to determine when all data has been read from a compressed file.

## Parameters / Member Variables
- `CFH`: Pointer to CompressFileHandle structure containing the gzip file handle in its private_data field

## Dependencies
- Functions called/Symbols referenced:
  - gzeof (from zlib library)
  - [CompressFileHandle](../C/CompressFileHandle.md) (structure type)
- Called from (representative examples):
  - No direct references found in the codebase (likely called through function pointers in compression interface)

## Notes and Other Information
- Returns true if EOF has been reached (gzeof returns 1), false otherwise
- This is a static function, so it's only accessible within the compress_gzip.c file
- Part of the gzip compression backend for PostgreSQL's pg_dump utility
- Requires HAVE_LIBZ to be defined for compilation (depends on zlib library)
- Essential for determining when to stop reading from compressed input files during restore operations

## Simplified Source

```c
static bool
Gzip_eof(CompressFileHandle *CFH)
{
    // Extract gzip file handle from compression wrapper
    gzFile gzfp = (gzFile) CFH->private_data;

    // Check if end-of-file reached using zlib function
    return gzeof(gzfp) == 1;
}
```