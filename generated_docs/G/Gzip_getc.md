# Gzip_getc

## Location
[src/bin/pg_dump/compress_gzip.c:300-318](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/compress_gzip.c#L300-L318)

## Overview
Reads a single character from a gzip-compressed file handle with comprehensive EOF and error handling.

## Definition
```c
static int Gzip_getc(CompressFileHandle *CFH)
```

## Detailed Description
This function provides a wrapper around zlib's gzgetc() function for reading a single character from a compressed file. It reads one character from the gzip file and handles both end-of-file and error conditions. When gzgetc() returns EOF, the function uses gzeof() to determine whether this is a legitimate end-of-file condition or an error. If it's an actual EOF, it reports this condition; if it's an error during reading, it reports a read error. The function ensures that errno is cleared before the operation to provide accurate error reporting.

## Parameters / Member Variables
- `CFH`: Compressed file handle containing the gzip file pointer in private_data

## Dependencies
- Functions called/Symbols referenced:
  - gzgetc
  - gzeof
  - [pg_fatal](../p/pg_fatal.md)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Returns the character as an int (allowing for EOF representation)
- Clears errno before operation for accurate error detection
- Distinguishes between legitimate EOF and read errors
- Part of the Compress File API for handling gzip-compressed files in pg_dump/pg_restore
- Uses CompressFileHandle structure to access the underlying gzFile
- Terminates program with pg_fatal() on any error condition (including EOF)

## Simplified Source

```c
static int Gzip_getc(CompressFileHandle *CFH)
{
    gzFile gzfp = (gzFile) CFH->private_data;
    int ret;

    // Read single character from gzip file
    errno = 0;
    ret = gzgetc(gzfp);

    // Handle EOF/error conditions
    if (ret == EOF) {
        if (!gzeof(gzfp))
            pg_fatal("could not read from input file: %m");
        else
            pg_fatal("could not read from input file: end of file");
    }

    return ret;
}
```