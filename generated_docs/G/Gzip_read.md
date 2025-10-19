# Gzip_read

## Location
[src/bin/pg_dump/compress_gzip.c:255-284](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/compress_gzip.c#L255-L284)

## Overview
Reads data from a gzip-compressed file handle and handles compression-related errors appropriately.

## Definition

```c
static size_t
Gzip_read(void *ptr, size_t size, CompressFileHandle *CFH)
```
## Detailed Description
This function provides a wrapper around zlib's gzread() function for reading compressed data. It reads up to 'size' bytes from the gzip file into the provided buffer. The function includes comprehensive error handling to distinguish between EOF conditions and actual read errors. When gzread() returns zero or negative values, it checks whether the file has reached EOF using gzeof(), and if not, it reports the specific error using gzerror(). This ensures that applications can properly handle both normal end-of-file conditions and error situations.

## Parameters / Member Variables
- `*ptr`: Pointer to the buffer where read data will be stored
- `size`: Maximum number of bytes to read
- `*CFH`: Compressed file handle containing the gzip file pointer in private_data
## Dependencies
- Functions called/Symbols referenced:
  - gzread
  - gzeof
  - gzerror
  - strerror
  - [pg_fatal](../p/pg_fatal.md)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Returns the actual number of bytes read, or 0 on EOF
- Distinguishes between Z_ERRNO errors (system errors) and zlib-specific errors
- Uses CompressFileHandle structure to access the underlying gzFile
- Part of the Compress File API for handling gzip-compressed files in pg_dump/pg_restore

## Simplified Source

```c
static size_t Gzip_read(void *ptr, size_t size, CompressFileHandle *CFH)
{
    gzFile gzfp = (gzFile) CFH->private_data;
    int gzret;

    // Read data from gzip file
    gzret = gzread(gzfp, ptr, size);

    // Handle error/EOF conditions
    if (gzret <= 0) {
        // Check if this is actually EOF
        if (gzret == 0 && gzeof(gzfp))
            return 0;

        // Handle read error
        int errnum;
        const char *errmsg = gzerror(gzfp, &errnum);
        pg_fatal("could not read from input file: %s",
                 errnum == Z_ERRNO ? strerror(errno) : errmsg);
    }

    return (size_t) gzret;
}
```