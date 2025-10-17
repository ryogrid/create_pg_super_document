# get_gz_error

## Location
[src/bin/pg_basebackup/bbstreamer_gzip.c:194-211](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/bbstreamer_gzip.c#L194-L211)

## Overview
Helper function that provides human-readable error messages for gzip/zlib operations by interpreting error codes and returning appropriate error strings.

## Definition
```c
static const char *get_gz_error(gzFile gzf)
```

## Detailed Description
This static utility function serves as an error message translator for gzip operations within the backup streaming system. It calls gzerror() to retrieve both the error message and error number from a gzip file handle, then determines whether the error is a system-level errno or a zlib-specific error. For system errors (Z_ERRNO), it uses strerror() to get the system error message, while for zlib-specific errors, it returns the message provided by gzerror() directly.

This abstraction allows other gzip writer functions to provide consistent, informative error messages without having to handle the complexity of different error types from the zlib library.

## Parameters / Member Variables
- `gzf`: The gzFile handle from which to extract error information

## Dependencies
- Functions called/Symbols referenced:
  - gzerror
  - strerror
- Called from (representative examples):
  - [bbstreamer_gzip_writer_new](../b/bbstreamer_gzip_writer_new.md) (src/bin/pg_basebackup/bbstreamer_gzip.c:112)
  - [bbstreamer_gzip_writer_content](../b/bbstreamer_gzip_writer_content.md) (src/bin/pg_basebackup/bbstreamer_gzip.c:144)

## Notes and Other Information
- Handles both system-level errors (Z_ERRNO) and zlib-specific errors
- Returns string pointers that should not be freed by the caller
- Essential for providing meaningful error messages in gzip backup operations
- Used consistently across gzip writer functions for error reporting
- Encapsulates the complexity of zlib error handling in a single location

## Simplified Source

```c
static const char *
get_gz_error(gzFile gzf)
{
    int errnum;
    const char *errmsg;

    // Get error info from gzip file
    errmsg = gzerror(gzf, &errnum);

    // Return system error or zlib error message
    if (errnum == Z_ERRNO)
        return strerror(errno);
    else
        return errmsg;
}
```