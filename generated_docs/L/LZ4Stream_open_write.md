# LZ4Stream_open_write

## Location
[src/bin/pg_dump/compress_lz4.c:753-772](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/compress_lz4.c#L753-L772)

## Overview
This static function is responsible for opening an LZ4-compressed file for write operations by appending the ".lz4" extension to the given path and delegating to the appropriate open function.

## Definition

```c
static bool
LZ4Stream_open_write(const char *path, const char *mode, CompressFileHandle *CFH)
```
## Detailed Description
LZ4Stream_open_write is an internal helper function used within the LZ4 compression module of pg_dump. It handles the file opening process for LZ4-compressed output files by:

1. Creating a filename with the ".lz4" extension appended to the provided path
2. Calling the configured open function through the CompressFileHandle structure
3. Managing memory cleanup and error preservation throughout the process

The function is designed to work as part of the compression file handle abstraction layer, providing a consistent interface for opening LZ4 files while handling the specific naming conventions required for LZ4 compressed files.

## Parameters / Member Variables
- `path`: The base file path without the .lz4 extension
- `mode`: The file opening mode (e.g., "w", "wb") to be passed to the underlying open function
- `CFH`: Pointer to the CompressFileHandle structure containing the configured open function and other compression-related state

## Dependencies
- Functions called/Symbols referenced:
  - [psprintf](../p/psprintf.md) (for string formatting)
  - CFH->open_func (configured open function)
  - [pg_free](../p/pg_free.md) (for memory cleanup)
- Called from (representative examples):
  - [InitCompressFileHandleLZ4](../I/InitCompressFileHandleLZ4.md) (sets this as the open_write_func callback)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the compress_lz4.c file
- The function carefully preserves errno across the pg_free call to maintain proper error reporting
- Returns a boolean indicating success or failure of the file opening operation
- The function assumes the caller will handle any necessary error checking based on the return value and errno
- Part of the LZ4 compression implementation for PostgreSQL's pg_dump utility

## Simplified Source

```c
static bool
LZ4Stream_open_write(const char *path, const char *mode, CompressFileHandle *CFH)
{
    // Create filename with .lz4 extension
    char *fname = psprintf("%s.lz4", path);

    // Delegate to the configured open function
    bool ret = CFH->open_func(fname, -1, mode, CFH);

    // Clean up allocated filename while preserving errno
    int save_errno = errno;
    pg_free(fname);
    errno = save_errno;

    return ret;
}
```