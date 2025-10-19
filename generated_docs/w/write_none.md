# write_none

## Location
[src/bin/pg_dump/compress_none.c:100-113](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/compress_none.c#L100-L113)

## Overview
The  function provides uncompressed file writing functionality for PostgreSQL's pg_dump utility, serving as the write operation handler when no compression is applied.

## Definition

```c
static void
write_none(const void *ptr, size_t size, CompressFileHandle *CFH)
```
## Detailed Description
The  function is a static helper function that handles uncompressed data writing in the pg_dump compression framework. It wraps the standard C library  function with proper error handling and reporting. The function writes the specified amount of data from a buffer to a file handle stored within a CompressFileHandle structure. If the write operation fails or doesn't write the expected number of bytes, the function terminates the program with a fatal error message.

## Parameters / Member Variables
- `*ptr`: Pointer to the data buffer to be written to the file
- `size`: Number of bytes to write from the buffer
- `*CFH`: Pointer to a CompressFileHandle structure containing the file handle in its private_data field
## Dependencies
- Functions called/Symbols referenced:
  - fwrite (C standard library)
  - [pg_fatal](../p/pg_fatal.md) (PostgreSQL error reporting function)
  - [CompressFileHandle](../C/CompressFileHandle.md) (structure type)
- Called from (representative examples):
  - [InitCompressFileHandleNone](../I/InitCompressFileHandleNone.md)

## Notes and Other Information
- This function is part of the "none" compression implementation, meaning no compression is applied
- Error handling includes checking both the return value and errno to distinguish between partial writes and storage space issues
- The function uses the ENOSPC error code when fwrite returns a short write count but errno is not set
- The function is static, limiting its scope to the compress_none.c file
- Part of the modular compression system in pg_dump that allows different compression methods to be plugged in

## Simplified Source

```c
static void
write_none(const void *ptr, size_t size, CompressFileHandle *CFH)
{
    // Write data using standard file I/O
    errno = 0;
    size_t ret = fwrite(ptr, 1, size, (FILE *) CFH->private_data);

    // Check for write errors or incomplete writes
    if (ret != size) {
        errno = (errno) ? errno : ENOSPC;
        pg_fatal("could not write to file: %m");
    }
}
```