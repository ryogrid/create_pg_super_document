# Gzip_write

## Location
src/bin/pg_dump/compress_gzip.c: 285 - 299

## Overview
Writes data to a gzip-compressed file handle with error checking and reporting.

## Definition
```c
static void Gzip_write(const void *ptr, size_t size, CompressFileHandle *CFH)
```

## Detailed Description
This function provides a wrapper around zlib's gzwrite() function for writing data to a compressed file. It writes the specified number of bytes from the buffer to the gzip file. The function includes error checking by verifying that gzwrite() successfully wrote the expected number of bytes. If the write operation fails or writes fewer bytes than requested, it retrieves the specific error information using gzerror() and reports it via pg_fatal(), distinguishing between system errors (Z_ERRNO) and zlib-specific errors.

## Parameters / Member Variables
- `ptr`: Pointer to the data buffer to write
- `size`: Number of bytes to write from the buffer
- `CFH`: Compressed file handle containing the gzip file pointer in private_data

## Dependencies
- Functions called/Symbols referenced:
  - gzwrite
  - gzerror
  - strerror
  - pg_fatal
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Part of the Compress File API for handling gzip-compressed files in pg_dump/pg_restore
- Uses CompressFileHandle structure to access the underlying gzFile
- Terminates the program with pg_fatal() if write operation fails
- Handles both system errors and zlib-specific compression errors appropriately