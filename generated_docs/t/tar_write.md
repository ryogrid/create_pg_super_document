# tar_write

## Location
src/bin/pg_basebackup/walmethods.c: 765 - 804

## Overview
Writes data to a TAR archive file with support for both uncompressed and gzip-compressed output during PostgreSQL WAL streaming operations.

## Definition
```c
static ssize_t tar_write(Walfile *f, const void *buf, size_t count)
```

## Detailed Description
This function serves as the main write interface for TAR-based WAL files in PostgreSQL's base backup and WAL archiving system. It handles both compressed and uncompressed writes based on the compression algorithm specified in the WAL method configuration. For uncompressed data, it directly writes to the file descriptor. For gzip compression, it delegates to tar_write_compressed_data() to handle the compression and buffering. The function maintains the current file position and provides appropriate error handling for different failure scenarios.

## Parameters / Member Variables
- `f`: Pointer to Walfile structure representing the open WAL file
- `buf`: Pointer to data buffer to be written
- `count`: Number of bytes to write from the buffer

## Dependencies
- Functions called/Symbols referenced:
  - clear_error (error state reset function)
  - write (system call for uncompressed writes)
  - tar_write_compressed_data (compression handler)
  - TarMethodData (cast target type)
  - PG_COMPRESSION_NONE (compression algorithm constant)
  - PG_COMPRESSION_GZIP (compression algorithm constant)
- Called from:
  - CreateWalDirectoryMethod (as function pointer assignment)
  - tar_write_padding_data
  - tar_close

## Notes and Other Information
- Returns the number of bytes written on success, -1 on failure
- Updates f->currpos to track the current logical position in the file
- Assumes ENOSPC when write() fails without setting errno
- Requires HAVE_LIBZ compilation flag for gzip compression support
- Returns ENOSYS error for unsupported compression algorithms
- Critical component of PostgreSQL's WAL archiving and base backup infrastructure