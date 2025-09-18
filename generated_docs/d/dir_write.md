# dir_write

## Location
src/bin/pg_basebackup/walmethods.c: 304 - 384

## Overview
Writes data to a WAL file, handling different compression algorithms and maintaining position tracking for the directory-based WAL method.

## Definition
```c
static ssize_t
dir_write(Walfile *f, const void *buf, size_t count)
```

## Detailed Description
This function handles writing data to WAL files in the directory-based method, supporting three different compression algorithms: none (uncompressed), gzip, and LZ4. The function adapts its behavior based on the compression algorithm specified in the WalWriteMethod configuration:

1. **Gzip compression**: Uses gzwrite() to write compressed data directly
2. **LZ4 compression**: Processes data in chunks using LZ4F_compressUpdate(), writing compressed chunks to the file descriptor
3. **No compression**: Uses standard write() system call

For LZ4 compression, the function processes data in LZ4_IN_SIZE chunks to manage memory efficiently and handle large writes. The function maintains the current position in the file and handles error conditions appropriately, defaulting to ENOSPC (no space left on device) when write operations fail without setting errno.

## Parameters / Member Variables
- `f`: Pointer to Walfile structure representing the open WAL file
- `buf`: Pointer to the data buffer to write
- `count`: Number of bytes to write from the buffer

## Dependencies
- Functions called/Symbols referenced:
  - clear_error (error state initialization)
  - gzwrite (gzip compression write)
  - LZ4F_compressUpdate (LZ4 compression)
  - LZ4F_isError, LZ4F_getErrorName (LZ4 error handling)
  - write (system call for uncompressed and LZ4 compressed data)
- Called from (representative examples):
  - WAL writing operations in pg_basebackup

## Notes and Other Information
- This is a static function, only accessible within the walmethods.c file
- Returns the number of bytes successfully written, or -1 on error
- For compressed files, the return value represents uncompressed bytes written
- Updates the current position (currpos) in the Walfile structure after successful writes
- LZ4 compression processes data in chunks to optimize memory usage
- Error handling defaults to ENOSPC when system calls don\t set errno