# Zstd_read

## Location
[src/bin/pg_dump/compress_zstd.c:429-434](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/compress_zstd.c#L429-L434)

## Overview
Reads data from a Zstd-compressed file handle with automatic decompression.

## Definition
```c
static size_t Zstd_read(void *ptr, size_t size, CompressFileHandle *CFH)
```

## Detailed Description
This function provides a simple wrapper around the internal Zstd reading functionality. It reads and decompresses data from a Zstd-compressed stream, automatically handling the decompression process. The function delegates to Zstd_read_internal with the 'eof_ok' parameter set to true, indicating that reaching end-of-file is an acceptable condition and should not be treated as an error.

This is the standard read interface for Zstd-compressed files in PostgreSQL's dump utility, providing a clean abstraction that mimics standard file I/O operations.

## Parameters / Member Variables
- `ptr`: Buffer to store the read and decompressed data
- `size`: Number of bytes to read
- `CFH`: Compressed file handle for the Zstd stream

## Dependencies
- Functions called/Symbols referenced:
  - [Zstd_read_internal](Zstd_read_internal.md)
  - [CompressFileHandle](../C/CompressFileHandle.md)
- Called from (representative examples):
  - [InitCompressFileHandleZstd](../I/InitCompressFileHandleZstd.md) (as part of function pointer assignment)

## Notes and Other Information
- This is a static function within the Zstd compression module
- Serves as a wrapper around Zstd_read_internal with eof_ok=true
- Returns the number of bytes successfully read and decompressed
- Part of the compression abstraction layer that allows pg_dump to work with different compression formats uniformly
- The function signature matches the standard read interface pattern used across compression implementations