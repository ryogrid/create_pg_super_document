# Zstd_eof

## Location
[src/bin/pg_dump/compress_zstd.c:496-503](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/compress_zstd.c#L496-L503)

## Overview
Checks whether the end-of-file has been reached for a Zstd-compressed file handle.

## Definition
```c
static bool Zstd_eof(CompressFileHandle *CFH)
```

## Detailed Description
This function provides a simple wrapper around the standard feof() function to check for end-of-file condition on a Zstd-compressed stream. It accesses the underlying FILE pointer from the ZstdCompressorState structure and delegates to the standard library's feof() function to determine if the end of the file has been reached.

The function serves as part of the compression abstraction layer, providing a uniform interface for EOF checking across different compression formats in PostgreSQL's dump utility.

## Parameters / Member Variables
- `CFH`: Compressed file handle to check for EOF condition

## Dependencies
- Functions called/Symbols referenced:
  - [ZstdCompressorState](ZstdCompressorState.md)
  - [CompressFileHandle](../C/CompressFileHandle.md)
  - feof (standard library function)
- Called from (representative examples):
  - [InitCompressFileHandleZstd](../I/InitCompressFileHandleZstd.md) (as part of function pointer assignment)

## Notes and Other Information
- This is a static function within the Zstd compression module
- Returns true if end-of-file has been reached, false otherwise
- Simple wrapper around standard feof() function
- Part of the compression abstraction layer that allows uniform EOF checking
- The function signature matches the EOF interface pattern used across compression implementations
- Does not perform any decompression or buffer management - purely checks the underlying file stream status