# Zstd_open_write

## Location
[src/bin/pg_dump/compress_zstd.c:542-550](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/compress_zstd.c#L542-L550)

## Overview
Opens a zstd-compressed file for writing by appending the ".zst" extension to the provided path and delegating to the underlying file opening function.

## Definition

```c
static bool
Zstd_open_write(const char *path, const char *mode, CompressFileHandle *CFH)
```
## Detailed Description
This function is a wrapper for opening zstd-compressed files in write mode within PostgreSQL's pg_dump utility. It modifies the provided file path by appending a ".zst" extension to indicate zstd compression, then calls the underlying file opening function through the CompressFileHandle's open_func pointer. The function serves as part of the zstd compression backend implementation for pg_dump's file I/O operations.

## Parameters / Member Variables
- `path`: The base file path without the compression extension
- `mode`: The file opening mode (e.g., "w", "wb", etc.)
- `CFH`: Pointer to the CompressFileHandle structure containing compression-specific function pointers and state

## Dependencies
- Functions called/Symbols referenced:
  - [CompressFileHandle](../C/CompressFileHandle.md) (structure)
  - sprintf (standard library function)
  - CFH->open_func (function pointer)
- Called from (representative examples):
  - [InitCompressFileHandleZstd](../I/InitCompressFileHandleZstd.md) (assigned as open_write_func)

## Notes and Other Information
- This is a static function local to compress_zstd.c
- The function assumes MAXPGPATH is sufficient for the path with ".zst" extension
- Returns the result of the underlying open_func call (boolean success/failure)
- Part of the modular compression system in pg_dump that supports multiple compression formats