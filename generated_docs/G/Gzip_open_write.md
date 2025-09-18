# Gzip_open_write

## Location
src/bin/pg_dump/compress_gzip.c: 389 - 405

## Overview
Opens a gzip-compressed file for write operations by automatically appending the ".gz" extension to the specified path and delegating to the compression file handle's open function.

## Definition
static bool Gzip_open_write(const char *path, const char *mode, CompressFileHandle *CFH)

## Detailed Description
This function is a specialized wrapper for opening gzip files in write mode. It automatically appends the ".gz" extension to the provided path to create the compressed output filename, then calls the CompressFileHandle's open_func function pointer to perform the actual file opening.

The function uses psprintf() to dynamically allocate memory for the new filename, ensuring proper concatenation of the original path with the ".gz" extension. After calling the open function, it carefully preserves the errno value across the memory cleanup operation using pg_free(), ensuring that any error conditions from the file opening are not masked by the memory deallocation.

This function provides a convenient interface specifically for write operations, abstracting away the filename generation details while maintaining compatibility with PostgreSQL's compression abstraction layer.

## Parameters / Member Variables
- `path`: Base file path to which ".gz" extension will be appended
- `mode`: File access mode for opening (typically write modes like "w", "wb")
- `CFH`: Pointer to CompressFileHandle structure containing the open_func function pointer

## Dependencies
- Functions called/Symbols referenced:
  - [psprintf](../p/psprintf.md) (PostgreSQL's version of sprintf with memory allocation)
  - [pg_free](../p/pg_free.md) (PostgreSQL's memory deallocation function)
  - CFH->open_func (function pointer to the actual open implementation)
  - [CompressFileHandle](../C/CompressFileHandle.md) (structure type)
- Called from (representative examples):
  - No direct references found in the codebase (likely called through function pointers in compression interface)

## Notes and Other Information
- Returns true on success, false if the file could not be opened
- Automatically appends ".gz" extension to the filename
- Uses file descriptor -1 to indicate path-based opening (not descriptor-based)
- Preserves errno across memory cleanup operations to maintain proper error reporting
- This is a static function, so it's only accessible within the compress_gzip.c file
- Part of the gzip compression backend for PostgreSQL's pg_dump utility
- Requires HAVE_LIBZ to be defined for compilation (depends on zlib library)
- Designed specifically for write operations where the .gz extension should be automatically added