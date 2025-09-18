# InitCompressFileHandleGzip

## Location
[src/bin/pg_dump/compress_gzip.c:432-437](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/compress_gzip.c#L432-L437)

## Overview
Initializes a CompressFileHandle structure for gzip file compression, setting up function pointers for file-based gzip operations using the zlib library.

## Definition
```c
void InitCompressFileHandleGzip(CompressFileHandle *CFH, const pg_compress_specification compression_spec)
```

## Detailed Description
InitCompressFileHandleGzip configures a CompressFileHandle structure to provide gzip compression and decompression capabilities for file operations. Like its companion InitCompressorGzip, this function has two implementations depending on build configuration:

1. **When HAVE_LIBZ is defined**: Sets up function pointers for all gzip file operations including open, read, write, close, and error handling functions. It assigns gzip-specific implementations for file I/O operations.
2. **When HAVE_LIBZ is not defined**: Terminates with a fatal error indicating that gzip support was not compiled in.

The function provides a complete file handle interface for gzip-compressed files, supporting both reading and writing operations with proper error handling and EOF detection.

## Parameters / Member Variables
- `CFH`: Pointer to a CompressFileHandle structure to be initialized with gzip file operation capabilities
- `compression_spec`: Structure containing compression parameters including compression level and other gzip-specific options

## Dependencies
- Functions called/Symbols referenced:
  - [Gzip_open](../G/Gzip_open.md)
  - [Gzip_open_write](../G/Gzip_open_write.md)
  - [Gzip_read](../G/Gzip_read.md)
  - [Gzip_write](../G/Gzip_write.md)
  - [Gzip_gets](../G/Gzip_gets.md)
  - [Gzip_getc](../G/Gzip_getc.md)
  - [Gzip_close](../G/Gzip_close.md)
  - [Gzip_eof](../G/Gzip_eof.md)
  - [Gzip_get_error](../G/Gzip_get_error.md)
  - [pg_fatal](../p/pg_fatal.md)
- Called from (representative examples):
  - [InitCompressFileHandle](InitCompressFileHandle.md) (src/bin/pg_dump/compress_io.c:204)

## Notes and Other Information
- The function is part of PostgreSQL's Compress File API for handling compressed files
- Conditionally compiled based on HAVE_LIBZ preprocessor definition  
- When zlib is not available, calling this function results in program termination
- Sets up a complete file interface including specialized functions for opening write files with .gz extension
- The private_data field is initialized to NULL and used to store the gzFile handle during operations
- Located in src/bin/pg_dump/compress_gzip.c:406-422 (HAVE_LIBZ version) and lines 432-437 (no-libz version)
- Supports both file descriptor and path-based file opening through the open_func pointer