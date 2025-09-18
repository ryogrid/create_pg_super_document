# InitCompressFileHandleNone

## Location
[src/bin/pg_dump/compress_none.c:201-215](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/compress_none.c#L201-L215)

## Overview
A public interface function that initializes a CompressFileHandle structure for uncompressed file operations in PostgreSQL's pg_dump utility.

## Definition


## Detailed Description
The `InitCompressFileHandleNone` function serves as the main initialization entry point for the "none" compression implementation in pg_dump. It sets up a CompressFileHandle structure by assigning appropriate function pointers for all file operations (open, read, write, close, etc.) to their uncompressed equivalents. This function implements the compression abstraction layer's interface, allowing pg_dump to work with uncompressed files using the same API as compressed files. All function pointers are set to the corresponding "_none" variants that handle uncompressed file operations.

## Parameters / Member Variables
- `CFH`: Pointer to CompressFileHandle structure to initialize
- `compression_spec`: Compression specification parameter (unused for none compression but required for interface consistency)

## Dependencies
- Functions called/Symbols referenced:
  - [CompressFileHandle](../C/CompressFileHandle.md) (structure type)
  - [pg_compress_specification](../p/pg_compress_specification.md) (structure type)
  - [open_none](../o/open_none.md) (function pointer assignment)
  - [open_write_none](../o/open_write_none.md) (function pointer assignment)
  - [read_none](../r/read_none.md) (function pointer assignment)
  - [write_none](../w/write_none.md) (function pointer assignment)
  - [gets_none](../g/gets_none.md) (function pointer assignment)
  - [getc_none](../g/getc_none.md) (function pointer assignment)
  - [close_none](../c/close_none.md) (function pointer assignment)
  - [eof_none](../e/eof_none.md) (function pointer assignment)
  - [get_error_none](../g/get_error_none.md) (function pointer assignment)
- Called from (representative examples):
  - [InitCompressFileHandle](InitCompressFileHandle.md) (main compression dispatcher)

## Notes and Other Information
- This is a public interface function (not static), available to other modules
- Part of the compression abstraction layer that allows uniform handling of compressed and uncompressed files
- The compression_spec parameter is accepted for interface consistency but not used in the none implementation
- Initializes private_data to NULL, which will later hold the FILE pointer when a file is opened
- Each function pointer corresponds to a specific file operation, providing a complete file I/O interface
- This function must be called before using any file operations on the CompressFileHandle
- The function follows a pattern where each compression method (none, gzip, lz4, etc.) provides its own initialization function