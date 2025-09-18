# LZ4Stream_read

## Location
[src/bin/pg_dump/compress_lz4.c:610-624](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/compress_lz4.c#L610-L624)

## Overview
Provides a fread() equivalent interface for reading decompressed data from LZ4 compressed files in PostgreSQL's pg_dump utility.

## Definition


## Detailed Description
LZ4Stream_read is a wrapper function that implements the standard C library fread() interface for LZ4 compressed streams. It serves as the primary entry point for reading decompressed data from LZ4 compressed files in pg_dump. The function delegates the actual work to LZ4Stream_read_internal() and handles error reporting by calling pg_fatal() if the read operation fails.

This function is part of PostgreSQL's compression infrastructure for pg_dump, allowing the tool to transparently read from LZ4 compressed backup files. It maintains the familiar fread() semantics while handling the complexities of LZ4 decompression internally.

## Parameters / Member Variables
- : Pointer to the buffer where the decompressed data will be stored
- : Number of bytes to read from the compressed stream
- : Pointer to the CompressFileHandle structure containing the LZ4 state and file information

## Dependencies
- Functions called/Symbols referenced:
  - [LZ4Stream_read_internal](LZ4Stream_read_internal.md) (performs the actual decompression work)
  - [LZ4Stream_get_error](LZ4Stream_get_error.md) (retrieves error messages)
  - [pg_fatal](../p/pg_fatal.md) (reports fatal errors)
- Types referenced:
  - [CompressFileHandle](../C/CompressFileHandle.md) (compression file handle structure)
  - [LZ4State](LZ4State.md) (LZ4 compression state structure)
- Called from (representative examples):
  - No direct callers found in the current codebase

## Notes and Other Information
- This is a static function, meaning it's only accessible within the compress_lz4.c file
- The function follows the fread() convention of returning the number of bytes successfully read
- Error handling is done via pg_fatal(), which terminates the program with an error message
- The function is designed to be used as a callback function pointer in the CompressFileHandle structure
- Part of PostgreSQL's modular compression system that supports multiple compression algorithms