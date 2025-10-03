# LZ4Stream_getc

## Location
[src/bin/pg_dump/compress_lz4.c:625-644](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/compress_lz4.c#L625-L644)

## Overview
Provides a fgetc() equivalent interface for reading a single character from LZ4 compressed files in PostgreSQL's pg_dump utility.

## Definition

```c
static int
LZ4Stream_getc(CompressFileHandle *CFH)
```
## Detailed Description
LZ4Stream_getc implements the standard C library fgetc() interface for LZ4 compressed streams. It reads exactly one byte (character) from the compressed stream and returns it as an unsigned char cast to an int. The function uses LZ4Stream_read_internal() to perform the actual decompression work and handles both normal operation and end-of-file conditions with appropriate error reporting.

This function is part of PostgreSQL's compression infrastructure for pg_dump, allowing byte-by-byte reading from LZ4 compressed backup files. It maintains the familiar fgetc() semantics while handling the complexities of LZ4 decompression internally.

## Parameters / Member Variables
- `*CFH`: Pointer to the CompressFileHandle structure containing the LZ4 state and file information
## Dependencies
- Functions called/Symbols referenced:
  - [LZ4Stream_read_internal](LZ4Stream_read_internal.md) (performs the actual decompression work)
  - [LZ4Stream_eof](LZ4Stream_eof.md) (checks for end of file condition)
  - [LZ4Stream_get_error](LZ4Stream_get_error.md) (retrieves error messages)
  - [pg_fatal](../p/pg_fatal.md) (reports fatal errors)
- Types referenced:
  - [CompressFileHandle](../C/CompressFileHandle.md) (compression file handle structure)
  - [LZ4State](LZ4State.md) (LZ4 compression state structure)
- Called from (representative examples):
  - No direct callers found in the current codebase

## Notes and Other Information
- This is a static function, meaning it's only accessible within the compress_lz4.c file
- The function follows the fgetc() convention of returning an unsigned char value cast to int
- Returns the character read on success, or causes program termination on error
- Uses sophisticated error handling to distinguish between read errors and end-of-file conditions
- The function is designed to be used as a callback function pointer in the CompressFileHandle structure
- Part of PostgreSQL's modular compression system that supports multiple compression algorithms
- The function always terminates the program via pg_fatal() on any error condition, following pg_dump's fail-fast error handling philosophy