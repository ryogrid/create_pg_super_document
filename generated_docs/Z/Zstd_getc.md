# Zstd_getc

## Location
[src/bin/pg_dump/compress_zstd.c:394-403](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/compress_zstd.c#L394-L403)

## Overview
Zstd_getc is a static function that reads a single character from a Zstd-compressed file, providing a character-based interface similar to the standard library's getc() function.

## Definition
static int Zstd_getc(CompressFileHandle *CFH)

## Detailed Description
This function implements single-character reading from Zstd-compressed files by leveraging the compressed file handle's read function. It serves as a simple wrapper that reads exactly one byte using the CFH's read_func and returns it as an integer, following the standard C library convention for character reading functions. The function provides fatal error handling when the read operation fails or reaches end-of-file unexpectedly, ensuring robust operation within pg_dump's compressed file handling infrastructure.

## Parameters / Member Variables
- : Compressed file handle containing the read function pointer and associated compression context

## Dependencies
- Functions called/Symbols referenced:
  - [CompressFileHandle](../C/CompressFileHandle.md) (struct type)
  - CFH->read_func (function pointer for reading compressed data)
  - [pg_fatal](../p/pg_fatal.md)() (PostgreSQL fatal error function)
- Called from (representative examples):
  - [InitCompressFileHandleZstd](../I/InitCompressFileHandleZstd.md) (assigned as getc function pointer)

## Notes and Other Information
- Returns the character as an unsigned char cast to int, following standard C library conventions
- Uses pg_fatal() for error handling, making read failures terminal rather than recoverable
- Designed to integrate with PostgreSQL's compressed file API, providing a consistent interface for character-based reading
- The function assumes that reaching end-of-file is an error condition in the context where it's used
- Relies on the CompressFileHandle's read_func to handle the actual decompression and data reading
- Simple implementation that delegates the complex decompression logic to the underlying read function