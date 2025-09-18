# LZ4Stream_get_error

## Location
[src/bin/pg_dump/compress_lz4.c:330-353](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/compress_lz4.c#L330-L353)

## Overview
Retrieves a human-readable error message from the last error that occurred in an LZ4 compression/decompression stream operation.

## Definition
```c
static const char *
LZ4Stream_get_error(CompressFileHandle *CFH)
```

## Detailed Description
This static function serves as an error reporting mechanism for LZ4 stream operations within PostgreSQL's pg_dump utility. It examines the error state stored in the LZ4State structure and returns an appropriate error message. The function intelligently distinguishes between LZ4-specific errors and general system errors by checking if the stored error code is an LZ4 error using the LZ4F_isError() function. If it's an LZ4-specific error, it uses LZ4F_getErrorName() to get the LZ4 library's own error description. Otherwise, it falls back to strerror(errno) for system-level error messages.

## Parameters / Member Variables
- `CFH`: Pointer to a CompressFileHandle containing the LZ4State private data structure with error information

## Dependencies
- Functions called/Symbols referenced:
  - LZ4F_isError (LZ4 library function)
  - LZ4F_getErrorName (LZ4 library function) 
  - strerror (standard C library function)
- Types referenced:
  - [CompressFileHandle](../C/CompressFileHandle.md)
  - [LZ4State](LZ4State.md)
- Called from:
  - [LZ4Stream_read](LZ4Stream_read.md) (at compress_lz4.c:616)
  - [LZ4Stream_getc](LZ4Stream_getc.md) (at compress_lz4.c:633)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the compress_lz4.c file
- The function relies on the errcode field in the LZ4State structure being properly set by other LZ4 operations
- It provides a unified error reporting interface that abstracts away the distinction between LZ4 library errors and system errors
- The function is typically called after other LZ4 stream operations fail to provide meaningful error messages to the user