# get_error_none

## Location
[src/bin/pg_dump/compress_none.c:114-119](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/compress_none.c#L114-L119)

## Overview
The `get_error_none` function provides error message retrieval functionality for the uncompressed file handling implementation in PostgreSQL's pg_dump utility.

## Definition
```c
static const char *get_error_none(CompressFileHandle *CFH)
```

## Detailed Description
The `get_error_none` function is a static helper function that serves as the error reporting mechanism for the "none" compression implementation in pg_dump's compression framework. It simply wraps the standard C library `strerror()` function to convert the current `errno` value into a human-readable error message string. The function follows the compression framework's interface by accepting a CompressFileHandle parameter, though it doesn't actually use this parameter since uncompressed file operations rely on standard C library error reporting via errno.

## Parameters / Member Variables
- `CFH`: Pointer to a CompressFileHandle structure (parameter is present for interface consistency but not used in the implementation)

## Dependencies
- Functions called/Symbols referenced:
  - strerror (C standard library)
  - [CompressFileHandle](../C/CompressFileHandle.md) (structure type)
- Called from (representative examples):
  - [InitCompressFileHandleNone](../I/InitCompressFileHandleNone.md)

## Notes and Other Information
- This function is part of the "none" compression implementation, providing error reporting for uncompressed file operations
- The function parameter CFH is unused but required to match the compression framework interface
- Returns a pointer to a static string provided by strerror(), so the returned string should not be modified or freed
- The function is static, limiting its scope to the compress_none.c file
- Part of the modular compression system in pg_dump that allows different compression methods to implement their own error reporting mechanisms

## Simplified Source

```c
static const char *
get_error_none(CompressFileHandle *CFH)
{
    // Return standard error string for current errno
    return strerror(errno);
}
```