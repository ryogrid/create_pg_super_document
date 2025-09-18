# close_none

## Location
[src/bin/pg_dump/compress_none.c:144-162](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/compress_none.c#L144-L162)

## Overview
The `close_none` function provides file closing functionality with error handling for uncompressed files in PostgreSQL's pg_dump utility compression framework.

## Definition
```c
static bool close_none(CompressFileHandle *CFH)
```

## Detailed Description
The `close_none` function is a static helper function that implements file closing for the "none" compression implementation in pg_dump's compression framework. It safely closes an uncompressed file by extracting the FILE pointer from the CompressFileHandle structure and calling the standard C library `fclose()` function. The function includes proper error handling, logging any close failures without terminating the program (unlike some other functions in this module). It also performs cleanup by setting the private_data pointer to NULL after closing, preventing potential use-after-close errors. The function returns a boolean indicating success or failure of the close operation.

## Parameters / Member Variables
- `CFH`: Pointer to a CompressFileHandle structure containing the file handle in its private_data field

## Dependencies
- Functions called/Symbols referenced:
  - fclose (C standard library)
  - pg_log_error (PostgreSQL error logging function)
  - [CompressFileHandle](../C/CompressFileHandle.md) (structure type)
- Called from (representative examples):
  - [InitCompressFileHandleNone](../I/InitCompressFileHandleNone.md)

## Notes and Other Information
- This function is part of the "none" compression implementation, handling uncompressed file closure
- Returns true on successful close, false on failure
- Uses pg_log_error() instead of pg_fatal(), allowing the program to continue after close errors
- Safely handles NULL file pointers by checking fp before attempting to close
- Clears the private_data pointer to prevent use-after-close scenarios
- The function is static, limiting its scope to the compress_none.c file
- Part of the modular compression system in pg_dump that provides consistent file closing interface across different compression methods
- errno is cleared before the fclose() call to ensure accurate error reporting