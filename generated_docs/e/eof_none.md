# eof_none

## Location
[src/bin/pg_dump/compress_none.c:163-168](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/compress_none.c#L163-L168)

## Overview
A static function that checks if the end-of-file indicator is set for an uncompressed file handle in PostgreSQL's pg_dump utility.

## Definition

```c
static bool
eof_none(CompressFileHandle *CFH)
```
## Detailed Description
The  function is a simple wrapper around the standard C library's  function. It operates on uncompressed files ("none" compression) and provides a consistent interface for checking end-of-file status within the compression abstraction layer of pg_dump. The function casts the private_data member of the CompressFileHandle to a FILE pointer and calls  on it, returning true if the end-of-file indicator is set.

## Parameters / Member Variables
- `*CFH`: Pointer to a CompressFileHandle structure containing the file handle and compression-related metadata. The private_data member is expected to contain a FILE pointer for uncompressed files.
## Dependencies
- Functions called/Symbols referenced:
  - feof (standard C library function)
  - [CompressFileHandle](../C/CompressFileHandle.md) (structure type)
- Called from (representative examples):
  - [InitCompressFileHandleNone](../I/InitCompressFileHandleNone.md) (used to initialize function pointer)

## Notes and Other Information
- This function is part of the "none" compression implementation, which handles uncompressed files
- It's a static function, meaning it's only accessible within the compress_none.c file
- The function assumes that CFH->private_data contains a valid FILE pointer
- Returns a boolean value: true if end-of-file is reached, false otherwise
- This function is typically assigned to a function pointer in compression handle initialization