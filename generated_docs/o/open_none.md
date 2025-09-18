# open_none

## Location
[src/bin/pg_dump/compress_none.c:169-184](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/compress_none.c#L169-L184)

## Overview
A static function that opens an uncompressed file for reading or writing as part of PostgreSQL's pg_dump compression abstraction layer.

## Definition


## Detailed Description
The `open_none` function handles opening files without compression ("none" compression mode) in pg_dump. It supports two methods of opening files: by file path or by file descriptor. If a valid file descriptor is provided (fd >= 0), it duplicates the descriptor and opens it using `fdopen()`. Otherwise, it opens the file using the provided path with `fopen()`. The resulting FILE pointer is stored in the CompressFileHandle's private_data member. The function returns true on success and false on failure.

## Parameters / Member Variables
- `path`: File path to open (used when fd < 0)
- `fd`: File descriptor to use (when >= 0, takes precedence over path)
- `mode`: File opening mode string (e.g., "r", "w", "rb", "wb")
- `CFH`: Pointer to CompressFileHandle structure where the opened FILE pointer will be stored

## Dependencies
- Functions called/Symbols referenced:
  - Assert (assertion macro)
  - fdopen (standard C library function)
  - dup (system call for duplicating file descriptor)
  - fopen (standard C library function)
  - [CompressFileHandle](../C/CompressFileHandle.md) (structure type)
- Called from (representative examples):
  - [InitCompressFileHandleNone](../I/InitCompressFileHandleNone.md) (used to initialize function pointer)

## Notes and Other Information
- This function is part of the "none" compression implementation for uncompressed files
- It's a static function, only accessible within compress_none.c
- The function assumes CFH->private_data is initially NULL (enforced by Assert)
- When using file descriptor, it duplicates the descriptor to avoid conflicts with the original
- Returns boolean success status - callers should check the return value
- The opened FILE pointer is stored in CFH->private_data for later use by other compression functions