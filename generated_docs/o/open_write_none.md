# open_write_none

## Location
[src/bin/pg_dump/compress_none.c:185-200](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/compress_none.c#L185-L200)

## Overview
A static function that opens an uncompressed file for writing using a file path in PostgreSQL's pg_dump compression abstraction layer.

## Definition


## Detailed Description
The `open_write_none` function is a specialized file opening function for uncompressed files ("none" compression mode) in pg_dump, specifically designed for write operations. Unlike `open_none` which supports both file paths and file descriptors, this function only works with file paths. It opens the specified file using `fopen()` with the provided mode and stores the resulting FILE pointer in the CompressFileHandle's private_data member. The function returns true on success and false on failure.

## Parameters / Member Variables
- `path`: File path to open for writing
- `mode`: File opening mode string (typically "w", "wb", "a", etc. for write modes)
- `CFH`: Pointer to CompressFileHandle structure where the opened FILE pointer will be stored

## Dependencies
- Functions called/Symbols referenced:
  - Assert (assertion macro)
  - fopen (standard C library function)
  - [CompressFileHandle](../C/CompressFileHandle.md) (structure type)
- Called from (representative examples):
  - [InitCompressFileHandleNone](../I/InitCompressFileHandleNone.md) (used to initialize function pointer)

## Notes and Other Information
- This function is part of the "none" compression implementation for uncompressed files
- It's a static function, only accessible within compress_none.c
- The function assumes CFH->private_data is initially NULL (enforced by Assert)
- Simpler than `open_none` as it only handles file path-based opening, not file descriptors
- Returns boolean success status - callers should check the return value
- The opened FILE pointer is stored in CFH->private_data for later use by other compression functions
- Typically used for write operations in the compression abstraction layer