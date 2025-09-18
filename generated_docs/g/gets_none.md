# gets_none

## Location
src/bin/pg_dump/compress_none.c: 120 - 125

## Overview
The `gets_none` function provides line-based reading functionality for uncompressed files in PostgreSQL's pg_dump utility compression framework.

## Definition
```c
static char *gets_none(char *ptr, int size, CompressFileHandle *CFH)
```

## Detailed Description
The `gets_none` function is a static helper function that implements line-oriented reading for the "none" compression implementation in pg_dump's compression framework. It serves as a thin wrapper around the standard C library `fgets()` function, reading a line of text from an uncompressed file. The function reads characters into the provided buffer until it encounters a newline character, reaches the buffer size limit, or hits end-of-file. The file handle is extracted from the CompressFileHandle structure's private_data field, which contains the standard FILE pointer for uncompressed operations.

## Parameters / Member Variables
- `ptr`: Pointer to the character buffer where the read line will be stored
- `size`: Maximum number of characters to read (including space for null terminator)
- `CFH`: Pointer to a CompressFileHandle structure containing the file handle in its private_data field

## Dependencies
- Functions called/Symbols referenced:
  - fgets (C standard library)
  - CompressFileHandle (structure type)
- Called from (representative examples):
  - InitCompressFileHandleNone

## Notes and Other Information
- This function is part of the "none" compression implementation, handling uncompressed file line reading
- Returns a pointer to the buffer on success, or NULL on error or end-of-file (following fgets() behavior)
- The buffer will be null-terminated and may include the newline character if one was encountered
- The function is static, limiting its scope to the compress_none.c file
- Part of the modular compression system in pg_dump that provides consistent line-reading interface across different compression methods
- Useful for reading text-based dump formats line by line