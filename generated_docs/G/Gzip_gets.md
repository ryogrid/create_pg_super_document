# Gzip_gets

## Location
src/bin/pg_dump/compress_gzip.c: 319 - 326

## Overview
Reads a line of text from a gzip-compressed file handle as a simple wrapper around zlib's gzgets function.

## Definition
```c
static char *Gzip_gets(char *ptr, int size, CompressFileHandle *CFH)
```

## Detailed Description
This function provides a direct wrapper around zlib's gzgets() function for reading a line of text from a compressed file. It reads up to size-1 characters from the gzip file into the provided buffer, stopping at a newline character or EOF. The function is implemented as a simple pass-through to gzgets() without additional error handling, relying on the underlying zlib library to handle error conditions and return appropriate values.

## Parameters / Member Variables
- `ptr`: Pointer to the character buffer where the line will be stored
- `size`: Maximum number of characters to read (including null terminator)
- `CFH`: Compressed file handle containing the gzip file pointer in private_data

## Dependencies
- Functions called/Symbols referenced:
  - gzgets
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Returns ptr on success, NULL on error or EOF (as per gzgets() behavior)
- Part of the Compress File API for handling gzip-compressed files in pg_dump/pg_restore
- Uses CompressFileHandle structure to access the underlying gzFile
- Unlike other Gzip_* functions, this does not include explicit error handling with pg_fatal()
- Reads until newline, EOF, or buffer limit is reached
- Null-terminates the result string automatically (handled by gzgets)