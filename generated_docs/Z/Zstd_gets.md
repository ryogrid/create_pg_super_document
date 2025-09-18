# Zstd_gets

## Location
src/bin/pg_dump/compress_zstd.c: 404 - 428

## Overview
Reads a line of text from a Zstd-compressed file handle, stopping at newline or end-of-file.

## Definition
```c
static char *Zstd_gets(char *buf, int len, CompressFileHandle *CFH)
```

## Detailed Description
This function implements line-by-line reading from a Zstd-compressed stream. It reads one byte at a time until it encounters a newline character ('\n') or reaches the end of file. The function is specifically designed for reading the list of Large Objects (LOs) during PostgreSQL dump operations. While reading byte-by-byte might seem inefficient, the I/O is buffered at a lower level, making this approach acceptable for its intended use case.

The function ensures proper null-termination of the resulting string and returns NULL if no characters were successfully read (indicating end-of-file or error condition).

## Parameters / Member Variables
- `buf`: Buffer to store the read line (caller-allocated)
- `len`: Maximum number of characters to read (including null terminator)
- `CFH`: Compressed file handle for the Zstd stream

## Dependencies
- Functions called/Symbols referenced:
  - Zstd_read_internal
  - CompressFileHandle
- Called from (representative examples):
  - InitCompressFileHandleZstd (as part of function pointer assignment)

## Notes and Other Information
- This is a static function within the Zstd compression module
- Reads byte-by-byte which is suitable for line-oriented data like LO lists
- Returns the original buffer pointer on success, NULL on failure/EOF
- The function includes an Assert to ensure len > 0 for safety
- Part of the compression abstraction layer in pg_dump utility