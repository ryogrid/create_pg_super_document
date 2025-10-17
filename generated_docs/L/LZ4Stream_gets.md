# LZ4Stream_gets

## Location
[src/bin/pg_dump/compress_lz4.c:645-673](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/compress_lz4.c#L645-L673)

## Overview
Provides a fgets() equivalent interface for reading line-oriented data from LZ4 compressed files in PostgreSQL's pg_dump utility.

## Definition

```c
static char *
LZ4Stream_gets(char *ptr, int size, CompressFileHandle *CFH)
```
## Detailed Description
LZ4Stream_gets implements the standard C library fgets() interface for LZ4 compressed streams. It reads characters from the compressed stream until either a newline character is encountered, the buffer is full (size-1 characters), or end-of-file/error occurs. The function uses LZ4Stream_read_internal() with the eol_flag set to true, enabling line-oriented reading that stops at newline characters.

This function is part of PostgreSQL's compression infrastructure for pg_dump, allowing line-by-line reading from LZ4 compressed backup files. It maintains the familiar fgets() semantics, including NULL-termination of the result string, while handling the complexities of LZ4 decompression internally.

## Parameters / Member Variables
- `*ptr`: Pointer to the character buffer where the line will be stored
- `size`: Maximum number of characters to read (including the null terminator)
- `*CFH`: Pointer to the CompressFileHandle structure containing the LZ4 state and file information
## Dependencies
- Functions called/Symbols referenced:
  - [LZ4Stream_read_internal](LZ4Stream_read_internal.md) (performs the actual decompression work with eol_flag=true)
- Types referenced:
  - [CompressFileHandle](../C/CompressFileHandle.md) (compression file handle structure)
  - [LZ4State](LZ4State.md) (LZ4 compression state structure)
- Called from (representative examples):
  - No direct callers found in the current codebase

## Notes and Other Information
- This is a static function, meaning it's only accessible within the compress_lz4.c file
- The function follows the fgets() convention: returns ptr on success, NULL on EOF or error
- Always null-terminates the string at position [ret-1] when successfully reading data
- Reads at most size-1 characters to leave space for the null terminator
- Uses line-oriented reading (eol_flag=true) which stops at newline characters
- The function is designed to be used as a callback function pointer in the CompressFileHandle structure
- Part of PostgreSQL's modular compression system that supports multiple compression algorithms
- Unlike other LZ4Stream functions, this one handles errors gracefully by returning NULL rather than calling pg_fatal()
- The implementation treats both EOF and error conditions identically, returning NULL in both cases

## Simplified Source

```c
static char *
LZ4Stream_gets(char *ptr, int size, CompressFileHandle *CFH)
{
    LZ4State *state = (LZ4State *) CFH->private_data;
    int ret;

    // Read line data using internal function with EOL detection
    ret = LZ4Stream_read_internal(state, ptr, size - 1, true);

    // Return NULL for EOF or error (both treated the same)
    if (ret <= 0)
        return NULL;

    // Null-terminate the string at the end
    ptr[ret - 1] = '\0';

    return ptr;
}
```