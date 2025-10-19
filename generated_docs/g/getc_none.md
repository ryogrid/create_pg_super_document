# getc_none

## Location
[src/bin/pg_dump/compress_none.c:126-143](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/compress_none.c#L126-L143)

## Overview
The `getc_none` function provides character-level reading functionality with error handling for uncompressed files in PostgreSQL's pg_dump utility compression framework.

## Definition
```c
static int getc_none(CompressFileHandle *CFH)
```

## Detailed Description
The `getc_none` function is a static helper function that implements character-oriented reading for the "none" compression implementation in pg_dump's compression framework. It wraps the standard C library `fgetc()` function with comprehensive error handling and reporting. Unlike the standard `fgetc()`, this function terminates the program with a fatal error when encountering EOF, whether due to an actual end-of-file condition or a read error. This behavior ensures that unexpected end-of-file conditions are caught and reported appropriately in the context of pg_dump operations where incomplete reads typically indicate serious problems.

## Parameters / Member Variables
- `CFH`: Pointer to a CompressFileHandle structure containing the file handle in its private_data field

## Dependencies
- Functions called/Symbols referenced:
  - fgetc (C standard library)
  - feof (C standard library)
  - [pg_fatal](../p/pg_fatal.md) (PostgreSQL error reporting function)
  - [CompressFileHandle](../C/CompressFileHandle.md) (structure type)
- Called from (representative examples):
  - [InitCompressFileHandleNone](../I/InitCompressFileHandleNone.md)

## Notes and Other Information
- This function is part of the "none" compression implementation, handling uncompressed file character reading
- Unlike standard fgetc(), this function does not return EOF - instead it calls pg_fatal() on any EOF condition
- Distinguishes between actual end-of-file and read errors using feof() check
- The function is static, limiting its scope to the compress_none.c file
- Part of the modular compression system in pg_dump that provides consistent character-reading interface across different compression methods
- The error-on-EOF behavior makes this function unsuitable for cases where EOF is an expected condition
- Returns the character read as an int (0-255 range for valid characters)

## Simplified Source

```c
static int
getc_none(CompressFileHandle *CFH)
{
    FILE *fp = (FILE *) CFH->private_data;
    int ret;

    // Read character and handle EOF as fatal error
    ret = fgetc(fp);
    if (ret == EOF) {
        if (!feof(fp))
            pg_fatal("could not read from input file: %m");
        else
            pg_fatal("could not read from input file: end of file");
    }

    return ret;
}
```