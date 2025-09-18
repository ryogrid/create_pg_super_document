# read_none

## Location
src/bin/pg_dump/compress_none.c: 87 - 99

## Overview
Reads data from an uncompressed file, implementing the compress file API for reading operations when no compression is used in pg_dump.

## Definition
static size_t read_none(void *ptr, size_t size, CompressFileHandle *CFH)

## Detailed Description
This function implements the file reading functionality for the "none" compression method in the compress file API. It directly reads from a standard FILE pointer without performing any decompression. The function uses the standard C library fread() function to read data and includes error checking to ensure successful read operations. If a read error occurs, it terminates the program with a fatal error message.

## Parameters / Member Variables
- : Pointer to the buffer where read data will be stored
- : Number of bytes to read from the file
- : Compress file handle containing the file pointer and other state information

## Dependencies
- Functions called/Symbols referenced:
  - [CompressFileHandle](../C/CompressFileHandle.md) (struct type)
  - fread (standard C library function)
  - ferror (standard C library function)
  - [pg_fatal](../p/pg_fatal.md) (PostgreSQL error reporting function)
- Called from (representative examples):
  - [InitCompressFileHandleNone](../I/InitCompressFileHandleNone.md)

## Notes and Other Information
- This function is part of the compress file API for handling uncompressed files
- Uses the private_data field of CompressFileHandle as a FILE pointer
- Includes error checking with pg_fatal for read failures
- Returns the number of bytes actually read, which may be less than requested
- Located in src/bin/pg_dump/compress_none.c:87-99