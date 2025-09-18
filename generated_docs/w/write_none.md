# write_none

## Location
src/bin/pg_dump/compress_none.c: 100 - 113

## Overview
The  function provides uncompressed file writing functionality for PostgreSQL's pg_dump utility, serving as the write operation handler when no compression is applied.

## Definition


## Detailed Description
The  function is a static helper function that handles uncompressed data writing in the pg_dump compression framework. It wraps the standard C library  function with proper error handling and reporting. The function writes the specified amount of data from a buffer to a file handle stored within a CompressFileHandle structure. If the write operation fails or doesn't write the expected number of bytes, the function terminates the program with a fatal error message.

## Parameters / Member Variables
- : Pointer to the data buffer to be written to the file
- : Number of bytes to write from the buffer
- : Pointer to a CompressFileHandle structure containing the file handle in its private_data field

## Dependencies
- Functions called/Symbols referenced:
  - fwrite (C standard library)
  - pg_fatal (PostgreSQL error reporting function)
  - CompressFileHandle (structure type)
- Called from (representative examples):
  - InitCompressFileHandleNone

## Notes and Other Information
- This function is part of the "none" compression implementation, meaning no compression is applied
- Error handling includes checking both the return value and errno to distinguish between partial writes and storage space issues
- The function uses the ENOSPC error code when fwrite returns a short write count but errno is not set
- The function is static, limiting its scope to the compress_none.c file
- Part of the modular compression system in pg_dump that allows different compression methods to be plugged in