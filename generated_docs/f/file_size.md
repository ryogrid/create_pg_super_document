# file_size

## Location
src/test/regress/pg_regress.c: 1261 - 1280

## Overview
A utility function that determines the size in bytes of a specified file.

## Definition


## Detailed Description
The  function opens a file in read mode and returns its size in bytes. It uses standard C library functions to seek to the end of the file and retrieve the current position, which corresponds to the file size. If the file cannot be opened, it logs an error message and returns -1 to indicate failure.

The function is implemented as a static function within the pg_regress.c file, making it a local utility function for PostgreSQL regression testing infrastructure.

## Parameters / Member Variables
- : A null-terminated string containing the path to the file whose size is to be determined

## Dependencies
- Functions called/Symbols referenced:
  - fopen (standard C library function for opening files)
  - diag (PostgreSQL regression test diagnostic function for error reporting)
  - fseek (standard C library function for file positioning)
  - ftell (standard C library function for getting current file position)
  - fclose (standard C library function for closing files)
- Called from (representative examples):
  - [run_diff](../r/run_diff.md) (used to check file sizes during regression testing)

## Notes and Other Information
- Returns -1 on error (file cannot be opened)
- Returns the file size in bytes as a long integer on success
- Uses "r" mode for file opening, which is read-only
- Properly closes the file handle after determining the size
- Part of the PostgreSQL regression testing infrastructure
- Error handling includes diagnostic output using the  function