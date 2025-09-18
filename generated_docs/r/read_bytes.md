# read_bytes

## Location
[src/bin/pg_combinebackup/reconstruct.c:533-550](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_combinebackup/reconstruct.c#L533-L550)

## Overview
A static utility function that reads a specified number of bytes from a reconstructed file (rfile) into a buffer, with error handling for incomplete reads.

## Definition


## Detailed Description
The  function is a wrapper around the standard  system call that ensures exactly the requested number of bytes are read from the file. It provides robust error handling by checking if the actual number of bytes read matches the expected length. If there's a mismatch, it terminates the program with a detailed error message indicating either a system error (if  returned -1) or a partial read scenario.

This function is part of the pg_combinebackup utility's reconstruction module, specifically designed to handle reliable file I/O operations during backup reconstruction processes.

## Parameters / Member Variables
- : Pointer to an rfile structure containing the file descriptor and filename for the file being read
- : Pointer to the destination buffer where the read data will be stored
- : Number of bytes to read from the file (unsigned integer)

## Dependencies
- Functions called/Symbols referenced:
  -  (system call)
  -  (PostgreSQL error reporting function)
  -  (struct type for reconstructed file handling)

- Called from (representative examples):
  -  (multiple calls at lines 463, 469, 475, 486)
  -  (in shared tuple store operations)
  -  (during parallel scanning)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the reconstruct.c file
- The function uses  for error reporting, which terminates the program immediately upon encountering any read errors
- Two types of errors are handled: system-level read errors (errno-based) and incomplete reads
- The function is designed for binary file operations where exact byte counts are critical
- Used primarily in backup reconstruction scenarios where data integrity is paramount