# bakcopy

## Location
[src/tools/pg_bsd_indent/indent.c:1203-1242](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/tools/pg_bsd_indent/indent.c#L1203-L1242)

## Overview
The bakcopy function creates a backup copy of the input file and swaps the input/output file handles so that the original file becomes the output target.

## Definition


## Detailed Description
This function is part of the pg_bsd_indent tool and implements in-place file editing functionality. It creates a backup file with a ".BAK" extension in the same directory as the original input file. The function then copies the entire contents of the input file to the backup, closes the original input, and reopens the backup as the new input source. Finally, it opens the original filename for writing as the output destination. This allows the indent tool to read from the backup while writing the formatted output to the original filename, effectively performing in-place editing with backup preservation.

## Parameters / Member Variables
- No parameters (operates on global variables: in_name, input, output, bakfile)

## Dependencies
- Functions called/Symbols referenced:
  - read (read file contents)
  - write (write file contents)
  - close (close file descriptor)
  - fopen (open file streams)
  - unlink (remove backup file on error)
- Called from (representative examples):
  - [main](../m/main.md) (in pg_bsd_indent when backup is requested)

## Notes and Other Information
- Uses a simple filename transformation: appends ".BAK" to the base filename
- Creates backup file with 0600 permissions (read/write for owner only)
- Handles errors by calling err() function which terminates the program
- Removes backup file if output file creation fails
- Buffer size for file copying is set to 8KB
- The function assumes global variables in_name, input, output, and bakfile are properly initialized