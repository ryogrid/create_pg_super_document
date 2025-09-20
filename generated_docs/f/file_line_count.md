# file_line_count

## Location
[src/test/regress/pg_regress.c:1281-1301](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/regress/pg_regress.c#L1281-L1301)

## Overview
A utility function that counts the number of lines in a specified file by reading through it character by character.

## Definition

```c
struct stat st;
```
## Detailed Description
The  function opens a file in read mode and counts the number of lines by iterating through each character and incrementing a counter whenever a newline character ('\n') is encountered. The function reads the entire file character by character using  until EOF is reached. If the file cannot be opened, it logs an error message and returns -1 to indicate failure.

This function is part of the PostgreSQL regression testing infrastructure, used to analyze test result files during the comparison process.

## Parameters / Member Variables
- : A null-terminated string containing the path to the file whose lines are to be counted

## Dependencies
- Functions called/Symbols referenced:
  - fopen (standard C library function for opening files)
  - diag (PostgreSQL regression test diagnostic function for error reporting)
  - fgetc (standard C library function for reading characters)
  - fclose (standard C library function for closing files)
- Called from (representative examples):
  - [results_differ](../r/results_differ.md) (used multiple times to count lines in expected and actual result files during regression testing)

## Notes and Other Information
- Returns -1 on error (file cannot be opened)
- Returns the number of lines as an integer on success
- Counts lines by detecting newline characters ('\n')
- Uses "r" mode for file opening, which is read-only
- Properly closes the file handle after counting lines
- Part of the PostgreSQL regression testing infrastructure
- Used in test result comparison to provide detailed information about differences
- The function counts actual newline characters, so files without a trailing newline may report one fewer line than expected by some text editors