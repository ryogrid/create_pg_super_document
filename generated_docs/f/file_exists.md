# file_exists

## Location
src/test/regress/pg_regress.c: 1302 - 1312

## Overview
A utility function that checks whether a specified file exists and can be opened for reading.

## Definition


## Detailed Description
The  function provides a simple way to test for file existence by attempting to open the file in read mode. If the file can be successfully opened, it immediately closes the file and returns true. If the file cannot be opened (either because it doesn't exist or due to permission issues), the function returns false.

This is a straightforward implementation that leverages the standard C library's file opening mechanism to determine file accessibility. The function is used throughout PostgreSQL's testing infrastructure to verify the presence of expected files before processing them.

## Parameters / Member Variables
- : A null-terminated string containing the path to the file whose existence is to be checked

## Dependencies
- Functions called/Symbols referenced:
  - fopen (standard C library function for opening files)
  - fclose (standard C library function for closing files)
- Called from (representative examples):
  - [isolation_start_test](../i/isolation_start_test.md) (used in isolation testing to check for test files)
  - [results_differ](../r/results_differ.md) (used to verify that result files exist before comparison)
  - [psql_start_test](../p/psql_start_test.md) (used in psql testing to check for test files)

## Notes and Other Information
- Returns true if the file exists and can be opened for reading
- Returns false if the file does not exist or cannot be accessed
- Uses "r" mode for file opening, which requires read permissions
- Properly closes the file handle immediately after successful opening
- Part of the PostgreSQL testing infrastructure
- This function tests both existence and readability - a file that exists but cannot be read will return false
- Does not distinguish between "file does not exist" and "file exists but cannot be read" - both conditions return false
- Used across multiple testing modules including isolation testing and regression testing