# pgfnames

## Location
src/common/pgfnames.c: 37 - 85

## Overview
Lists all file and directory names in a specified directory path, returning a dynamically allocated array of strings.

## Definition


## Detailed Description
The  function reads a directory specified by the  parameter and returns an array of strings containing all the names of files and directories in that location, excluding the special entries "." and "..". It dynamically allocates memory for the array and strings, starting with space for 200 entries and doubling the size as needed. The function handles directory access errors gracefully by logging warnings and returning NULL on failure. The returned array is NULL-terminated to facilitate iteration.

## Parameters / Member Variables
- : The directory path to read from, specified as a const char pointer

## Dependencies
- Functions called/Symbols referenced:
  - opendir: Opens the directory for reading
  - readdir: Reads directory entries sequentially
  - closedir: Closes the directory handle
  - palloc: Allocates initial memory for the filename array
  - repalloc: Reallocates memory when the array needs to grow
  - pstrdup: Duplicates strings for filename storage
  - pg_log_warning: Logs warning messages for error conditions
- Called from (representative examples):
  - scan_available_timezones: Used in initdb to scan timezone directories

## Notes and Other Information
- Caller must call pgfnames_cleanup() to free the allocated memory
- Initial array size is 200 entries, sufficient for many small databases
- Array doubles in size when more space is needed
- Function sets errno to 0 before readdir() calls for proper error detection
- Returns NULL on directory open failure
- Memory allocation uses PostgreSQL's palloc/repalloc memory management
- All filenames are duplicated using pstrdup for independent memory management