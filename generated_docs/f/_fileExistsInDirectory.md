# _fileExistsInDirectory

## Location
src/bin/pg_dump/pg_backup_archiver.c: 2209 - 2220

## Overview
_fileExistsInDirectory is a static utility function that checks whether a specific file exists within a given directory and is a regular file.

## Definition
```c
static bool _fileExistsInDirectory(const char *dir, const char *filename)
```

## Detailed Description
This function constructs a full path by concatenating a directory path and filename, then uses the stat() system call to check if the resulting path exists and refers to a regular file. It includes safety checks to prevent buffer overflow when constructing the path and uses the S_ISREG macro to verify the file type.

## Parameters / Member Variables
- `dir`: const char pointer - the directory path to search in
- `filename`: const char pointer - the name of the file to look for

## Dependencies
- Functions called/Symbols referenced:
  - S_ISREG (macro for checking regular file type)
  - snprintf (standard C library function)
  - stat (POSIX system call)
  - pg_fatal (PostgreSQL error reporting function)
- Called from (representative examples):
  - _discoverArchiveFormat

## Notes and Other Information
- Static function, only accessible within pg_backup_archiver.c
- Uses MAXPGPATH constant to limit path length and prevent buffer overflows
- Returns true only for regular files, not directories, symlinks, or other file types
- Used primarily in archive format detection logic to check for specific marker files
- Path construction uses forward slash separator, appropriate for Unix-like systems