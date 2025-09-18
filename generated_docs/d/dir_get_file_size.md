# dir_get_file_size

## Location
[src/bin/pg_basebackup/walmethods.c:565-583](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/walmethods.c#L565-L583)

## Overview
Returns the size of a file within a directory-based WAL writing method implementation.

## Definition


## Detailed Description
This function is a static implementation of the file size retrieval operation for the directory-based WAL writing method. It constructs the full file path by combining the base directory from the DirectoryMethodData structure with the provided pathname, then uses the system's stat() function to retrieve the file size. The function is designed to work within PostgreSQL's WAL (Write-Ahead Logging) backup infrastructure, specifically for pg_basebackup operations that write WAL files directly to a directory.

## Parameters / Member Variables
- : Pointer to the WalWriteMethod structure containing the directory method data
- : Relative path of the file whose size is to be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - snprintf (system function)
  - [stat](../s/stat.md) (system function)
- Data structures used:
  - [WalWriteMethod](../W/WalWriteMethod.md)
  - [DirectoryMethodData](../D/DirectoryMethodData.md)
  - struct stat
- Called from:
  - Used as a function pointer in WAL writing method operations

## Notes and Other Information
- Returns the file size in bytes as ssize_t, or -1 on error
- Sets wwmethod->lasterrno to errno if stat() fails
- The function constructs the full path using MAXPGPATH buffer size limit
- Part of the directory-based WAL writing method implementation for pg_basebackup
- Static function, only accessible within the walmethods.c compilation unit