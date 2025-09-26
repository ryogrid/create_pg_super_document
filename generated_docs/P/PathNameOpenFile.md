# PathNameOpenFile

## Location
[src/backend/storage/file/fd.c:1572-1584](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L1572-L1584)

## Overview
PathNameOpenFile is a convenience wrapper function that opens a file using default file permissions, simplifying the interface compared to the more flexible PathNameOpenFilePerm function.

## Definition

```c
File
PathNameOpenFile(const char *fileName, int fileFlags)
```
## Detailed Description
PathNameOpenFile serves as a simplified interface to PathNameOpenFilePerm by automatically providing the default file creation mode (pg_file_create_mode) as the third parameter. This function eliminates the need for callers to explicitly specify file permissions when the default PostgreSQL file creation mode is sufficient. The function is essentially a thin wrapper that promotes code simplicity and consistency across the codebase where custom file permissions are not required.

## Parameters / Member Variables
- : Path to the file to be opened
- : File access flags (read, write, etc.) passed through to the underlying open system call

## Dependencies
- Functions called/Symbols referenced:
  - PathNameOpenFilePerm
  - pg_file_create_mode (global variable)
- Called from (representative examples):
  - logical_rewrite_log_mapping
  - bbsink_server_begin_archive
  - OpenWalSummaryFile
  - ReorderBufferRestoreChanges
  - mdcreate
  - mdopenfork

## Notes and Other Information
This function is part of PostgreSQL's file descriptor management system located in src/backend/storage/file/fd.c. It provides a cleaner API for the common case where default file permissions are acceptable, reducing code duplication throughout the PostgreSQL codebase. The return type 'File' is PostgreSQL's internal file descriptor type used for managing virtual file descriptors.