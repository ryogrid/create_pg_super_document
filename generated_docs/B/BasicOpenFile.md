# BasicOpenFile

## Location
src/backend/storage/file/fd.c: 1084 - 1105

## Overview
A convenience wrapper function that opens a file using default permissions by calling BasicOpenFilePerm with PostgreSQL's standard file creation mode.

## Definition
```c
int BasicOpenFile(const char *fileName, int fileFlags)
```

## Detailed Description
BasicOpenFile provides a simplified interface for opening files when the default PostgreSQL file permissions are acceptable. It internally calls BasicOpenFilePerm with `pg_file_create_mode` as the permission parameter, eliminating the need for callers to specify file permissions explicitly.

This function is commonly used throughout PostgreSQL for opening files where the standard database file permissions (typically 0600 - readable and writable by owner only) are appropriate. It's particularly useful for opening WAL files, control files, and other PostgreSQL-managed files that should have consistent permissions.

## Parameters / Member Variables
- `fileName`: Path to the file to be opened
- `fileFlags`: File access flags (O_RDONLY, O_WRONLY, O_RDWR, O_CREAT, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - BasicOpenFilePerm
  - pg_file_create_mode (global variable for default file permissions)
- Called from (representative examples):
  - XLogFileInitInternal
  - XLogFileInit  
  - XLogFileOpen
  - WriteControlFile
  - ReadControlFile
  - XLogFileRead
  - wal_segment_open
  - WalSndSegmentOpen
  - AlterSystemSetConfigFile
  - update_controlfile

## Notes and Other Information
- This is a thin wrapper around BasicOpenFilePerm for convenience
- Uses PostgreSQL's standard file creation mode (pg_file_create_mode) automatically
- Commonly used for WAL files, control files, and configuration files
- Returns a file descriptor on success, or -1 on error
- The actual file opening logic and error handling is implemented in BasicOpenFilePerm
- Helps maintain consistent file permissions across PostgreSQL-managed files