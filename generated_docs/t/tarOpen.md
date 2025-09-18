# tarOpen

## Location
src/bin/pg_dump/pg_backup_tar.c: 302 - 397

## Overview
Opens a file within a tar archive for reading or writing, creating a TAR_MEMBER structure to handle the file operations.

## Definition


## Detailed Description
The tarOpen function is a core component of PostgreSQL's tar-based backup format handling. It provides a unified interface for opening files within tar archives for both reading and writing operations. When opening for reading (mode 'r'), it locates the specified file within the archive using _tarPositionTo. When opening for writing (mode 'w'), it creates a temporary file that will later be written to the tar archive.

The function handles platform-specific differences, particularly between Unix-like systems and Windows for temporary file creation. It also enforces that compression is not supported with the tar format, failing if compression is attempted.

## Parameters / Member Variables
- : ArchiveHandle pointer containing archive context and format-specific data
- : Name of the file to open within the tar archive (can be NULL for read mode to get any available file)
- : File access mode - 'r' for reading, 'w' for writing

## Dependencies
- Functions called/Symbols referenced:
  - _tarPositionTo
  - pg_malloc0_object
  - pg_strdup
  - tmpfile (Unix) / _tempnam + open (Windows)
  - umask
  - fdopen
- Called from (representative examples):
  - _StartData
  - _PrintFileData
  - _LoadLOs
  - _CloseArchive
  - _StartLOs
  - _StartLO

## Notes and Other Information
- Returns NULL if no file is found when filename is NULL in read mode
- Fails with pg_fatal if a specific filename cannot be found in read mode
- Creates platform-specific temporary files for write operations with appropriate security permissions
- Does not support compression - will fail if compression is enabled
- On Windows, uses a retry loop to handle temporary file creation in case of name collisions
- Sets restrictive file permissions using umask for security when creating temporary files