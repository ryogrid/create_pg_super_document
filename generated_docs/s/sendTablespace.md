# sendTablespace

## Location
src/backend/backup/basebackup.c: 1134 - 1186

## Overview
sendTablespace includes a tablespace directory in the output tar stream during base backup operations, handling auxiliary tablespaces (not PGDATA).

## Definition


## Detailed Description
This function processes auxiliary tablespace directories during PostgreSQL base backup operations. It constructs the path to the tablespace version directory, creates a directory entry in the tar stream with proper permissions, and recursively sends all files within that directory. The function handles cases where tablespaces may be removed during the backup process gracefully by returning 0 if the directory no longer exists.

## Parameters / Member Variables
- `sink`: bbsink object representing the backup destination stream
- `path`: File system path pointing to the tablespace location
- `spcoid`: Object identifier (OID) of the tablespace being processed
- `sizeonly`: Boolean flag - if true, only calculates total size without sending data
- `manifest`: Pointer to backup manifest information structure for tracking backup contents
- `ib`: Pointer to incremental backup information structure

## Dependencies
- Functions called/Symbols referenced:
  - lstat
  - _tarWriteHeader
  - sendDir
  - TABLESPACE_VERSION_DIRECTORY
- Called from (representative examples):
  - perform_base_backup

## Notes and Other Information
- Only used for auxiliary tablespaces, not for the main PGDATA directory
- Appends TABLESPACE_VERSION_DIRECTORY to the provided path to ensure only the correct version directory is included
- Gracefully handles tablespace removal during backup by checking for ENOENT errors
- Returns the total size of data processed/sent
- Part of PostgreSQL's base backup infrastructure located in src/backend/backup/basebackup.c:1134-1186