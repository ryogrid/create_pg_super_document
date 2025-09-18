# linkFile

## Location
src/bin/pg_upgrade/file.c: 190 - 215

## Overview
Creates a hard link from a source relation file to a destination path, providing an efficient way to have multiple directory entries point to the same file data.

## Definition


## Detailed Description
The linkFile function creates a hard link between two file paths using the POSIX  system call. A hard link creates a new directory entry that points to the same inode as the source file, effectively giving the same file data multiple names in the filesystem. This is the most efficient file "copying" method as no actual data duplication occurs.

Hard linking is particularly useful during PostgreSQL upgrades when the same relation data needs to be accessible from multiple locations without consuming additional disk space. The operation is atomic and instantaneous regardless of file size.

The function provides a simple wrapper around the  system call with appropriate error handling and PostgreSQL-specific error messaging.

## Parameters / Member Variables
- : Source file path to create a hard link from
- : Destination path where the hard link will be created
- : SQL schema name of the relation (used only for error reporting)
- : SQL relation name (used only for error reporting)

## Dependencies
- Functions called/Symbols referenced:
  - link
  - pg_fatal
- Called from (representative examples):
  - transfer_relfile

## Notes and Other Information
- Hard links can only be created on the same filesystem as the source file
- The source and destination will share the same inode, so changes to one are immediately visible in the other
- Hard links do not consume additional disk space beyond the directory entry
- The file data is only deleted when the last hard link is removed
- This is the most efficient file transfer method used by pg_upgrade when filesystem constraints allow
- Hard linking preserves all file metadata including permissions, timestamps, and ownership
- Cannot create hard links to directories (only regular files)
- Used as the preferred file transfer method in pg_upgrade when both old and new data directories are on the same filesystem