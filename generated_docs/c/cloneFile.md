# cloneFile

## Location
src/bin/pg_upgrade/file.c: 39 - 81

## Overview
Creates a file clone/reflink from a source file to a destination file, providing efficient copy-on-write file duplication for relation files in PostgreSQL.

## Definition


## Detailed Description
The cloneFile function implements efficient file cloning using platform-specific system calls. It attempts to create a copy-on-write clone of a file, which shares disk blocks between source and destination until either file is modified. This is significantly more efficient than traditional file copying for large relation files during pg_upgrade operations.

The function provides two platform-specific implementations:
1. **macOS**: Uses the  system call with  flag
2. **Linux**: Uses the  ioctl operation on BTRFS and other filesystems that support reflinks

If cloning fails at any point, the function terminates the program with a fatal error message that includes the schema name, relation name, and file paths for debugging.

## Parameters / Member Variables
- : Source file path to clone from
- : Destination file path to create as a clone
- : SQL schema name of the relation (used only for error reporting)
- : SQL relation name (used only for error reporting)

## Dependencies
- Functions called/Symbols referenced:
  - copyfile (macOS implementation)
  - open
  - ioctl (Linux implementation)
  - close
  - unlink
  - strerror
  - [pg_fatal](../p/pg_fatal.md)
  - PG_BINARY
  - pg_file_create_mode
- Called from (representative examples):
  - [transfer_relfile](../t/transfer_relfile.md)

## Notes and Other Information
- The function is conditionally compiled based on platform capabilities (,  for macOS,  and  for Linux)
- On Linux, the function opens the source file in read-only mode and creates the destination file with appropriate permissions
- If the clone operation fails on Linux, the partially created destination file is cleaned up using 
- The function is primarily used during PostgreSQL upgrades to efficiently duplicate relation files
- Clone operations require filesystem support (e.g., BTRFS, APFS) and may fall back to regular copying in some upgrade scenarios