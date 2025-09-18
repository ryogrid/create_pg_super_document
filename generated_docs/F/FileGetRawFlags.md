# FileGetRawFlags

## Location
src/backend/storage/file/fd.c: 2484 - 2493

## Overview
FileGetRawFlags returns the file flags that were used when opening a PostgreSQL File, providing access to the original open(2) system call flags.

## Definition


## Detailed Description
FileGetRawFlags retrieves the fileFlags field from the VfdCache for a given PostgreSQL File descriptor. These flags represent the original flags passed to the open(2) system call when the file was opened, such as O_RDONLY, O_WRONLY, O_RDWR, O_CREAT, O_TRUNC, O_DIRECT, and other system-specific flags. This information is useful for determining the access mode and special attributes of an open file.

The function provides access to the cached flags without requiring additional system calls, as PostgreSQL stores this information in its virtual file descriptor cache when files are opened through the VFD system.

## Parameters / Member Variables
- : A PostgreSQL File descriptor representing an open file in the virtual file descriptor system

## Dependencies
- Functions called/Symbols referenced:
  - FileIsValid (validates the file descriptor)
  - VfdCache (global virtual file descriptor cache array)
- Called from (representative examples):
  - PG_O_DIRECT (for checking direct I/O flags)

## Notes and Other Information
- The function includes an assertion to validate the file descriptor using FileIsValid
- The returned flags are the original flags used during the open(2) system call
- Common flags include access modes (O_RDONLY, O_WRONLY, O_RDWR) and special attributes (O_DIRECT, O_SYNC)
- This function is part of PostgreSQL's file descriptor introspection capabilities
- Used primarily for checking file access modes and special I/O attributes
- The flags remain constant for the lifetime of the file descriptor