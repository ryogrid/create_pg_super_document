# fdatasync

## Location
src/port/win32fdatasync.c: 23 - 51

## Overview
The  function is a Windows-specific implementation that provides POSIX  functionality for synchronizing a file's data to storage without flushing metadata.

## Definition


## Detailed Description
This function implements the POSIX  system call for Windows platforms where the native function is not available. Unlike  which synchronizes both file data and metadata,  only synchronizes the file's data contents to persistent storage, making it potentially faster for scenarios where metadata synchronization is not required.

The implementation uses Windows NT's native API  with the  flag to achieve data-only synchronization. This provides better performance than full synchronization while still ensuring data durability.

The function handles Windows-specific error mapping and provides proper POSIX-compliant return values and error codes.

## Parameters / Member Variables
- : The file descriptor of the file to be synchronized. Must be a valid file descriptor obtained from a successful file open operation.

## Dependencies
- Functions called/Symbols referenced:
  -  - Convert C runtime file descriptor to Windows HANDLE
  -  - Initialize NT DLL function pointers
  -  - Windows NT API for selective buffer flushing
  -  - Flag constant for data-only sync
  -  - Convert NT status to DOS error code
  -  - Map DOS error to C runtime errno

- Called from (representative examples):
  -  - PostgreSQL wrapper function in fd.c:485
  -  - File sync testing utility in pg_test_fsync.c:351

## Notes and Other Information
- This function is only compiled and used on Windows platforms as part of the portability layer
- The function is declared in  for Windows builds
- Returns 0 on success, -1 on failure with appropriate errno set
- Handles EINVAL error case by converting Windows NTSTATUS to POSIX errno values
- Part of PostgreSQL's effort to provide cross-platform POSIX compatibility
- The data-only synchronization can provide performance benefits over full fsync in write-heavy scenarios where metadata changes are less frequent
- Used internally by PostgreSQL's file descriptor management system through the  wrapper