# walsummary_read_callback

## Location
src/bin/pg_walsummary/pg_walsummary.c: 246 - 264

## Overview
Read callback function for WAL summary file operations that performs file I/O with error handling and returns bytes read.

## Definition


## Detailed Description
This callback function handles reading data from WAL summary files through a standardized interface. It receives a ws_file_info structure containing file descriptor and filename, then performs a read operation to fill the provided buffer with the requested number of bytes. If the read operation fails, it terminates the program with a fatal error message including the filename and system error details. The function returns the actual number of bytes read, which may be less than requested if end-of-file is reached or fewer bytes are available.

## Parameters / Member Variables
- : Pointer to ws_file_info structure containing file descriptor and filename
- : Buffer to receive the read data
- : Number of bytes to attempt to read

## Dependencies
- Functions called/Symbols referenced:
  - read (POSIX file read system call)
  - [pg_fatal](../p/pg_fatal.md) (PostgreSQL fatal error reporting function)
  - [ws_file_info](ws_file_info.md) (file information structure type)
- Called from:
  - [CreateBlockRefTableReader](../C/CreateBlockRefTableReader.md) (in pg_walsummary.c:111 as read callback)

## Notes and Other Information
- Function follows standard callback interface for file reading operations
- Static function scope limits visibility to pg_walsummary.c file
- Uses pg_fatal for error reporting which includes system error message via %m
- Returns actual bytes read count, enabling caller to handle partial reads or EOF
- Callback mechanism allows BlockRefTableReader to perform file I/O without direct file access
- The ws_file_info structure contains both file descriptor (fd) and filename for operations and error reporting