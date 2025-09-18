# pgrename

## Location
[src/port/dirmod.c:52-103](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/dirmod.c#L52-L103)

## Overview
A cross-platform file rename function that handles platform-specific file locking issues and retries operations when files are temporarily unavailable due to sharing violations.

## Definition


## Detailed Description
The  function provides a robust file renaming operation that addresses platform-specific challenges with file access. On Windows, it uses  with  flag, while on Unix-like systems it uses the standard  system call. The function implements a retry mechanism to handle cases where other processes might have the target file open without appropriate sharing flags.

The function will retry up to 100 times (approximately 10 seconds total) when encountering specific sharing violations or access denied errors. This retry logic is essential in PostgreSQL's multi-process environment where files might be temporarily locked by other backend processes or external applications like anti-virus software.

## Parameters / Member Variables
- : Source file path to be renamed
- : Destination file path for the renamed file

## Dependencies
- Functions called/Symbols referenced:
  -  (Windows only)
  -  (Unix-like systems)
  -  (Windows error mapping)
  -  (PostgreSQL sleep function)
- Called from (representative examples):
  -  (pg_upgrade utility)
  - Various file management operations throughout PostgreSQL

## Notes and Other Information
- The function returns 0 on success and -1 on failure
- On Windows, it specifically handles ERROR_ACCESS_DENIED, ERROR_SHARING_VIOLATION, and ERROR_LOCK_VIOLATION errors with retry logic
- On Unix systems, it retries only on EACCES (access denied) errors
- The 100-iteration limit prevents infinite loops while allowing sufficient time for temporary locks to be released
- Each retry iteration waits 100ms (100,000 microseconds) before attempting the operation again
- This function is critical for PostgreSQL's WAL file management and other file operations that must be atomic