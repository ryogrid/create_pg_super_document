# pgunlink

## Location
[src/port/dirmod.c:119-181](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/dirmod.c#L119-L181)

## Overview
A robust cross-platform file unlinking function that handles both regular files and symbolic links/junction points with retry logic for sharing violations and special Windows file system states.

## Definition


## Detailed Description
The  function provides a comprehensive file deletion mechanism that addresses several cross-platform challenges. It first attempts a direct unlink operation, which covers the most common case of deleting regular files. If this fails with EACCES (access denied), the function performs additional analysis to determine the appropriate deletion strategy.

The function handles a critical Windows-specific scenario involving junction points (used to emulate symbolic links), which require  instead of  for proper removal. It uses  to examine the file type and determine whether to use  or  in subsequent retry attempts.

A key feature is the handling of STATUS_DELETE_PENDING errors on Windows - files that have been marked for deletion but are still held open by other processes. In such cases, the function continues retrying rather than immediately reporting failure, allowing recursive directory deletion algorithms to work correctly.

The function implements a retry loop similar to , waiting up to 10 seconds (100 iterations × 100ms) for temporary access issues to resolve.

## Parameters / Member Variables
- : File or directory path to be unlinked/removed

## Dependencies
- Functions called/Symbols referenced:
  -  (standard file deletion function)
  -  (file status examination)
  -  (Windows STATUS_DELETE_PENDING detection)
  -  (symbolic link type checking macro)
  -  (directory/junction point removal)
  -  (PostgreSQL sleep function)
- Called from (representative examples):
  - Various file management operations throughout PostgreSQL
  - Directory cleanup routines

## Notes and Other Information
- Returns 0 on success, -1 on failure
- The function automatically detects and handles symbolic links/junction points by using  instead of 
- Implements intelligent retry logic that only retries on EACCES errors, avoiding infinite loops on permission errors
- The STATUS_DELETE_PENDING handling is crucial for Windows environments where files can exist in a pending deletion state
- Each retry iteration waits 100ms to allow temporary locks or sharing violations to resolve
- The function is designed to work correctly with recursive directory deletion algorithms by properly handling edge cases
- The single lstat() call optimization means it won't detect file type changes during retry loops, but this is considered acceptable for the expected use cases